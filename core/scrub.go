package core

import "regexp"

// Client-side PII scrubbing. Runs before any payload leaves the host
// application: secrets must never reach the wire.

// Redacted is the replacement text for scrubbed secrets.
const Redacted = "[redacted]"

const maxDepth = 8

// defaultPatterns mirror the Node SDK's built-ins (all RE2-compatible).
var defaultPatterns = []*regexp.Regexp{
	// Bearer / token-ish assignments.
	regexp.MustCompile(`(?i)\b(?:bearer|token|api[-_]?key|secret|password|passwd|pwd|authorization)\b\s*[:=]\s*\S+`),
	// JWTs.
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`),
	// Credit-card-shaped numbers (13–19 digits with optional separators).
	regexp.MustCompile(`\b(?:\d[ -]?){13,19}\b`),
	// Email addresses.
	regexp.MustCompile(`\b[\w.+-]+@[\w-]+\.[\w.-]+\b`),
}

var sensitiveKeys = regexp.MustCompile(`(?i)^(authorization|cookie|set-cookie|x-api-key|x-reelay-token|password|passwd|secret|token|access_token|refresh_token|credit_card|card_number|cvv|ssn)$`)

// Scrubber redacts secrets from strings and JSON-like values.
type Scrubber struct {
	patterns []*regexp.Regexp
}

// NewScrubber returns a scrubber with the built-in patterns plus any extras.
func NewScrubber(extra []*regexp.Regexp) *Scrubber {
	patterns := make([]*regexp.Regexp, 0, len(defaultPatterns)+len(extra))
	patterns = append(patterns, defaultPatterns...)
	patterns = append(patterns, extra...)
	return &Scrubber{patterns: patterns}
}

// ScrubString replaces every matching secret in value with [redacted].
func (s *Scrubber) ScrubString(value string) string {
	out := value
	for _, p := range s.patterns {
		out = p.ReplaceAllString(out, Redacted)
	}
	return out
}

// IsSensitiveKey reports whether a map/header key should be redacted wholesale.
func (s *Scrubber) IsSensitiveKey(key string) bool {
	return sensitiveKeys.MatchString(key)
}

// Scrub deep-scrubs any JSON-serializable value: sensitive keys are redacted
// wholesale; remaining strings run through the pattern engine. Depth-capped so
// scrubbing can never hang the host app. Mirrors the Node SDK's generic
// scrubber, used for free-form values; the typed HTTP/Timeline helpers below
// build on the same rules.
func (s *Scrubber) Scrub(value any) any {
	return s.walk(value, 0)
}

func (s *Scrubber) walk(value any, depth int) any {
	if value == nil || depth > maxDepth {
		return value
	}
	switch v := value.(type) {
	case string:
		return s.ScrubString(v)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			if s.IsSensitiveKey(key) {
				out[key] = Redacted
			} else {
				out[key] = s.walk(item, depth+1)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = s.walk(item, depth+1)
		}
		return out
	default:
		return value
	}
}

// scrubHTTP returns a scrubbed copy of an HTTP context: sensitive headers are
// redacted, remaining header values and the URL run through the pattern engine.
func (s *Scrubber) scrubHTTP(h *HTTPContext) *HTTPContext {
	if h == nil {
		return nil
	}
	out := *h
	out.URL = s.ScrubString(h.URL)
	if h.Headers != nil {
		headers := make(map[string]string, len(h.Headers))
		for k, v := range h.Headers {
			if s.IsSensitiveKey(k) {
				headers[k] = Redacted
			} else {
				headers[k] = s.ScrubString(v)
			}
		}
		out.Headers = headers
	}
	return &out
}

// scrubTimeline returns a scrubbed copy of a timeline: log messages and span
// names run through the pattern engine.
func (s *Scrubber) scrubTimeline(t *Timeline) *Timeline {
	if t == nil {
		return nil
	}
	out := &Timeline{}
	if t.Logs != nil {
		out.Logs = make([]TimelineLog, len(t.Logs))
		for i, l := range t.Logs {
			l.Msg = s.ScrubString(l.Msg)
			out.Logs[i] = l
		}
	}
	if t.Spans != nil {
		out.Spans = make([]TimelineSpan, len(t.Spans))
		for i, sp := range t.Spans {
			sp.Name = s.ScrubString(sp.Name)
			out.Spans[i] = sp
		}
	}
	return out
}
