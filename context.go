package reelay

import (
	"context"
	"sync"
	"time"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// Per-request context.
//
// The Node SDK carries this across async boundaries with AsyncLocalStorage. Go
// has no implicit ambient store, so the SDK uses the idiomatic mechanism:
// context.Context. The middleware seeds a *RequestContext into the request's
// context; capture reads it; user code appends logs/spans/breadcrumbs through
// the public API by passing that context down (as Go code already does).

const maxTimelineEntries = 100

// RequestContext holds everything captured for the life of one request. It is
// safe for concurrent use because a request may fan out across goroutines.
type RequestContext struct {
	Trace     core.TraceContext
	SessionID string
	// TraceSampleRate is the effective probability chosen at the distributed
	// trace root. It is propagated so every service weights metrics equally.
	TraceSampleRate float64
	// TracePropagationEnabled distinguishes "sampling disabled here" from an
	// explicit upstream unsampled decision, which must continue downstream.
	TracePropagationEnabled bool

	mu    sync.Mutex
	http  *core.HTTPContext
	logs  []core.TimelineLog
	spans []core.TimelineSpan

	// Crumbs is this request's private breadcrumb buffer, so one request's
	// breadcrumbs never leak onto another concurrent request's error report.
	Crumbs *core.BreadcrumbBuffer

	// Txn is the request's performance transaction, present only when this
	// request was sampled (Options.TracesSampleRate). Spans attach here via
	// reelay.WithSpan(ctx, …).
	Txn *core.ActiveTransaction
}

type contextKey struct{}
type spanContextKey struct{}

var rcKey = contextKey{}
var activeSpanKey = spanContextKey{}

func activeSpanFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(activeSpanKey).(string)
	return id
}

// ContextWith returns a copy of parent carrying rc, retrievable via FromContext.
func ContextWith(parent context.Context, rc *RequestContext) context.Context {
	return context.WithValue(parent, rcKey, rc)
}

// FromContext returns the active RequestContext, or nil when there is none.
func FromContext(ctx context.Context) *RequestContext {
	if ctx == nil {
		return nil
	}
	rc, _ := ctx.Value(rcKey).(*RequestContext)
	return rc
}

// setHTTP / httpSnapshot / setStatus guard the http field.

func (rc *RequestContext) setHTTP(h *core.HTTPContext) {
	rc.mu.Lock()
	rc.http = h
	rc.mu.Unlock()
}

func (rc *RequestContext) setStatus(code int) {
	rc.mu.Lock()
	if rc.http != nil {
		rc.http.StatusCode = code
	}
	rc.mu.Unlock()
}

func (rc *RequestContext) httpSnapshot() *core.HTTPContext {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.http == nil {
		return nil
	}
	cp := *rc.http
	if rc.http.Headers != nil {
		cp.Headers = make(map[string]string, len(rc.http.Headers))
		for k, v := range rc.http.Headers {
			cp.Headers[k] = v
		}
	}
	return &cp
}

func (rc *RequestContext) appendLog(level, msg string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(rc.logs) >= maxTimelineEntries {
		return
	}
	rc.logs = append(rc.logs, core.TimelineLog{
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Level: level,
		Msg:   truncateRunes(msg, 500),
	})
}

func (rc *RequestContext) appendSpan(span core.TimelineSpan) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(rc.spans) >= maxTimelineEntries {
		return
	}
	rc.spans = append(rc.spans, span)
}

// timelineSnapshot returns a copy of the recorded logs and spans, or nil if
// none were recorded.
func (rc *RequestContext) timelineSnapshot() *core.Timeline {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(rc.logs) == 0 && len(rc.spans) == 0 {
		return &core.Timeline{}
	}
	t := &core.Timeline{}
	if len(rc.logs) > 0 {
		t.Logs = append([]core.TimelineLog(nil), rc.logs...)
	}
	if len(rc.spans) > 0 {
		t.Spans = append([]core.TimelineSpan(nil), rc.spans...)
	}
	return t
}

// AppendLog adds a log line to the active request's timeline. It is a no-op
// when ctx carries no request context (e.g. a background job). Messages are
// trimmed to 500 characters.
func AppendLog(ctx context.Context, level, msg string) {
	if rc := FromContext(ctx); rc != nil {
		rc.appendLog(level, msg)
	}
}

// AppendSpan adds a span to the active request's timeline. No-op without an
// active request context.
func AppendSpan(ctx context.Context, span core.TimelineSpan) {
	if rc := FromContext(ctx); rc != nil {
		rc.appendSpan(span)
	}
}

// StartSpan manually times a child span on the active APM transaction and also
// records its compact form on the error timeline. It is the manual-control
// counterpart to WithSpan. The returned finish function is idempotent.
func StartSpan(ctx context.Context, category, name string) func(status string) {
	spanID := core.NewSpanID()
	started := time.Now()
	parentSpanID := activeSpanFromContext(ctx)
	var once sync.Once
	return func(status string) {
		once.Do(func() {
			AppendSpan(ctx, core.TimelineSpan{
				SpanID: spanID,
				Name:   name,
				Status: status,
			})
			if rc := FromContext(ctx); rc != nil && rc.Txn != nil {
				rc.Txn.AddChildSpan(category, name, started, time.Now(), status, nil, spanID, parentSpanID)
			}
		})
	}
}

// truncateRunes caps a string to max runes, preserving valid UTF-8.
func truncateRunes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}
