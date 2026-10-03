package core

import (
	"math/rand"
	"net/url"
	"strings"
	"time"
)

// CaptureContext carries the per-event enrichment a platform SDK resolves
// (from its request context) and hands to BaseClient at capture time.
type CaptureContext struct {
	Mechanism   string
	Trace       *TraceContext
	SessionID   string
	HTTP        *HTTPContext
	Timeline    *Timeline
	Breadcrumbs []Breadcrumb
	// Stacktrace is captured by the platform layer (runtime.Callers for direct
	// captures, parsed debug.Stack() for panics) since Go errors carry none.
	Stacktrace []StackFrame
}

// BaseClient holds the behavior every runtime shares: event assembly,
// sampling, scrubbing, BeforeSend, and transport. Platform SDKs embed it and
// add their instrumentation. Every public method is wrapped by Guard so the
// SDK can never panic into the host application.
type BaseClient struct {
	Options   Options
	Transport *Transport
	Scrubber  *Scrubber
	// Crumbs is the process-global breadcrumb buffer used outside a request.
	Crumbs    *BreadcrumbBuffer
	Kind      string // "backend" | "frontend"
	Platform  string // e.g. "go"
	Release   string
	CommitSHA string
}

// NewBaseClient wires up scrubber, breadcrumb buffer, and transport.
func NewBaseClient(opts Options, kind, platform string) *BaseClient {
	opts.CommitSHA = ResolveCommitSHA(opts.CommitSHA)
	opts.Release = ResolveRelease(opts.Release, opts.CommitSHA)
	endpoint := StripTrailingSlash(opts.Endpoint)
	if parsed, err := url.Parse(endpoint); err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !opts.AllowInsecureHTTPForTesting) {
		endpoint = "" // a non-HTTPS endpoint is never allowed to receive telemetry
		if opts.Debug != nil {
			opts.Debug("reelay: endpoint must use https", nil)
		}
	}
	key := opts.IngestKey
	if key == "" {
		key = opts.Token
	}
	if (key == "" || opts.NodeID == "") && !opts.AllowInsecureHTTPForTesting {
		endpoint = ""
		if opts.Debug != nil {
			opts.Debug("reelay: ingest key and node id are required", nil)
		}
	}
	return &BaseClient{
		Options:   opts,
		Scrubber:  NewScrubber(opts.ScrubPatterns),
		Crumbs:    NewBreadcrumbBuffer(50),
		Kind:      kind,
		Platform:  platform,
		Release:   opts.Release,
		CommitSHA: opts.CommitSHA,
		Transport: NewTransport(TransportOptions{
			URL:          endpoint + "/api/ingest/errors",
			IngestKey:    key,
			NodeID:       opts.NodeID,
			MaxQueueSize: opts.MaxQueueSize,
			Debug:        opts.Debug,
		}),
	}
}

// Debug emits an SDK-internal diagnostic if a debug hook is configured.
func (c *BaseClient) Debug(message string, detail any) {
	if c.Options.Debug != nil {
		c.Options.Debug(message, detail)
	}
}

// Guard runs fn inside the SDK's defensive boundary: an internal panic is
// reported to the debug hook and swallowed — never the host's problem.
func (c *BaseClient) Guard(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			c.Debug("internal SDK error", r)
		}
	}()
	fn()
}

// CaptureException assembles, scrubs, filters, and queues an error event,
// returning the event id ("" when sampled out or vetoed by BeforeSend).
func (c *BaseClient) CaptureException(err any, cc CaptureContext) (id string) {
	defer func() {
		if r := recover(); r != nil {
			c.Debug("internal SDK error", r)
			id = ""
		}
	}()

	if !c.sampled() {
		return ""
	}

	typ, value := NormalizeError(err)
	mechanism := cc.Mechanism
	if mechanism == "" {
		mechanism = "generic"
	}

	event := &ErrorEventPayload{
		EventID:   NewEventID(),
		Release:   c.Release,
		CommitSHA: c.CommitSHA,
		Kind:      c.Kind,
		Platform:  c.Platform,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Trace:     cc.Trace,
		SessionID: cc.SessionID,
		Exception: ExceptionPayload{
			Type:       typ,
			Value:      truncate(c.Scrubber.ScrubString(value), 2000),
			Mechanism:  mechanism,
			Stacktrace: cc.Stacktrace,
		},
		HTTP:        c.Scrubber.scrubHTTP(cc.HTTP),
		Timeline:    c.Scrubber.scrubTimeline(cc.Timeline),
		Breadcrumbs: cc.Breadcrumbs,
	}

	outgoing := event
	if c.Options.BeforeSend != nil {
		outgoing = c.Options.BeforeSend(event)
		if outgoing == nil {
			return ""
		}
	}
	c.Transport.Send(outgoing)
	return outgoing.EventID
}

// Flush drains pending events (call before process exit).
func (c *BaseClient) Flush() {
	c.Transport.Flush()
}

func (c *BaseClient) sampled() bool {
	rate := 1.0
	if c.Options.SampleRate != nil {
		rate = *c.Options.SampleRate
	}
	return rate >= 1 || rand.Float64() < rate
}

// StripTrailingSlash removes a single trailing slash from a URL.
func StripTrailingSlash(url string) string {
	return strings.TrimSuffix(url, "/")
}

// truncate caps a string to max runes, preserving valid UTF-8.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}
