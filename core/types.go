// Package core holds the runtime-agnostic behavior every Reelay SDK shares:
// wire types, event assembly, sampling, scrubbing, the breadcrumb ring buffer,
// stack parsing, id generation, and the resilient transport. Platform packages
// (e.g. the root reelay package for Go backends) embed BaseClient and add their
// own instrumentation.
package core

import (
	"regexp"
	"time"
)

// Wire types — mirror reelay-backend's ingest contract exactly
// (POST /api/ingest/errors, POST /api/ingest/sessions). The JSON tags must not
// drift from internal/application/usecases.SDKErrorInput on the backend.

// StackFrame is a single parsed call-stack frame. Frames from runtime/stdlib or
// the module cache are marked InApp=false so fingerprinting groups on
// application code only.
type StackFrame struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Col      int    `json:"col,omitempty"`
	Function string `json:"function,omitempty"`
	Module   string `json:"module,omitempty"`
	InApp    bool   `json:"in_app"`
}

// ExceptionPayload is the captured error itself.
type ExceptionPayload struct {
	Type       string       `json:"type"`
	Value      string       `json:"value"`
	Mechanism  string       `json:"mechanism,omitempty"`
	Stacktrace []StackFrame `json:"stacktrace,omitempty"`
}

// TraceContext links an event to a distributed trace (W3C trace-context ids).
type TraceContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id,omitempty"`
}

// HTTPContext describes the request an error happened inside.
type HTTPContext struct {
	Method     string            `json:"method"`
	URL        string            `json:"url"`
	StatusCode int               `json:"status_code,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// TimelineLog is one log line attached to a request timeline.
type TimelineLog struct {
	Ts    string `json:"ts"` // RFC3339
	Level string `json:"level"`
	Msg   string `json:"msg"`
}

// TimelineSpan is one span attached to a request timeline.
type TimelineSpan struct {
	SpanID string `json:"span_id"`
	Name   string `json:"name"`
	Status string `json:"status,omitempty"`
}

// Timeline groups the logs and spans recorded during a request.
type Timeline struct {
	Logs  []TimelineLog  `json:"logs,omitempty"`
	Spans []TimelineSpan `json:"spans,omitempty"`
}

// Breadcrumb is a small "what just happened" entry leading up to an error.
type Breadcrumb struct {
	Timestamp int64  `json:"timestamp"` // unix ms
	Type      string `json:"type"`
	Category  string `json:"category,omitempty"`
	Message   string `json:"message,omitempty"`
}

// ErrorEventPayload is the full body POSTed to /api/ingest/errors.
type ErrorEventPayload struct {
	EventID     string           `json:"event_id"`
	Release     string           `json:"release,omitempty"`
	CommitSHA   string           `json:"commit_sha,omitempty"`
	Kind        string           `json:"kind"`     // "backend" | "frontend"
	Platform    string           `json:"platform"` // e.g. "go"
	Timestamp   string           `json:"timestamp"`
	Trace       *TraceContext    `json:"trace,omitempty"`
	SessionID   string           `json:"session_id,omitempty"`
	Exception   ExceptionPayload `json:"exception"`
	HTTP        *HTTPContext     `json:"http,omitempty"`
	Timeline    *Timeline        `json:"timeline,omitempty"`
	Breadcrumbs []Breadcrumb     `json:"breadcrumbs,omitempty"`
}

// Options are shared by every Reelay SDK. Zero values mean "use the default"
// (SampleRate nil = 1, MaxQueueSize 0 = 30), matching the Node SDK's optional
// fields.
type Options struct {
	// Endpoint is the customer-controlled HTTPS ingestion base URL, e.g.
	// https://ingest.customer.example. Plain HTTP endpoints are rejected.
	// The SDK appends /api/ingest/errors itself — do not add a path.
	Endpoint string
	// NodeID identifies the self-hosted node that owns the event.
	NodeID string
	// IngestKey authenticates SDK requests. It is validated by Reelay Cloud by
	// the agent for every event and is never sent with the event payload.
	IngestKey string
	// Token is retained only as a source-compatible alias for IngestKey during
	// migration. New integrations must use IngestKey.
	Token string
	// CommitSHA is the full revision of the artifact being run. When empty,
	// REELAY_COMMIT_SHA is resolved once during client construction.
	CommitSHA string
	// Release is the artifact version. When empty, REELAY_RELEASE is used,
	// then CommitSHA. Examples: checkout@1.8.0 or a full Git SHA.
	Release string
	// SampleRate is the fraction of error events to send, 0–1. nil defaults to
	// 1 (every error). Purely a cost/volume lever for high-throughput services.
	SampleRate *float64
	// BeforeSend mutates or vetoes an event just before send. Return nil to
	// drop the event. It runs after built-in scrubbing — do not reinsert
	// secrets here.
	BeforeSend func(event *ErrorEventPayload) *ErrorEventPayload
	// ScrubPatterns are extra secret formats redacted in every string value, on
	// top of the built-ins.
	ScrubPatterns []*regexp.Regexp
	// MaxQueueSize is retained for compatibility. The delivery queue now grows
	// during bursts rather than dropping events. 0 defaults to 30.
	MaxQueueSize int
	// Debug receives SDK-internal diagnostics. nil silences them (the SDK never
	// logs to stderr on its own, except the fatal crash path).
	Debug func(message string, detail any)
	// AllowInsecureHTTPForTesting permits HTTP only in controlled SDK tests.
	// Production integrations must leave this false; endpoints are HTTPS-only.
	AllowInsecureHTTPForTesting bool

	// TracesSampleRate is the fraction of HTTP requests recorded as
	// performance transactions, 0–1. nil or 0 disables performance
	// monitoring. Sampling is head-based per request; the rate travels with
	// each transaction so the server extrapolates accurate metrics.
	TracesSampleRate *float64
	// Profiling enables continuous wall-clock profiling: the SDK samples all
	// running goroutines' stacks in short windows on a fixed cycle and
	// uploads pre-folded stacks. Steady-state overhead is well under ~2% at
	// the defaults (10s window per 60s cycle, 100ms sample period).
	Profiling bool
	// ProfileWindow / ProfileCycle tune the profiling duty cycle. Zero
	// values default to 10s / 60s.
	ProfileWindow time.Duration
	ProfileCycle  time.Duration
}
