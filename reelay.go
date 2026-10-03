// Package reelay is the Reelay SDK for Go backends. It reports errors without
// blocking your application and can attach the HTTP request, trace, logs,
// spans, and recent breadcrumbs that explain what happened.
//
// Quick start (net/http):
//
//	reelay.Init(reelay.Options{
//	    Endpoint:  "https://ingest.customer.example",
//	    NodeID:    os.Getenv("REELAY_NODE_ID"),
//	    IngestKey: os.Getenv("REELAY_INGEST_KEY"),
//	})
//	defer reelay.Flush()
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
//	    reelay.AppendLog(r.Context(), "info", "Starting checkout")
//	    panic("payment provider is unavailable")
//	})
//	http.ListenAndServe(":3000", reelay.Middleware()(mux))
package reelay

import (
	"context"
	"net/http"
	"sync"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// Re-exported core types so callers need only import this package, mirroring
// the Node SDK's re-export of CoreOptions/ErrorEventPayload/Breadcrumb.
type (
	// ErrorEventPayload is the wire body POSTed to /api/ingest/errors.
	ErrorEventPayload = core.ErrorEventPayload
	// Breadcrumb is a "what just happened" entry leading up to an error.
	Breadcrumb = core.Breadcrumb
	// TimelineSpan is one span attached to a request timeline.
	TimelineSpan = core.TimelineSpan
	// StackFrame is a single parsed call-stack frame.
	StackFrame = core.StackFrame
)

var (
	mu     sync.Mutex
	active *Client
)

// Init initializes the Reelay Go SDK once per process and installs it as the
// package singleton. Later calls return the original client and ignore the new
// options, matching the Node SDK.
func Init(opts Options) *Client {
	mu.Lock()
	defer mu.Unlock()
	if active != nil {
		if opts.Debug != nil {
			opts.Debug("reelay: Init called more than once; ignoring the new options", nil)
		}
		return active
	}
	active = NewClient(opts)
	return active
}

// GetClient returns the active client, or nil if Init has not run.
func GetClient() *Client {
	mu.Lock()
	defer mu.Unlock()
	return active
}

// CaptureException reports a caught error with the active request context from
// ctx. No-op (returns "") before Init.
func CaptureException(ctx context.Context, err any) string {
	if c := GetClient(); c != nil {
		return c.CaptureException(ctx, err)
	}
	return ""
}

// CaptureMessage reports a string as a synthetic Message error. No-op before
// Init.
func CaptureMessage(ctx context.Context, message string) string {
	if c := GetClient(); c != nil {
		return c.CaptureMessage(ctx, message)
	}
	return ""
}

// AddBreadcrumb records a breadcrumb on the active client. No-op before Init.
func AddBreadcrumb(ctx context.Context, crumb Breadcrumb) {
	if c := GetClient(); c != nil {
		c.AddBreadcrumb(ctx, crumb)
	}
}

// Middleware returns net/http middleware bound to the active client. It panics
// if called before Init, mirroring the Node SDK's middleware() guard.
func Middleware() func(http.Handler) http.Handler {
	c := GetClient()
	if c == nil {
		panic("reelay: call Init() before Middleware()")
	}
	return c.Middleware
}

// Recover captures a panic via the active client and re-panics to preserve
// crash semantics. Use as `defer reelay.Recover(ctx)`. No-op before Init.
func Recover(ctx context.Context) {
	if r := recover(); r != nil {
		if c := GetClient(); c != nil {
			c.capturePanic(ctx, r)
			c.Transport.FlushTimeout(fatalFlushTimeout)
		}
		panic(r)
	}
}

// RecoverAndContinue captures a panic via the active client and swallows it, so
// a worker goroutine survives. Use as `defer reelay.RecoverAndContinue(ctx)`.
func RecoverAndContinue(ctx context.Context) {
	if r := recover(); r != nil {
		if c := GetClient(); c != nil {
			c.capturePanic(ctx, r)
		}
	}
}

// Flush drains the in-memory delivery queue. Call it before the process exits
// (`defer reelay.Flush()` in main). No-op before Init.
func Flush() {
	if c := GetClient(); c != nil {
		c.Flush()
	}
}

// Close gracefully shuts the SDK down: flushes all pending telemetry bounded
// by ctx, stops the profiler and every background drain goroutine, and clears
// the package singleton (a later Init builds a fresh client). No-op before
// Init. Returns true when everything drained before ctx expired.
func Close(ctx context.Context) bool {
	mu.Lock()
	c := active
	active = nil
	mu.Unlock()
	if c == nil {
		return true
	}
	return c.Close(ctx)
}
