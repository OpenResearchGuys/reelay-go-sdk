package reelay

import (
	"bufio"
	"net"
	"net/http"
	"strconv"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// HTTP middleware.
//
// The Node SDK splits this into requestHandler (seeds context) and errorHandler
// (captures route errors). Go's net/http has a single wrapper shape and already
// isolates a panicking handler to its request, so the SDK ships one middleware
// that does both jobs:
//
//  1. reads incoming W3C traceparent and reelay-session-id headers;
//  2. seeds a RequestContext into the request context;
//  3. captures method, URL, six safe headers, and the final response status;
//  4. recovers a panicking handler, captures it, and returns 500 — one bad
//     request never takes down the server.
//
// The browser SDK adds traceparent + reelay-session-id to selected fetches;
// adopting them here lets a backend error share the browser session's trace.

// safeHeaders are the only request headers captured. Authentication and cookie
// headers are deliberately never selected.
var safeHeaders = []string{"User-Agent", "Content-Type", "Accept", "Referer", "Origin", "Host", "X-Request-Id", "X-Correlation-Id"}

// Middleware wraps an http.Handler with Reelay request context and panic
// capture. Register it ahead of your routes:
//
//	mux := http.NewServeMux()
//	// ... routes ...
//	http.ListenAndServe(":3000", client.Middleware(mux))
func (c *Client) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := &RequestContext{
			Trace:     core.TraceContext{SpanID: core.NewSpanID()},
			SessionID: r.Header.Get("Reelay-Session-Id"),
			Crumbs:    core.NewBreadcrumbBuffer(50),
		}
		parentSpanID := ""
		if incoming := core.ParseTraceparent(r.Header.Get("Traceparent")); incoming != nil {
			rc.TracePropagationEnabled = true
			rc.Trace.TraceID = incoming.TraceID
			parentSpanID = incoming.SpanID
		} else {
			rc.Trace.TraceID = core.NewTraceID()
		}
		rc.setHTTP(&core.HTTPContext{
			Method:  r.Method,
			URL:     r.URL.String(),
			Headers: pickHeaders(r),
		})

		// Performance transaction (head-based sampling). It adopts the
		// incoming traceparent, so a browser pageload, this server
		// transaction, and downstream services share one distributed trace.
		// Handlers can refine the name via reelay.SetTransactionName.
		sampled := c.tracesSampled(rc.Trace.TraceID)
		if incoming := core.ParseTraceparent(r.Header.Get("Traceparent")); incoming != nil {
			sampled = incoming.Sampled
			if rate, err := strconv.ParseFloat(r.Header.Get("Reelay-Trace-Sample-Rate"), 64); err == nil && rate > 0 && rate <= 1 {
				rc.TraceSampleRate = rate
			} else {
				rc.TraceSampleRate = 1
			}
		} else {
			rc.TraceSampleRate = c.tracesSampleRate()
			rc.TracePropagationEnabled = rc.TraceSampleRate > 0
		}
		if sampled {
			rc.Txn = core.NewActiveTransaction(
				r.Method+" "+core.ParameterizePath(r.URL.Path),
				"http.server",
				rc.Trace.TraceID,
				rc.Trace.SpanID,
				parentSpanID,
			)
			rc.Txn.SetRequestHeaders(pickHeaders(r))
		}

		ctx := ContextWith(r.Context(), rc)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		finishTxn := func() {
			if rc.Txn == nil {
				return
			}
			rc.Txn.SetHTTPStatus(sw.status)
			c.RecordTransaction(rc.Txn, rc.SessionID, rc.TraceSampleRate)
		}

		defer func() {
			if rec := recover(); rec != nil {
				// Stamp the status before capture, mirroring the Node
				// errorHandler which forces an unsent sub-500 status to 500.
				if !sw.wroteHeader {
					http.Error(sw, "internal server error", http.StatusInternalServerError)
				} else if sw.status < http.StatusInternalServerError {
					sw.status = http.StatusInternalServerError
				}
				rc.setStatus(sw.status)
				c.captureWithStack(ctx, rec, "middleware", core.CurrentStack(50))
				finishTxn()
				return
			}
			rc.setStatus(sw.status)
			finishTxn()
		}()

		next.ServeHTTP(sw, r.WithContext(ctx))
	})
}

// statusWriter records the response status so it can be attached to a captured
// event, mirroring the Node middleware's res.once('finish') status capture.
//
// Wrapping a ResponseWriter erases its optional interfaces, which would break
// streaming (http.Flusher) and websocket upgrades (http.Hijacker) for every
// handler behind the middleware. Unwrap restores everything routed through
// http.ResponseController (Go 1.20+), and Flush/Hijack are forwarded directly
// for the many libraries that still type-assert.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Flush forwards to the underlying writer when it supports streaming.
func (w *statusWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards a connection takeover (websockets) when supported.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func pickHeaders(r *http.Request) map[string]string {
	out := make(map[string]string)
	for _, name := range safeHeaders {
		if v := r.Header.Get(name); v != "" {
			out[name] = v
		}
	}
	return out
}
