package reelay

import (
	"net/http"
	"strconv"
	"time"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// Transport is an http.RoundTripper that instruments outbound HTTP calls.
//
// For every request made with a request-scoped context (one that carries the
// reelay RequestContext seeded by the middleware, or by WithTransaction), it:
//
//   - records an "http.client" span on the active transaction with
//     OpenTelemetry-style attributes (http.request.method, url.full,
//     http.response.status_code, …), and
//   - injects W3C traceparent + reelay-session-id headers so the callee's
//     middleware adopts the same trace id — stitching this service and the ones
//     it calls into a single distributed trace.
//
// It is a strict superset of the wrapped RoundTripper: with no active
// transaction (a background call outside any request/job) it passes straight
// through, and it never mutates the caller's *http.Request.
//
// Usage:
//
//	client := &http.Client{Transport: reelay.NewTransport(nil)}
//	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil) // ctx from the handler
//	res, err := client.Do(req)
type Transport struct {
	Base http.RoundTripper
}

// NewTransport wraps base (or http.DefaultTransport when nil).
func NewTransport(base http.RoundTripper) *Transport {
	return &Transport{Base: base}
}

// WrapClient returns a shallow copy of c whose Transport is instrumented. A nil
// client yields a new instrumented client.
func WrapClient(c *http.Client) *http.Client {
	if c == nil {
		return &http.Client{Transport: NewTransport(nil)}
	}
	cp := *c
	cp.Transport = NewTransport(cp.Transport)
	return &cp
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	rc := FromContext(req.Context())
	// Outside a request/job there is no context to propagate or span to record.
	if rc == nil {
		return t.base().RoundTrip(req)
	}

	// Inject trace headers on a clone so the caller's request is never mutated.
	spanID := core.NewSpanID()
	out := req.Clone(req.Context())
	if out.Header == nil {
		out.Header = make(http.Header)
	}
	if rc.TracePropagationEnabled {
		out.Header.Set("traceparent", core.Traceparent(rc.Trace.TraceID, spanID, rc.Txn != nil))
		if rc.TraceSampleRate > 0 {
			out.Header.Set("reelay-trace-sample-rate", strconv.FormatFloat(rc.TraceSampleRate, 'g', -1, 64))
		}
		if rc.SessionID != "" {
			out.Header.Set("reelay-session-id", rc.SessionID)
		}
	}

	method := req.Method
	if method == "" {
		method = "GET"
	}
	host, path := "", ""
	data := map[string]string{"http.request.method": method}
	if req.URL != nil {
		host = req.URL.Host
		path = req.URL.Path
		data["url.full"] = req.URL.Scheme + "://" + req.URL.Host + req.URL.Path // query dropped (may hold secrets)
		if h := req.URL.Hostname(); h != "" {
			data["server.address"] = h
		}
		if p := req.URL.Port(); p != "" {
			data["server.port"] = p
		}
	}

	start := time.Now()
	res, err := t.base().RoundTrip(out)
	end := time.Now()

	status := "ok"
	if err != nil {
		status = "error"
		data["error.type"] = "transport_error"
	} else {
		data["http.response.status_code"] = strconv.Itoa(res.StatusCode)
		if res.StatusCode >= 400 {
			status = "error"
		}
	}

	if rc.Txn != nil {
		rc.Txn.AddChildSpan("http.client", method+" "+host+path, start, end, status, data, spanID, activeSpanFromContext(req.Context()))
	}
	return res, err
}
