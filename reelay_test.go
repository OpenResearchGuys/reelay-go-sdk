package reelay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// collectingClient captures events into a sink instead of sending them, by
// vetoing every event in BeforeSend (no network I/O).
func collectingClient(sink *[]*core.ErrorEventPayload) *Client {
	var mu sync.Mutex
	return NewClient(Options{
		Endpoint:                    "http://127.0.0.1:0",
		Token:                       "test",
		AllowInsecureHTTPForTesting: true,
		BeforeSend: func(e *core.ErrorEventPayload) *core.ErrorEventPayload {
			mu.Lock()
			*sink = append(*sink, e)
			mu.Unlock()
			return nil
		},
	})
}

func newRequestContext(traceID string) *RequestContext {
	return &RequestContext{
		Trace:  core.TraceContext{TraceID: traceID},
		Crumbs: core.NewBreadcrumbBuffer(50),
	}
}

func TestBreadcrumbRequestIsolation(t *testing.T) {
	var events []*core.ErrorEventPayload
	client := collectingClient(&events)

	ctxA := ContextWith(context.Background(), newRequestContext("a"))
	client.AddBreadcrumb(ctxA, core.Breadcrumb{Timestamp: 1, Type: "log", Message: "belongs-to-A"})
	client.CaptureException(ctxA, errFromString("err-A"))

	ctxB := ContextWith(context.Background(), newRequestContext("b"))
	client.CaptureException(ctxB, errFromString("err-B"))

	var eventA, eventB *core.ErrorEventPayload
	for _, e := range events {
		switch e.Exception.Value {
		case "err-A":
			eventA = e
		case "err-B":
			eventB = e
		}
	}
	if eventA == nil || eventB == nil {
		t.Fatalf("missing events: A=%v B=%v", eventA, eventB)
	}
	if !hasCrumb(eventA, "belongs-to-A") {
		t.Fatal("request A should carry its breadcrumb")
	}
	if len(eventB.Breadcrumbs) != 0 {
		t.Fatalf("request B leaked breadcrumbs: %+v", eventB.Breadcrumbs)
	}
}

func TestLeavesReleaseUnsetWithoutArtifactIdentity(t *testing.T) {
	var events []*core.ErrorEventPayload
	client := collectingClient(&events)
	client.CaptureException(context.Background(), errFromString("plain"))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Release != "" {
		t.Fatalf("unexpected release: %q", events[0].Release)
	}
}

func TestStampsCommitSHA(t *testing.T) {
	var events []*core.ErrorEventPayload
	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	client := NewClient(Options{
		Endpoint: "http://127.0.0.1:0", Token: "test", CommitSHA: sha, AllowInsecureHTTPForTesting: true,
		BeforeSend: func(e *core.ErrorEventPayload) *core.ErrorEventPayload {
			events = append(events, e)
			return nil
		},
	})
	client.CaptureException(context.Background(), errFromString("versioned"))
	if len(events) != 1 || events[0].CommitSHA != sha {
		t.Fatalf("commit SHA not stamped: %+v", events)
	}
	if events[0].Release != sha {
		t.Fatalf("commit SHA was not used as default release: %+v", events)
	}
}

func TestMiddlewareStampsStatusOnPanic(t *testing.T) {
	var events []*core.ErrorEventPayload
	client := collectingClient(&events)

	handler := client.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("kaboom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 response, got %d", rec.Code)
	}
	var event *core.ErrorEventPayload
	for _, e := range events {
		if e.Exception.Value == "kaboom" {
			event = e
		}
	}
	if event == nil {
		t.Fatal("panic was not captured")
	}
	if event.HTTP == nil || event.HTTP.StatusCode != 500 {
		t.Fatalf("status not stamped onto event: %+v", event.HTTP)
	}
	if event.Exception.Mechanism != "middleware" {
		t.Fatalf("unexpected mechanism: %q", event.Exception.Mechanism)
	}
}

func TestMiddlewareAdoptsTraceparent(t *testing.T) {
	var events []*core.ErrorEventPayload
	client := collectingClient(&events)

	traceID := "abcdabcdabcdabcdabcdabcdabcdabcd"
	handler := client.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client.CaptureException(r.Context(), errFromString("with-trace"))
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Traceparent", "00-"+traceID+"-1111111111111111-01")
	req.Header.Set("Reelay-Session-Id", "sess_123")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	e := events[0]
	if e.Trace == nil || e.Trace.TraceID != traceID {
		t.Fatalf("trace not adopted: %+v", e.Trace)
	}
	if e.SessionID != "sess_123" {
		t.Fatalf("session not adopted: %q", e.SessionID)
	}
}

func TestTimelineLogsAttachToRequest(t *testing.T) {
	var events []*core.ErrorEventPayload
	client := collectingClient(&events)
	ctx := ContextWith(context.Background(), newRequestContext("t"))

	AppendLog(ctx, "info", "calling payment provider")
	AppendSpan(ctx, core.TimelineSpan{SpanID: "s1", Name: "payments.charge", Status: "failed"})
	client.CaptureException(ctx, errFromString("with-timeline"))

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	tl := events[0].Timeline
	if tl == nil || len(tl.Logs) != 1 || tl.Logs[0].Msg != "calling payment provider" {
		t.Fatalf("logs not attached: %+v", tl)
	}
	if len(tl.Spans) != 1 || tl.Spans[0].Name != "payments.charge" {
		t.Fatalf("spans not attached: %+v", tl)
	}
}

func TestCaptureMessageSyntheticType(t *testing.T) {
	var events []*core.ErrorEventPayload
	client := collectingClient(&events)
	client.CaptureMessage(context.Background(), "fallback mode")
	if len(events) != 1 || events[0].Exception.Mechanism != "message" {
		t.Fatalf("unexpected capture: %+v", events)
	}
	if events[0].Exception.Value != "fallback mode" {
		t.Fatalf("unexpected message value: %q", events[0].Exception.Value)
	}
}

// hasCrumb reports whether an event carries a breadcrumb with the given message.
func hasCrumb(e *core.ErrorEventPayload, msg string) bool {
	for _, c := range e.Breadcrumbs {
		if c.Message == msg {
			return true
		}
	}
	return false
}

// errFromString is a tiny error helper so test assertions can match on value.
type stringError string

func (s stringError) Error() string { return string(s) }
func errFromString(s string) error  { return stringError(s) }
