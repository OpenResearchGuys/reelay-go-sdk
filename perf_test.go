package reelay

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

func TestParameterizePath(t *testing.T) {
	cases := map[string]string{
		"/users/123/orders":   "/users/{id}/orders",
		"/items/deadbeefcafe": "/items/{id}",
		"/checkout?step=2":    "/checkout",
		"/health":             "/health",
	}
	for in, want := range cases {
		if got := core.ParameterizePath(in); got != want {
			t.Errorf("ParameterizePath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestActiveTransaction(t *testing.T) {
	txn := core.NewActiveTransaction("GET /x", "http.server", "t", "s", "p")
	now := time.Now()
	txn.AddSpan("db.query", "SELECT 1", now.Add(-5*time.Millisecond), now, "ok", nil, "")
	txn.SetHTTPStatus(502)

	payload := txn.Finish("txn_1", "backend", "go", "", 0.5)
	if payload == nil {
		t.Fatal("nil payload")
	}
	if payload.Status != "error" {
		t.Errorf("status = %q, want error (5xx)", payload.Status)
	}
	if payload.SampleRate != 0.5 || payload.HTTPStatus != 502 {
		t.Errorf("payload = %+v", payload)
	}
	if len(payload.Spans) != 1 || payload.Spans[0].Op != "db.query" {
		t.Errorf("spans = %+v", payload.Spans)
	}
	if txn.Finish("txn_2", "backend", "go", "", 1) != nil {
		t.Error("double finish returned a payload")
	}
	// Spans after finish are dropped.
	txn.AddSpan("late", "", now, now, "", nil, "")
	if len(payload.Spans) != 1 {
		t.Errorf("span recorded after finish: %+v", payload.Spans)
	}
}

func TestWithSpan(t *testing.T) {
	txn := core.NewActiveTransaction("job", "task", "t", "s", "")
	rc := &RequestContext{Crumbs: core.NewBreadcrumbBuffer(5), Txn: txn}
	ctx := ContextWith(context.Background(), rc)

	if err := WithSpan(ctx, "db.query", "SELECT 1", func(ctx context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	wantErr := fmt.Errorf("boom")
	if err := WithSpan(ctx, "db.query", "", func(ctx context.Context) error { return wantErr }); err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	// A panicking fn seals the span as error, then the panic propagates.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic did not propagate through WithSpan")
			}
		}()
		_ = WithSpan(ctx, "db.query", "", func(ctx context.Context) error { panic("kaboom") })
	}()
	// No transaction in ctx: fn just runs.
	if err := WithSpan(context.Background(), "op", "", func(ctx context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	payload := txn.Finish("txn_1", "backend", "go", "", 1)
	if len(payload.Spans) != 3 {
		t.Fatalf("got %d spans, want 3", len(payload.Spans))
	}
	if payload.Spans[0].Status != "ok" || payload.Spans[1].Status != "error" || payload.Spans[2].Status != "error" {
		t.Errorf("span statuses = %q %q %q", payload.Spans[0].Status, payload.Spans[1].Status, payload.Spans[2].Status)
	}
}

func TestStartSpanRecordsTimedAPMSpanOnce(t *testing.T) {
	traceID := strings.Repeat("a", 32)
	transactionSpanID := strings.Repeat("b", 16)
	txn := core.NewActiveTransaction("job", "task", traceID, transactionSpanID, "")
	rc := &RequestContext{Crumbs: core.NewBreadcrumbBuffer(5), Txn: txn}
	ctx := ContextWith(context.Background(), rc)

	finish := StartSpan(ctx, "db.query", "inventory.reserveStockInMemory")
	time.Sleep(time.Millisecond)
	finish("ok")
	finish("error") // Finishing twice must not duplicate or overwrite the span.

	payload := txn.Finish("txn_manual", "backend", "go", "", 1)
	if len(payload.Spans) != 1 {
		t.Fatalf("got %d APM spans, want 1", len(payload.Spans))
	}
	span := payload.Spans[0]
	if span.Op != "db.query" || span.Description != "inventory.reserveStockInMemory" {
		t.Errorf("span identity = %q %q", span.Op, span.Description)
	}
	if span.Status != "ok" {
		t.Errorf("span status = %q, want ok", span.Status)
	}
	if span.ParentSpanID != transactionSpanID {
		t.Errorf("span parent = %q, want transaction span %q", span.ParentSpanID, transactionSpanID)
	}
	startedAt, startErr := time.Parse(time.RFC3339Nano, span.StartedAt)
	endedAt, endErr := time.Parse(time.RFC3339Nano, span.EndedAt)
	if startErr != nil || endErr != nil || !endedAt.After(startedAt) {
		t.Errorf("span timing = %q to %q, want a positive duration", span.StartedAt, span.EndedAt)
	}

	timeline := rc.timelineSnapshot()
	if len(timeline.Spans) != 1 {
		t.Fatalf("got %d timeline spans, want 1", len(timeline.Spans))
	}
	if timeline.Spans[0].SpanID != span.SpanID || timeline.Spans[0].Status != "ok" {
		t.Errorf("timeline span = %+v, APM span = %+v", timeline.Spans[0], span)
	}
}

func TestWithSpanPreservesNestedParent(t *testing.T) {
	txn := core.NewActiveTransaction("job", "task", strings.Repeat("a", 32), strings.Repeat("b", 16), "")
	ctx := ContextWith(context.Background(), &RequestContext{Crumbs: core.NewBreadcrumbBuffer(5), Txn: txn})
	if err := WithSpan(ctx, "app.outer", "", func(ctx context.Context) error {
		return WithSpan(ctx, "app.inner", "", func(ctx context.Context) error { return nil })
	}); err != nil {
		t.Fatal(err)
	}
	spans := txn.Finish("txn_nested", "backend", "go", "", 1).Spans
	if len(spans) != 2 {
		t.Fatalf("got %d spans", len(spans))
	}
	var outer, inner core.SpanPayload
	for _, span := range spans {
		if span.Op == "app.outer" {
			outer = span
		}
		if span.Op == "app.inner" {
			inner = span
		}
	}
	if outer.ParentSpanID != strings.Repeat("b", 16) {
		t.Errorf("outer parent = %q", outer.ParentSpanID)
	}
	if inner.ParentSpanID != outer.SpanID {
		t.Errorf("inner parent = %q, outer = %q", inner.ParentSpanID, outer.SpanID)
	}
}

func TestWithTransactionPanicRecordsAndRepanics(t *testing.T) {
	cs, srv := newCaptureServer()
	defer srv.Close()
	client := NewClient(Options{Endpoint: srv.URL, Token: "tok", AllowInsecureHTTPForTesting: true})

	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic did not propagate through WithTransaction")
			}
		}()
		_ = client.WithTransaction(context.Background(), "reindex", "task", func(ctx context.Context) error {
			panic("kaboom")
		})
	}()
	client.Flush()

	batches := cs.get("/api/ingest/transactions")
	if len(batches) != 1 {
		t.Fatalf("got %d transaction batches, want 1 (panicking txn lost)", len(batches))
	}
	txn := batches[0]["transactions"].([]any)[0].(map[string]any)
	if txn["status"] != "error" {
		t.Errorf("status = %v, want error", txn["status"])
	}
	errs := cs.get("/api/ingest/errors")
	if len(errs) != 1 {
		t.Fatalf("got %d error events, want 1 (panic not captured)", len(errs))
	}
}

// captureServer records JSON bodies per path.
type captureServer struct {
	mu     sync.Mutex
	bodies map[string][]map[string]any
}

func newCaptureServer() (*captureServer, *httptest.Server) {
	cs := &captureServer{bodies: map[string][]map[string]any{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		cs.mu.Lock()
		cs.bodies[r.URL.Path] = append(cs.bodies[r.URL.Path], body)
		cs.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}))
	return cs, srv
}

func (cs *captureServer) get(path string) []map[string]any {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.bodies[path]
}

func TestMiddlewareRecordsSampledTransaction(t *testing.T) {
	cs, srv := newCaptureServer()
	defer srv.Close()

	rate := 1.0
	client := NewClient(Options{Endpoint: srv.URL, Token: "tok", TracesSampleRate: &rate, AllowInsecureHTTPForTesting: true})

	handler := client.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = WithSpan(r.Context(), "db.query", "SELECT 1", func(ctx context.Context) error { return nil })
		SetTransactionName(r.Context(), "GET /users/{user_id}")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/users/42", nil)
	req.Header.Set("Traceparent", "00-"+strings.Repeat("a", 32)+"-"+strings.Repeat("b", 16)+"-01")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	client.Flush()

	batches := cs.get("/api/ingest/transactions")
	if len(batches) != 1 {
		t.Fatalf("got %d transaction batches, want 1", len(batches))
	}
	txns := batches[0]["transactions"].([]any)
	if len(txns) != 1 {
		t.Fatalf("got %d transactions", len(txns))
	}
	txn := txns[0].(map[string]any)
	if txn["name"] != "GET /users/{user_id}" {
		t.Errorf("name = %v", txn["name"])
	}
	if txn["trace_id"] != strings.Repeat("a", 32) {
		t.Errorf("trace_id = %v (traceparent not adopted)", txn["trace_id"])
	}
	if txn["parent_span_id"] != strings.Repeat("b", 16) {
		t.Errorf("parent_span_id = %v", txn["parent_span_id"])
	}
	spans := txn["spans"].([]any)
	if len(spans) != 1 {
		t.Fatalf("spans = %v", spans)
	}
}

func TestMiddlewareHonorsUnsampledRemoteParent(t *testing.T) {
	cs, srv := newCaptureServer()
	defer srv.Close()
	rate := 1.0
	client := NewClient(Options{Endpoint: srv.URL, Token: "tok", TracesSampleRate: &rate, AllowInsecureHTTPForTesting: true})
	handler := client.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	req := httptest.NewRequest(http.MethodGet, "/users/42", nil)
	req.Header.Set("Traceparent", "00-"+strings.Repeat("a", 32)+"-"+strings.Repeat("b", 16)+"-00")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	client.Flush()
	if batches := cs.get("/api/ingest/transactions"); len(batches) != 0 {
		t.Fatalf("unsampled remote parent produced %d transaction batches", len(batches))
	}
}

func TestWithMonitorCheckIns(t *testing.T) {
	cs, srv := newCaptureServer()
	defer srv.Close()

	client := NewClient(Options{Endpoint: srv.URL, Token: "tok", AllowInsecureHTTPForTesting: true})
	err := client.WithMonitor(context.Background(), "nightly", &MonitorConfig{
		Schedule: MonitorSchedule{Type: "crontab", Crontab: "0 2 * * *"},
	}, func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = client.WithMonitor(context.Background(), "nightly", nil, func(ctx context.Context) error { return fmt.Errorf("boom") })
	client.Flush()

	checkins := cs.get("/api/ingest/checkins")
	if len(checkins) != 4 {
		t.Fatalf("got %d check-ins, want 4", len(checkins))
	}
	if checkins[0]["status"] != "in_progress" || checkins[0]["monitor_config"] == nil {
		t.Errorf("first check-in = %v", checkins[0])
	}
	if checkins[1]["status"] != "ok" || checkins[1]["check_in_id"] != checkins[0]["check_in_id"] {
		t.Errorf("terminal check-in = %v", checkins[1])
	}
	if checkins[3]["status"] != "error" {
		t.Errorf("failed job check-in = %v", checkins[3])
	}
}

func TestParseGoroutineDump(t *testing.T) {
	dump := "goroutine 1 [running]:\n" +
		"main.work(0x1)\n" +
		"\t/app/main.go:10 +0x1a\n" +
		"main.main()\n" +
		"\t/app/main.go:20 +0x2b\n" +
		"\n" +
		"goroutine 7 [chan receive]:\n" +
		"main.idle()\n" +
		"\t/app/idle.go:5 +0x10\n"
	stacks := parseGoroutineDump(dump)
	if len(stacks) != 1 {
		t.Fatalf("got %d stacks, want 1 (parked goroutine skipped)", len(stacks))
	}
	frames := stacks[0]
	if frames[0].Function != "main.main" || frames[1].Function != "main.work" {
		t.Errorf("frames not root-first: %+v", frames)
	}
	if frames[1].File != "/app/main.go" || frames[1].Line != 10 {
		t.Errorf("location parse: %+v", frames[1])
	}
	if !frames[0].InApp {
		t.Error("main.main should be in_app")
	}
}
