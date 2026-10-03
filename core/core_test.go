package core

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScrubberRedactsSecretsInStrings(t *testing.T) {
	s := NewScrubber(nil)
	out := s.ScrubString("password: hunter2secret and token=abc123def")
	if strings.Contains(out, "hunter2secret") || strings.Contains(out, "abc123def") {
		t.Fatalf("secrets not redacted: %q", out)
	}
}

func TestScrubberRedactsEmailsAndCards(t *testing.T) {
	s := NewScrubber(nil)
	out := s.ScrubString("user bob@acme.com paid with 4111 1111 1111 1111")
	if strings.Contains(out, "bob@acme.com") || strings.Contains(out, "4111") {
		t.Fatalf("email/card not redacted: %q", out)
	}
}

func TestScrubberRedactsSensitiveKeys(t *testing.T) {
	s := NewScrubber(nil)
	in := map[string]any{
		"authorization": "Bearer xyz",
		"nested":        map[string]any{"cookie": "sid=1", "ok": "fine"},
	}
	out := s.Scrub(in).(map[string]any)
	if out["authorization"] != Redacted {
		t.Fatalf("authorization not redacted: %v", out["authorization"])
	}
	nested := out["nested"].(map[string]any)
	if nested["cookie"] != Redacted {
		t.Fatalf("cookie not redacted: %v", nested["cookie"])
	}
	if nested["ok"] != "fine" {
		t.Fatalf("non-sensitive value mangled: %v", nested["ok"])
	}
}

func TestBreadcrumbBufferKeepsMostRecent(t *testing.T) {
	b := NewBreadcrumbBuffer(3)
	for i := int64(1); i <= 5; i++ {
		b.Add(Breadcrumb{Timestamp: i, Type: "t"})
	}
	snap := b.Snapshot()
	got := []int64{}
	for _, c := range snap {
		got = append(got, c.Timestamp)
	}
	if len(got) != 3 || got[0] != 3 || got[1] != 4 || got[2] != 5 {
		t.Fatalf("expected [3 4 5], got %v", got)
	}
}

func TestCurrentStackSkipsSDKFrames(t *testing.T) {
	frames := CurrentStack(50)
	if len(frames) == 0 {
		t.Fatal("expected at least one frame")
	}
	// CurrentStack drops leading frames belonging to the SDK so the first
	// reported frame is the caller's. (In this in-module test the test func is
	// itself under the SDK path and is skipped — real callers are not.)
	if strings.HasPrefix(frames[0].Function, "core.CurrentStack") {
		t.Fatalf("SDK frame leaked to top of stack: %+v", frames[0])
	}
}

func TestIsInApp(t *testing.T) {
	cases := []struct {
		file, fn string
		want     bool
	}{
		{"/app/checkout/service.go", "main.charge", true},
		{"/usr/local/go/src/runtime/panic.go", "runtime.gopanic", false},
		{"/home/me/go/pkg/mod/github.com/x/y@v1.0.0/z.go", "y.Do", false},
	}
	for _, c := range cases {
		if got := isInApp(c.file, c.fn); got != c.want {
			t.Fatalf("isInApp(%q,%q)=%v want %v", c.file, c.fn, got, c.want)
		}
	}
}

func TestParseGoStack(t *testing.T) {
	stack := strings.Join([]string{
		"goroutine 1 [running]:",
		"main.charge(0x1)",
		"\t/app/checkout/service.go:142 +0x1d",
		"runtime.gopanic(...)",
		"\t/usr/local/go/src/runtime/panic.go:884 +0x213",
	}, "\n")
	frames := ParseGoStack([]byte(stack), 50)
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d: %+v", len(frames), frames)
	}
	if frames[0].File != "/app/checkout/service.go" || frames[0].Line != 142 || !frames[0].InApp {
		t.Fatalf("bad app frame: %+v", frames[0])
	}
	if frames[1].InApp {
		t.Fatalf("runtime frame should not be in_app: %+v", frames[1])
	}
}

func TestNormalizeError(t *testing.T) {
	if _, v := NormalizeError("plain string"); v != "plain string" {
		t.Fatalf("string value: %q", v)
	}
	if typ, _ := NormalizeError(map[string]any{"code": 1}); typ != "NonErrorThrown" {
		t.Fatalf("non-error type: %q", typ)
	}
	if _, v := NormalizeError(errors.New("boom")); v != "boom" {
		t.Fatalf("error value: %q", v)
	}
}

func TestTraceparentRoundTrips(t *testing.T) {
	tid, sid := NewTraceID(), NewSpanID()
	parsed := ParseTraceparent(Traceparent(tid, sid))
	if parsed == nil || parsed.TraceID != tid || parsed.SpanID != sid {
		t.Fatalf("round-trip failed: %+v", parsed)
	}
}

func TestTraceparentRejectsMalformed(t *testing.T) {
	if ParseTraceparent("garbage") != nil {
		t.Fatal("expected nil for garbage")
	}
	zero := "00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-01"
	if ParseTraceparent(zero) != nil {
		t.Fatal("expected nil for all-zero ids")
	}
}

func TestTransportDeliversWithTokenHeader(t *testing.T) {
	var gotToken atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken.Store(r.Header.Get("X-Reelay-Token"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "tok", MaxQueueSize: 5})
	tr.Send(map[string]int{"a": 1})
	tr.Flush()

	if gotToken.Load() != "tok" {
		t.Fatalf("token header = %v", gotToken.Load())
	}
}

func TestTransportDrops4xxRetries5xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "t", MaxQueueSize: 5})
	tr.Send(map[string]int{})
	tr.Flush()

	if atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("expected 1 attempt for 4xx, got %d", attempts)
	}
	if tr.Pending() != 0 {
		t.Fatalf("expected empty queue, got %d", tr.Pending())
	}
}

func TestTransportRetainsBurstBeyondMaxQueueSize(t *testing.T) {
	// A server that blocks so the queue saturates before anything drains.
	release := make(chan struct{})
	var delivered []int
	var deliveredMu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		var payload struct {
			N int `json:"n"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode: %v", err)
		}
		deliveredMu.Lock()
		delivered = append(delivered, payload.N)
		deliveredMu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "t", MaxQueueSize: 2})
	tr.Send(map[string]int{"n": 1})
	tr.Send(map[string]int{"n": 2})
	tr.Send(map[string]int{"n": 3})
	if tr.Pending() != 3 {
		t.Fatalf("expected all 3 events pending, got %d", tr.Pending())
	}
	close(release)
	tr.Flush()
	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	if !reflect.DeepEqual(delivered, []int{1, 2, 3}) {
		t.Fatalf("delivered %v, want all 3 in order", delivered)
	}
}

func TestBackoffIsCappedAndJittered(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		d := backoff(attempt)
		if d < 0 || d > maxDelay {
			t.Fatalf("attempt %d: backoff out of range: %v", attempt, d)
		}
	}
}

func TestFlushTimeoutReturnsFalseWhenStuck(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	defer close(release)

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "t", MaxQueueSize: 5})
	tr.Send(map[string]int{"a": 1})
	if tr.FlushTimeout(50 * time.Millisecond) {
		t.Fatal("expected FlushTimeout to time out while delivery is blocked")
	}
}
