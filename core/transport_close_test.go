package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// The graceful-shutdown contract: Close drains what it can within its
// deadline, then stops the drain goroutine even mid-backoff, and later sends
// are dropped safely.

func TestCloseDrainsPendingEvents(t *testing.T) {
	var received atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "t", MaxQueueSize: 10})
	tr.Send(map[string]string{"event_id": "evt_1"})
	tr.Send(map[string]string{"event_id": "evt_2"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !tr.Close(ctx) {
		t.Fatal("Close should report a full drain against a healthy endpoint")
	}
	if got := received.Load(); got != 2 {
		t.Fatalf("want 2 deliveries before close, got %d", got)
	}
}

func TestCloseInterruptsBackoffSleep(t *testing.T) {
	// Endpoint always 500s → the drain loop parks in exponential backoff.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "t", MaxQueueSize: 10})
	tr.Send(map[string]string{"event_id": "evt_stuck"})
	time.Sleep(50 * time.Millisecond) // let the drain hit its first backoff

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	drained := tr.Close(ctx)
	elapsed := time.Since(start)

	if drained {
		t.Fatal("Close cannot report drained against a failing endpoint")
	}
	// Without the stop channel this would block for the full backoff ladder
	// (up to ~30s per attempt); with it, Close returns at the ctx deadline.
	if elapsed > 2*time.Second {
		t.Fatalf("Close took %v; a parked backoff must be interruptible", elapsed)
	}

	// The drain goroutine must actually exit: flushing resets and Flush
	// returns immediately on a closed transport.
	done := make(chan struct{})
	go func() {
		tr.Flush()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Flush must not block after Close")
	}
}

func TestSendAfterCloseIsDropped(t *testing.T) {
	var received atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tr := NewTransport(TransportOptions{URL: srv.URL, Token: "t", MaxQueueSize: 10})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tr.Close(ctx)

	tr.Send(map[string]string{"event_id": "evt_late"}) // must not panic or hang
	time.Sleep(50 * time.Millisecond)
	if got := received.Load(); got != 0 {
		t.Fatalf("no deliveries expected after close, got %d", got)
	}
	if tr.Pending() != 0 {
		t.Fatalf("late sends must be dropped, %d queued", tr.Pending())
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	tr := NewTransport(TransportOptions{URL: "http://127.0.0.1:0", Token: "t", MaxQueueSize: 10})
	ctx := context.Background()
	tr.Close(ctx)
	tr.Close(ctx) // second close must not panic (closed stop channel)
}
