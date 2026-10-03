package core

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Resilient, non-blocking transport:
//   - never panics into the host application;
//   - non-blocking in-memory queue that retains bursts until delivery;
//   - exponential backoff with full jitter;
//   - honors 429 Retry-After (capped);
//   - drops (never retries) 4xx payload errors — they will never succeed;
//   - fully cancellable: Close stops the drain goroutine even mid-backoff, so
//     the SDK can never leak a goroutine or hold a host past shutdown.
//
// The Node SDK relies on a single-threaded event loop to serialize its drain
// loop. Go uses a goroutine guarded by a mutex; a sync.Cond lets Flush block
// until the queue is fully drained (process exit depends on this).

const (
	baseDelay        = 1 * time.Second
	maxDelay         = 30 * time.Second
	requestTimeout   = 10 * time.Second
	defaultRetryWait = 5 * time.Second
	// maxRetryAfter caps an honored Retry-After: a misconfigured proxy
	// replying with an hours-long value must not park the whole queue.
	maxRetryAfter = 60 * time.Second
)

// TransportOptions configures a Transport.
type TransportOptions struct {
	URL       string
	IngestKey string
	NodeID    string
	// Token is the deprecated name for IngestKey, kept for direct transport
	// users while SDK clients migrate to the explicit field.
	Token string
	// MaxQueueSize is retained for compatibility; it no longer caps the queue.
	MaxQueueSize int
	// HTTPClient is injectable for tests. nil uses a default client.
	HTTPClient *http.Client
	Debug      func(message string, detail any)
}

// queuedItem is heap-allocated so the drain loop can recognize the item it
// just delivered after concurrent Sends appended to the queue.
type queuedItem struct {
	body     []byte
	attempts int
}

type outcome int

const (
	outcomeOK outcome = iota
	outcomeRetry
	outcomeDrop
)

// Transport delivers serialized event payloads with retries and backoff.
type Transport struct {
	opts   TransportOptions
	client *http.Client

	mu          sync.Mutex
	cond        *sync.Cond
	queue       []*queuedItem
	flushing    bool
	closed      bool
	pausedUntil time.Time

	stop      chan struct{}
	closeOnce sync.Once
}

// NewTransport builds a Transport. MaxQueueSize <= 0 defaults to 30.
func NewTransport(opts TransportOptions) *Transport {
	if opts.MaxQueueSize <= 0 {
		opts.MaxQueueSize = 30
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	t := &Transport{opts: opts, client: client, stop: make(chan struct{})}
	t.cond = sync.NewCond(&t.mu)
	return t
}

func (t *Transport) debug(msg string, detail any) {
	if t.opts.Debug != nil {
		t.opts.Debug(msg, detail)
	}
}

// Pending reports how many payloads are waiting to be delivered.
func (t *Transport) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.queue)
}

// Send enqueues a payload and starts the drain goroutine if idle. It never
// blocks or drops an event merely because a burst exceeds MaxQueueSize.
// After Close, sends are dropped (with a debug signal).
func (t *Transport) Send(payload any) {
	if t.opts.URL == "" {
		t.debug("event dropped: endpoint must be HTTPS", nil)
		return
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.debug("serialize failed", err)
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.debug("transport closed; event dropped", nil)
		return
	}
	t.queue = append(t.queue, &queuedItem{body: body})
	if !t.flushing {
		t.flushing = true
		go t.drain()
	}
	t.mu.Unlock()
}

// Flush blocks until the queue is fully drained (or the transport is closed).
// Safe to call concurrently.
func (t *Transport) Flush() {
	t.flushCtx(context.Background())
}

// FlushTimeout blocks until the queue drains or the timeout elapses, returning
// true if the queue drained in time. Used on the fatal crash path.
func (t *Transport) FlushTimeout(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return t.flushCtx(ctx)
}

// Close drains until ctx expires, then stops the drain goroutine — even one
// parked in a backoff sleep — and releases every Flush waiter. Idempotent.
// Returns true when the queue drained fully before shutdown.
func (t *Transport) Close(ctx context.Context) bool {
	drained := t.flushCtx(ctx)
	t.closeOnce.Do(func() { close(t.stop) })
	t.mu.Lock()
	t.closed = true
	t.cond.Broadcast()
	t.mu.Unlock()
	return drained
}

// flushCtx waits for a full drain, bounded by ctx, and reports whether the
// queue actually drained. The wait happens on the calling goroutine — no
// helper goroutine that could outlive the deadline — with ctx expiry turned
// into a cond broadcast so the wait loop re-checks and returns promptly.
func (t *Transport) flushCtx(ctx context.Context) bool {
	stop := context.AfterFunc(ctx, func() {
		t.mu.Lock()
		t.cond.Broadcast()
		t.mu.Unlock()
	})
	defer stop()

	t.mu.Lock()
	defer t.mu.Unlock()
	for (len(t.queue) > 0 || t.flushing) && !t.closed && ctx.Err() == nil {
		t.cond.Wait()
	}
	return len(t.queue) == 0 && !t.flushing
}

// sleep waits d, returning false if the transport was closed meanwhile.
func (t *Transport) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.stop:
		return false
	}
}

// exitDrain releases the flushing flag and wakes Flush waiters.
func (t *Transport) exitDrain() {
	t.mu.Lock()
	t.flushing = false
	t.cond.Broadcast()
	t.mu.Unlock()
}

func (t *Transport) drain() {
	for {
		t.mu.Lock()
		if t.closed || len(t.queue) == 0 {
			t.flushing = false
			t.cond.Broadcast()
			t.mu.Unlock()
			return
		}
		wait := time.Until(t.pausedUntil)
		item := t.queue[0]
		t.mu.Unlock()

		if wait > 0 && !t.sleep(wait) {
			t.exitDrain()
			return
		}

		result := t.deliver(item.body)

		t.mu.Lock()
		// Mutate only the item this attempt actually delivered.
		stillHead := len(t.queue) > 0 && t.queue[0] == item
		retrying := false
		if stillHead {
			if result == outcomeOK || result == outcomeDrop {
				t.queue[0] = nil // release the payload before slicing the backing array
				t.queue = t.queue[1:]
				if len(t.queue) == 0 {
					t.queue = nil
				}
			} else {
				// Retry transient failures while the transport is open.
				// The backoff is capped, so the counter can be capped too.
				if item.attempts < 6 {
					item.attempts++
				}
				retrying = true
			}
		}
		t.mu.Unlock()

		if retrying && !t.sleep(backoff(item.attempts)) {
			t.exitDrain()
			return
		}
	}
}

func (t *Transport) deliver(body []byte) outcome {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.opts.URL, bytes.NewReader(body))
	if err != nil {
		t.debug("request build failed", err)
		return outcomeDrop
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "reelay-go-sdk/0.1")
	key := t.opts.IngestKey
	if key == "" {
		key = t.opts.Token
	}
	req.Header.Set("X-Reelay-Token", key)
	req.Header.Set("X-Reelay-Node-ID", t.opts.NodeID)

	res, err := t.client.Do(req)
	if err != nil {
		t.debug("network error", err) // offline / transient / timeout
		return outcomeRetry
	}
	// Drain (bounded) and close on every path so the connection returns to
	// the pool for keep-alive reuse instead of being torn down.
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<14))
		_ = res.Body.Close()
	}()

	switch {
	case res.StatusCode == http.StatusTooManyRequests: // 429
		retryAfter := defaultRetryWait
		if v, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && v > 0 {
			retryAfter = time.Duration(v) * time.Second
			if retryAfter > maxRetryAfter {
				retryAfter = maxRetryAfter
			}
		}
		t.mu.Lock()
		t.pausedUntil = time.Now().Add(retryAfter)
		t.mu.Unlock()
		return outcomeRetry
	case res.StatusCode >= 500:
		return outcomeRetry
	case res.StatusCode == http.StatusRequestTimeout || res.StatusCode == 425: // 408 / 425 Too Early
		return outcomeRetry
	case res.StatusCode >= 400:
		reason, _ := io.ReadAll(io.LimitReader(res.Body, 1<<12))
		t.debug("event rejected with "+strconv.Itoa(res.StatusCode), string(reason))
		return outcomeDrop // malformed/unauthorized: retrying cannot succeed
	default:
		return outcomeOK
	}
}

// backoff returns an exponential backoff with full jitter, capped at maxDelay.
func backoff(attempt int) time.Duration {
	ceiling := baseDelay << (attempt - 1)
	if ceiling > maxDelay || ceiling <= 0 {
		ceiling = maxDelay
	}
	return time.Duration(rand.Int63n(int64(ceiling)))
}
