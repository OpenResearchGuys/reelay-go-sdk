package reelay

import (
	"context"
	"time"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// Performance monitoring, cron-monitor check-ins, and continuous profiling —
// the Client-side surface over the wire types in core/perf.go.

// Re-exported so callers need only import this package.
type (
	// TransactionPayload is the wire body element of /api/ingest/transactions.
	TransactionPayload = core.TransactionPayload
	// ActiveTransaction accumulates one in-flight unit of work and its spans.
	ActiveTransaction = core.ActiveTransaction
	// MonitorConfig lets a check-in create/update its monitor in-band.
	MonitorConfig = core.MonitorConfig
	// MonitorSchedule is the crontab/interval schedule shape.
	MonitorSchedule = core.MonitorSchedule
	// CheckInPayload is one cron-monitor check-in.
	CheckInPayload = core.CheckInPayload
)

// perf holds the Client's performance-monitoring machinery, built once in
// NewClient. Separate transports keep a burst of transactions or a large
// profile chunk from ever delaying an error event.
type perf struct {
	txns     *core.TxnBuffer
	checkins *core.Transport
	profiles *core.Transport
	profiler *continuousProfiler
}

func newPerf(opts Options) *perf {
	base := core.StripTrailingSlash(opts.Endpoint)
	key := opts.IngestKey
	if key == "" {
		key = opts.Token
	}
	p := &perf{
		txns: core.NewTxnBuffer(core.NewTransport(core.TransportOptions{
			URL:          base + "/api/ingest/transactions",
			IngestKey:    key,
			NodeID:       opts.NodeID,
			MaxQueueSize: opts.MaxQueueSize,
			Debug:        opts.Debug,
		}), 0, 0),
		checkins: core.NewTransport(core.TransportOptions{
			URL:          base + "/api/ingest/checkins",
			IngestKey:    key,
			NodeID:       opts.NodeID,
			MaxQueueSize: 20,
			Debug:        opts.Debug,
		}),
		profiles: core.NewTransport(core.TransportOptions{
			URL:          base + "/api/ingest/profiles",
			IngestKey:    key,
			NodeID:       opts.NodeID,
			MaxQueueSize: 5, // profiles are big; shed rather than hoard
			Debug:        opts.Debug,
		}),
	}
	if opts.Profiling {
		p.profiler = newContinuousProfiler(opts.ProfileWindow, opts.ProfileCycle,
			func(chunk core.ProfileChunkPayload) {
				chunk.Release = opts.Release
				chunk.CommitSHA = opts.CommitSHA
				p.profiles.Send(chunk)
			}, opts.Debug)
		p.profiler.start()
	}
	return p
}

// tracesSampled makes one head-based sampling decision.
func (c *Client) tracesSampled(traceID string) bool {
	rate := c.tracesSampleRate()
	return core.ShouldSampleTrace(traceID, rate)
}

func (c *Client) tracesSampleRate() float64 {
	if c.Options.TracesSampleRate == nil {
		return 0
	}
	rate := *c.Options.TracesSampleRate
	if rate > 1 {
		return 1
	}
	return rate
}

// RecordTransaction finalizes and enqueues a transaction (batched; flushed
// within one second). The middleware calls this; it is public for custom
// instrumentation that manages its own ActiveTransaction.
func (c *Client) RecordTransaction(txn *core.ActiveTransaction, sessionID string, effectiveRate ...float64) {
	c.Guard(func() {
		rate := c.tracesSampleRate()
		if len(effectiveRate) > 0 && effectiveRate[0] > 0 && effectiveRate[0] <= 1 {
			rate = effectiveRate[0]
		}
		if rate <= 0 {
			rate = 1
		}
		payload := txn.Finish("txn_"+core.NewEventID()[4:], c.Kind, c.Platform, sessionID, rate)
		if payload != nil {
			payload.Release = c.Release
			payload.CommitSHA = c.CommitSHA
			c.perf.txns.Push(*payload)
		}
	})
}

// WithSpan runs fn as a child span of ctx's sampled transaction. The span is
// measured around fn and recorded however fn ends — return, error, or panic
// (the span closes with status "error" before the panic continues, so a
// panicking handler can never leak a half-open span). When ctx carries no
// sampled transaction, fn simply runs. fn's error is returned unchanged.
//
//	err := reelay.WithSpan(ctx, "db.query", "SELECT * FROM users", func(ctx context.Context) error {
//	    return db.QueryRowContext(ctx, query).Scan(&count)
//	})
//
// There is no open-span handle to forget to end: this wrapper is the only way
// to create a timed user span.
func WithSpan(ctx context.Context, op, description string, fn func(ctx context.Context) error) error {
	rc := FromContext(ctx)
	if rc == nil || rc.Txn == nil {
		return fn(ctx)
	}
	txn := rc.Txn
	spanID := core.NewSpanID()
	parentSpanID := activeSpanFromContext(ctx)
	workCtx := context.WithValue(ctx, activeSpanKey, spanID)
	started := time.Now()
	completed := false
	defer func() {
		if !completed {
			// fn is panicking: seal the span as an error, then let the panic
			// propagate untouched to the recovery boundary (middleware/Recover).
			txn.AddChildSpan(op, description, started, time.Now(), "error", nil, spanID, parentSpanID)
		}
	}()
	err := fn(workCtx)
	completed = true
	status := "ok"
	if err != nil {
		status = "error"
	}
	txn.AddChildSpan(op, description, started, time.Now(), status, nil, spanID, parentSpanID)
	return err
}

// SetTransactionName overrides ctx's transaction name with a route pattern
// (call it inside a handler when your router knows the pattern, e.g.
// chi.RouteContext(ctx).RoutePattern()).
func SetTransactionName(ctx context.Context, name string) {
	if rc := FromContext(ctx); rc != nil && rc.Txn != nil {
		rc.Txn.SetName(name)
	}
}

// WithTransaction times an arbitrary unit of work (queue job, script) as its
// own transaction, independent of any HTTP request. Always recorded (the
// caller decided to measure it), with sample_rate 1. The worked ctx carries
// the transaction, so WithSpan works inside fn.
//
// A panicking fn is handled like a panicking HTTP handler: the panic is
// captured with the job's context, the transaction is finalized with status
// "error", and the panic then propagates untouched — crash semantics belong
// to the caller's recovery boundary, not to instrumentation.
func (c *Client) WithTransaction(ctx context.Context, name, op string, fn func(ctx context.Context) error) error {
	if op == "" {
		op = "task"
	}
	traceID := core.NewTraceID()
	spanID := core.NewSpanID()
	txn := core.NewActiveTransaction(name, op, traceID, spanID, "")
	// A fresh RequestContext scopes the transaction; the parent request's
	// context (if any) contributes its session id and breadcrumb buffer but
	// keeps its own transaction untouched.
	rc := &RequestContext{
		Trace:                   core.TraceContext{TraceID: traceID, SpanID: spanID},
		Crumbs:                  core.NewBreadcrumbBuffer(50),
		Txn:                     txn,
		TraceSampleRate:         1,
		TracePropagationEnabled: true,
	}
	sessionID := ""
	if parent := FromContext(ctx); parent != nil {
		sessionID = parent.SessionID
		rc.SessionID = parent.SessionID
		rc.Crumbs = parent.Crumbs
	}
	workCtx := ContextWith(ctx, rc)

	record := func() {
		c.Guard(func() {
			payload := txn.Finish("txn_"+core.NewEventID()[4:], c.Kind, c.Platform, sessionID, 1)
			if payload != nil {
				payload.Release = c.Release
				payload.CommitSHA = c.CommitSHA
				c.perf.txns.Push(*payload)
			}
		})
	}
	defer func() {
		if r := recover(); r != nil {
			// Capture with the job's own trace/timeline, seal the transaction,
			// re-panic. Finish() is idempotent, so an outer boundary that also
			// touches the transaction cannot double-record it.
			c.capturePanic(workCtx, r)
			txn.SetStatus("error")
			record()
			panic(r)
		}
	}()

	err := fn(workCtx)
	if err != nil {
		txn.SetStatus("error")
	}
	record()
	return err
}

// CheckIn reports one cron-monitor check-in. For two-step check-ins, send
// in_progress first, keep the returned id, and send ok/error with the same
// id when the job ends (WithMonitor does this automatically).
func (c *Client) CheckIn(payload CheckInPayload) string {
	id := payload.CheckInID
	c.Guard(func() {
		if id == "" {
			id = "ci_" + core.NewEventID()[4:]
		}
		payload.CheckInID = id
		payload.Release = c.Release
		payload.CommitSHA = c.CommitSHA
		c.perf.checkins.Send(payload)
	})
	return id
}

// WithMonitor wraps a scheduled job with in_progress/ok/error check-ins. ctx
// is first like every other trace-aware function, and flows into fn so the
// job's own calls (WithSpan, CaptureException, outbound HTTP) stay stitched
// to whatever context the caller established:
//
//	err := client.WithMonitor(ctx, "nightly-report", &reelay.MonitorConfig{
//	    Schedule: reelay.MonitorSchedule{Type: "crontab", Crontab: "0 2 * * *"},
//	}, func(ctx context.Context) error { … })
func (c *Client) WithMonitor(ctx context.Context, slug string, config *MonitorConfig, fn func(ctx context.Context) error) (err error) {
	started := time.Now()
	id := c.CheckIn(CheckInPayload{MonitorSlug: slug, Status: "in_progress", MonitorConfig: config})
	// The deferred terminal check-in also fires when fn panics (reported as
	// error), then the panic propagates untouched — crash semantics intact.
	completed := false
	defer func() {
		status := "ok"
		if err != nil || !completed {
			status = "error"
		}
		c.CheckIn(CheckInPayload{
			MonitorSlug: slug,
			Status:      status,
			CheckInID:   id,
			DurationMS:  float64(time.Since(started)) / float64(time.Millisecond),
		})
	}()
	err = fn(ctx)
	completed = true
	return err
}

// StopProfiling halts the continuous profiler (it starts automatically when
// Options.Profiling is set).
func (c *Client) StopProfiling() {
	if c.perf.profiler != nil {
		c.perf.profiler.stop()
	}
}

// Flush drains errors, transactions, check-ins, and profiles. It shadows the
// embedded BaseClient.Flush so package-level reelay.Flush() drains everything.
func (c *Client) Flush() {
	c.BaseClient.Flush()
	c.perf.txns.Flush()
	c.perf.checkins.Flush()
	c.perf.profiles.Flush()
}

// Close is the graceful-shutdown hook (the Sentry Go draining contract): stop
// the profiler, flush every transport bounded by ctx, then stop the drain
// goroutines — even ones parked in a backoff sleep — so the SDK never leaks a
// goroutine or holds the host past shutdown. Idempotent. Returns true when
// everything drained before ctx expired.
func (c *Client) Close(ctx context.Context) bool {
	c.StopProfiling()
	drained := c.perf.txns.Close(ctx)
	for _, transport := range []*core.Transport{
		c.Transport, c.perf.checkins, c.perf.profiles,
	} {
		if !transport.Close(ctx) {
			drained = false
		}
	}
	return drained
}

// CheckIn reports a check-in on the package singleton. No-op before Init.
func CheckIn(payload CheckInPayload) string {
	if c := GetClient(); c != nil {
		return c.CheckIn(payload)
	}
	return ""
}

// WithMonitor wraps fn with check-ins on the package singleton. Before Init
// it just runs fn.
func WithMonitor(ctx context.Context, slug string, config *MonitorConfig, fn func(ctx context.Context) error) error {
	if c := GetClient(); c != nil {
		return c.WithMonitor(ctx, slug, config, fn)
	}
	return fn(ctx)
}

// WithTransaction times fn on the package singleton. Before Init it just
// runs fn.
func WithTransaction(ctx context.Context, name, op string, fn func(ctx context.Context) error) error {
	if c := GetClient(); c != nil {
		return c.WithTransaction(ctx, name, op, fn)
	}
	return fn(ctx)
}
