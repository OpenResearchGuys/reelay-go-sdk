package reelay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// Options are exactly the shared core options.
type Options = core.Options

const fatalFlushTimeout = 2 * time.Second

// Client is the Go backend SDK client. It embeds core.BaseClient and adds
// request-context enrichment plus panic capture.
//
// Process-level crash capture differs from Node by necessity. Node installs
// global uncaughtException/unhandledRejection listeners; Go has no global
// panic hook, and the runtime already isolates a panic to its goroutine. The
// Go equivalents are:
//
//   - Recover(ctx): defer at the top of a goroutine or main; captures the panic
//     with crash semantics preserved (flush, then re-panic so the process still
//     dies exactly like an uninstrumented one).
//   - RecoverAndContinue(ctx): defer in a worker goroutine that should survive a
//     panic; captures and swallows.
//   - Middleware: recovers panics in HTTP handlers, captures them, and returns
//     500 — one bad request never takes down the server.
type Client struct {
	*core.BaseClient
	perf *perf
}

// NewClient builds a Client without installing it as the package singleton.
func NewClient(opts Options) *Client {
	opts.CommitSHA = core.ResolveCommitSHA(opts.CommitSHA)
	opts.Release = core.ResolveRelease(opts.Release, opts.CommitSHA)
	return &Client{
		BaseClient: core.NewBaseClient(opts, "backend", "go"),
		perf:       newPerf(opts),
	}
}

// AddBreadcrumb records a breadcrumb (scrubbed, bounded). When ctx carries a
// request it uses that request's private buffer; otherwise the process-global
// buffer (cron jobs, startup, queue consumers without a seeded context).
func (c *Client) AddBreadcrumb(ctx context.Context, crumb core.Breadcrumb) {
	c.Guard(func() {
		buf := c.Crumbs
		if rc := FromContext(ctx); rc != nil {
			buf = rc.Crumbs
		}
		if crumb.Message != "" {
			crumb.Message = truncateRunes(c.Scrubber.ScrubString(crumb.Message), 500)
		}
		buf.Add(crumb)
	})
}

// CaptureException reports a recovered/handled error, enriched with the active
// request context from ctx. Returns the event id, or "" when dropped.
func (c *Client) CaptureException(ctx context.Context, err any) string {
	return c.captureWithStack(ctx, err, "manual", core.CurrentStack(50))
}

// CaptureMessage reports a string as a synthetic Message error.
func (c *Client) CaptureMessage(ctx context.Context, message string) string {
	return c.captureWithStack(ctx, errors.New(message), "message", core.CurrentStack(50))
}

// captureWithStack is the shared enrichment path. The stack is passed in so the
// panic path can supply a parsed debug.Stack() instead of the live call stack.
func (c *Client) captureWithStack(ctx context.Context, err any, mechanism string, stack []core.StackFrame) string {
	cc := core.CaptureContext{Mechanism: mechanism, Stacktrace: stack}

	buf := c.Crumbs
	if rc := FromContext(ctx); rc != nil {
		trace := rc.Trace
		cc.Trace = &trace
		cc.SessionID = rc.SessionID
		cc.HTTP = rc.httpSnapshot()
		cc.Timeline = rc.timelineSnapshot()
		buf = rc.Crumbs
	}
	cc.Breadcrumbs = buf.Snapshot()
	return c.BaseClient.CaptureException(err, cc)
}

// Log appends a log line to the active request's timeline.
func (c *Client) Log(ctx context.Context, level, msg string) {
	c.Guard(func() { AppendLog(ctx, level, msg) })
}

// Recover captures a panic and re-panics to preserve crash semantics: the
// process still terminates exactly like an uninstrumented one, but the crash is
// flushed to Reelay first. Use it as `defer client.Recover(ctx)` at the top of
// main or a goroutine whose panic should remain fatal.
func (c *Client) Recover(ctx context.Context) {
	if r := recover(); r != nil {
		c.capturePanic(ctx, r)
		c.Transport.FlushTimeout(fatalFlushTimeout)
		fmt.Fprintln(os.Stderr, r)
		panic(r)
	}
}

// RecoverAndContinue captures a panic and swallows it, so a worker goroutine
// survives. Use it as `defer client.RecoverAndContinue(ctx)`.
func (c *Client) RecoverAndContinue(ctx context.Context) {
	if r := recover(); r != nil {
		c.capturePanic(ctx, r)
	}
}

// capturePanic captures a recovered value with the goroutine's stack (parsed
// from debug.Stack(), since the panicking frames have already unwound).
func (c *Client) capturePanic(ctx context.Context, r any) string {
	return c.captureWithStack(ctx, r, "uncaughtException", core.ParseGoStack(debug.Stack(), 50))
}
