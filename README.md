# reelay-go-sdk

The Reelay SDK for Go backends. It reports errors without blocking your
application and can attach the HTTP request, trace, logs, spans, and recent
breadcrumbs that explain what happened.

It is a port of [`@usereelay/node`](https://github.com/OpenResearchGuys/replay-node-sdk):
same wire contract, same delivery/privacy behavior, idiomatic Go API.

Requires Go 1.21 or newer.

## Install

```bash
go get github.com/OpenResearchGuys/reelay-go-sdk
```

```go
import reelay "github.com/OpenResearchGuys/reelay-go-sdk"
```

## Quick start (net/http)

```go
package main

import (
	"log"
	"net/http"
	"os"

	reelay "github.com/OpenResearchGuys/reelay-go-sdk"
)

func main() {
	// Initialize once, before the server starts handling requests.
	reelay.Init(reelay.Options{
		// Customer-owned HTTPS endpoint. The SDK adds /api/ingest/errors.
		Endpoint: "https://ingest.customer.example",
		NodeID: os.Getenv("REELAY_NODE_ID"),
		IngestKey: os.Getenv("REELAY_INGEST_KEY"),
	})
	defer reelay.Flush() // drain the queue on shutdown

	mux := http.NewServeMux()
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		reelay.AppendLog(r.Context(), "info", "Starting checkout")
		panic("Payment provider is unavailable")
	})

	// Wrap your handler. This creates a separate context for each request and
	// recovers panics.
	log.Fatal(http.ListenAndServe(":3000", reelay.Middleware()(mux)))
}
```

Hit `/checkout`. The error appears in Reelay with `GET /checkout`, a 500 status,
and the “Starting checkout” log.

## Configuration reference

`Options` contains these settings (matching the Node SDK): artifact identity,
error capture, performance monitoring, and profiling.

| Setting | Type | Default | What it does |
|---|---|---|---|
| `Endpoint` | `string` | none | **Required.** Customer-owned HTTPS ingest base URL, e.g. `https://ingest.customer.example`. Do not add an ingest path. |
| `NodeID` | `string` | none | **Required.** Target self-hosted node ID. |
| `IngestKey` | `string` | none | **Required.** Key validated by Cloud for every event. |
| `Token` | `string` | none | **Required.** Node API key beginning with `rlyt_live_`. It authenticates events. Never use the Reelay node ID here. |
| `Release` | `string` | `REELAY_RELEASE`, then commit SHA | Artifact version stamped on all telemetry, e.g. `api@1.4.0`. |
| `CommitSHA` | `string` | `REELAY_COMMIT_SHA` | Optional explicit full Git SHA. The deployment environment is recommended; short/invalid SHAs are ignored. |
| `SampleRate` | `*float64` (0–1) | `1` | Random sampling per **error**. `nil`/`1` sends every error, `0.25` sends roughly 25%, `0` sends none. Leave it unset unless event volume is intentionally being reduced. |
| `BeforeSend` | `func(*ErrorEventPayload) *ErrorEventPayload` | not set | Final filter after built-in scrubbing. Return the event to send it, edit and return it to change it, or return `nil` to discard it. |
| `ScrubPatterns` | `[]*regexp.Regexp` | `nil` | Extra secret formats for your application. Matching text becomes `[redacted]`. Built-ins already cover common token/password assignments, JWTs, card-shaped numbers, and email addresses. |
| `MaxQueueSize` | `int` | `30` | Deprecated compatibility option; bursts remain queued until delivery. This is not permanent offline storage. |
| `Debug` | `func(message string, detail any)` | silent | Receives SDK delivery/setup diagnostics. Reelay does not log these by default. Add the hook temporarily while troubleshooting. |
| `TracesSampleRate` | `*float64` (0–1) | `0` | Fraction of HTTP requests recorded as **performance transactions**. `nil`/`0` disables performance monitoring. Head-based per request; the rate travels with each transaction so the server extrapolates accurate metrics. Start at `0.1`–`0.2` in production. |
| `Profiling` | `bool` | `false` | Enables continuous wall-clock profiling (samples goroutine stacks, uploads pre-folded stacks). Requires `TracesSampleRate > 0`. Steady-state overhead well under ~2% at defaults. |
| `ProfileWindow` | `time.Duration` | `10s` | Profiling window captured per cycle. |
| `ProfileCycle` | `time.Duration` | `60s` | Profiling cycle length. |

Complete example:

```go
rate := 1.0
reelay.Init(reelay.Options{
	Endpoint:   "https://ingest.customer.example",
	NodeID:     os.Getenv("REELAY_NODE_ID"),
	IngestKey:  os.Getenv("REELAY_INGEST_KEY"),
	Release:    os.Getenv("REELAY_RELEASE"),
	CommitSHA:  os.Getenv("REELAY_COMMIT_SHA"),
	SampleRate: &rate, // use 0.1 to send roughly one in ten
	MaxQueueSize: 30,
	ScrubPatterns: []*regexp.Regexp{
		regexp.MustCompile(`(?i)merchant-secret-[a-z0-9]+`),
	},
	BeforeSend: func(e *reelay.ErrorEventPayload) *reelay.ErrorEventPayload {
		if strings.Contains(e.Exception.Value, "expected health-check failure") {
			return nil // drop one known, harmless error
		}
		return e
	},
	Debug: func(msg string, detail any) {
		log.Printf("[reelay] %s %v", msg, detail)
	},
})
```

Set `REELAY_RELEASE` to the artifact version and `REELAY_COMMIT_SHA` to the
full commit. The SDK resolves both once and stamps every error, transaction,
profile, and check-in; when release is absent it falls back to the commit SHA.
For a direct deployment, prefix the binary command with both variables. Crash
capture is wired through `Recover` / `Middleware` (see below).

A release may be supplied without a commit; telemetry and release history still
work. Exact code analysis requires a repository-linked node and a full commit
from `CommitSHA`, an existing release association, or a release that is itself
a full SHA. Reelay never guesses repository HEAD.

Release names are trimmed, limited to 200 characters, and cannot be `.`, `..`,
or contain tabs, newlines, `/`, or `\`. Reusing one release with different
commits is reported as a conflict rather than silently remapped.

## Request context: `context.Context` instead of AsyncLocalStorage

The Node SDK carries per-request state with `AsyncLocalStorage`. Go has no
ambient store, so the SDK uses the idiomatic mechanism: `context.Context`. The
middleware seeds a request context into `r.Context()`; you pass that context to
the SDK functions (as Go code already threads `ctx`).

### `Middleware()`

`reelay.Middleware()` returns `func(http.Handler) http.Handler`. It does the job
of the Node SDK's `requestHandler` **and** `errorHandler` in one wrapper,
because Go's `net/http` already isolates a panicking handler to its request. For
every request it:

1. reads incoming W3C `Traceparent` and `Reelay-Session-Id` headers;
2. seeds an isolated request context;
3. stores method, URL, eight safe headers, trace, logs, spans, and breadcrumbs;
4. records the final response status;
5. recovers a panicking handler, captures it, and returns `500`.

The eight captured headers are `User-Agent`, `Content-Type`, `Accept`,
`Referer`, `Origin`, `Host`, `X-Request-ID`, and `X-Correlation-ID`. They are
also retained on sampled performance transactions for request search.
Authentication, cookies, tokens, and forwarding/IP headers are never selected.

You can also bind it to a specific client: `client.Middleware(next)`.

## Manual error capture

Use `CaptureException` for an error your code catches and handles:

```go
if err := runNightlyReport(ctx); err != nil {
	id := reelay.CaptureException(ctx, err)
	log.Println("Reelay event:", id)
}
```

Inside an HTTP request, pass `r.Context()` and the active request context is
attached automatically. Outside a request (a cron job), pass
`context.Background()`: the error is still sent but has no HTTP context.

Use `CaptureMessage` when there is no error value but a condition should be
reported:

```go
reelay.CaptureMessage(ctx, "Inventory sync entered fallback mode")
```

Both return an event ID such as `evt_…`, or `""` when sampling or `BeforeSend`
drops the event (or before `Init`).

## Timeline logs and spans

Calls made during a request are attached to an error captured in that request:

```go
reelay.AppendLog(ctx, "info", "Calling payment provider")

reelay.AppendSpan(ctx, reelay.TimelineSpan{
	SpanID: core.NewSpanID(),
	Name:   "payments.charge",
	Status: "failed",
})
```

- `AppendLog(ctx, level, message)` accepts any level string. Messages are
  trimmed to 500 characters. Common values are `debug`, `info`, `warn`, `error`.
- `AppendSpan(ctx, span)` stores an ID, a human-readable name, and an optional
  status such as `ok` or `failed`.
- Each request keeps at most 100 logs and 100 spans.
- Both are no-ops when `ctx` carries no request context.

## Breadcrumbs

```go
client.AddBreadcrumb(ctx, reelay.Breadcrumb{
	Timestamp: time.Now().UnixMilli(),
	Type:      "database",
	Category:  "orders",
	Message:   "Order query completed",
})
```

Each request has its own buffer of the latest 50 breadcrumbs, so concurrent
users do not share context. Outside a request, breadcrumbs use a process-level
buffer. The package-level `reelay.AddBreadcrumb(ctx, …)` uses the active client.

## Browser-to-backend trace linking

The browser SDK can add `traceparent` and `reelay-session-id` to selected fetch
requests. `Middleware` adopts those headers, so a backend error shares the
browser session’s trace and replay link. No extra Go-side setting is required.

## Performance monitoring (APM)

Set `TracesSampleRate` above `0` to record a share of requests as **performance
transactions**: the data behind the dashboard's Performance page (throughput,
p50/p95/p99 latency, error rate, Apdex). With `Middleware` installed, every
sampled request automatically becomes a transaction named after its route; you
do not open transactions by hand for HTTP traffic.

Root decisions are deterministic from the trace ID and remote decisions are
parent-based. Both sampled and unsampled flags propagate through wrapped HTTP
clients, preventing each service from independently re-sampling the same
distributed request.

```go
rate := 0.2
reelay.Init(reelay.Options{
    Endpoint:         "https://ingest.customer.example",
    NodeID:           os.Getenv("REELAY_NODE_ID"),
    IngestKey:        os.Getenv("REELAY_INGEST_KEY"),
    TracesSampleRate: &rate, // record 20% of requests
})
```

### `WithSpan(ctx, op, description, fn)`

Times `fn` as a **child span** of the request's transaction. The span is
recorded with `ok` status when `fn` returns `nil` and `error` when it returns a
non-nil error (the error is passed through, never swallowed):

```go
err := reelay.WithSpan(ctx, "db.query", "SELECT * FROM users", func(ctx context.Context) error {
    return db.QueryRowContext(ctx, "SELECT * FROM users").Scan(&u)
})
```

Pass the `ctx` through to child calls so nested spans attach correctly. The
lower-level `StartSpan(ctx, category, name) func(status string)` returns a
finish func if you need manual control instead of a callback; it records the
same timed APM span and is safe to finish more than once.

### `WithTransaction(ctx, name, op, fn)` and `SetTransactionName(ctx, name)`

For non-HTTP work (a cron job, a worker, a CLI), wrap it in its own
transaction. Use `SetTransactionName` to override the active transaction's name
(e.g. collapse a path into its router pattern):

```go
_ = reelay.WithTransaction(ctx, "nightly-rollup", "task", func(ctx context.Context) error {
    return runRollup(ctx)
})
```

### Profiling

Set `Profiling: true` (with `TracesSampleRate > 0`) to capture continuous
wall-clock profiles. The SDK samples goroutine stacks in short windows and
uploads pre-folded stacks that power the flamegraph and "busiest functions"
view. `client.StopProfiling()` halts it; it starts automatically on `Init`.

## Cron & heartbeat monitors

Monitors are a **dead-man's switch** for scheduled jobs: your job checks in each
run, and Reelay alerts when an expected check-in doesn't arrive. The SDK never
runs or schedules your job; it only reports that the job ran.

### `WithMonitor(ctx, slug, config, fn)`

The one-line wrapper. It sends an `in_progress` check-in when `fn` starts and a
terminal `ok`/`error` when it ends (reporting `error` automatically if `fn`
returns an error), catching both jobs that stop running **and** jobs that hang.

```go
_ = reelay.WithMonitor(ctx, "nightly-backup", nil, func(ctx context.Context) error {
    return runBackup(ctx)
})
```

Pass `nil` for `config` when the monitor is managed on the dashboard
(recommended). To manage the schedule from code ("monitors as code"), pass a
`*MonitorConfig` and the first check-in creates/updates the monitor:

```go
_ = reelay.WithMonitor(ctx, "nightly-backup", &reelay.MonitorConfig{
    Schedule:         reelay.MonitorSchedule{Type: "crontab", Crontab: "0 2 * * *"},
    Timezone:         "Europe/Lisbon",
    CheckinMarginMin: 5,  // grace before a run counts as missed
    MaxRuntimeMin:    30, // mark timed-out if it runs longer
    FailureThreshold: 1,
    RecoveryThreshold: 1,
}, runBackup)
```

`CheckIn(payload CheckInPayload) string` is the low-level primitive if you want
to send heartbeats yourself instead of using the wrapper.

## Panics and crash semantics

Go has no global `uncaughtException` hook, and the runtime already isolates a
panic to its goroutine. The SDK provides three explicit, idiomatic mechanisms
in place of the Node SDK's always-on process listeners:

| Helper | Use it for | Behavior |
|---|---|---|
| `Middleware()` | HTTP handlers | Recovers a panicking handler, captures it (mechanism `middleware`), returns 500. The server stays up. |
| `Recover(ctx)` | top of `main` or a goroutine whose crash should stay fatal | `defer reelay.Recover(ctx)`: captures (mechanism `uncaughtException`), flushes (bounded by a 2s timeout), then **re-panics** so the process still dies like an uninstrumented one. |
| `RecoverAndContinue(ctx)` | worker goroutines that should survive | `defer reelay.RecoverAndContinue(ctx)`: captures and swallows. |

```go
func worker(ctx context.Context, jobs <-chan Job) {
	defer reelay.RecoverAndContinue(ctx)
	for j := range jobs {
		process(j)
	}
}
```

Because Go cannot intercept process exit, call `defer reelay.Flush()` in `main`
to drain the queue on a clean shutdown.

## Delivery and privacy behavior

- Sending is non-blocking and queues every event in a burst for a background
  goroutine to drain. Pending events consume memory while delivery falls behind.
- Network errors, 408, 425, 429, and server errors retry while the transport
  remains open, with jittered backoff. A single attempt times out after ten seconds. `429`
  honors `Retry-After`.
- Other 4xx responses are dropped because retrying invalid authentication or a
  bad payload cannot fix it.
- Error messages are scrubbed and limited to 2,000 characters.
- Breadcrumb messages are scrubbed and limited to 500 characters.
- HTTP/timeline objects are scrubbed. Sensitive keys such as `authorization`,
  `cookie`, `password`, `token`, and `cvv` are replaced with `[redacted]`.
- Stack file paths and line numbers are not scrubbed.

Stack traces are captured from the Go runtime (`runtime.Callers` for manual
captures, the parsed goroutine stack for panics), since Go errors do not carry
one. Runtime, standard-library, and module-cache frames are marked
`in_app=false` so fingerprinting groups on application code.

## API summary

| Export | Purpose |
|---|---|
| `Init(opts)` | Creates and installs the singleton `*Client`. Later calls return the original client and ignore new options. |
| `GetClient()` | Returns the active client or `nil`. |
| `CaptureException(ctx, err)` | Reports a caught error with active request context. |
| `CaptureMessage(ctx, msg)` | Reports a string as a synthetic `Message` error. |
| `Middleware()` | Returns `net/http` middleware bound to the active client. |
| `AppendLog(ctx, level, msg)` | Adds a request timeline log. |
| `AppendSpan(ctx, span)` | Adds a request timeline span. |
| `AddBreadcrumb(ctx, crumb)` | Records a breadcrumb. |
| `WithSpan(ctx, op, desc, fn)` | Times `fn` as a child span of the active transaction (`ok`/`error` by returned error). |
| `WithTransaction(ctx, name, op, fn)` | Times `fn` as its own transaction (cron job, worker, CLI). |
| `SetTransactionName(ctx, name)` | Overrides the active transaction's name. |
| `WithMonitor(ctx, slug, cfg, fn)` | Wraps a scheduled job with two-step cron check-ins. |
| `CheckIn(payload)` | Sends one cron-monitor check-in. |
| `Recover(ctx)` / `RecoverAndContinue(ctx)` | Panic capture (fatal / survivable). |
| `Flush()` / `Close(ctx)` | Drains the in-memory delivery queue (`Close` respects its context deadline). |
| `FromContext(ctx)` / `ContextWith(ctx, rc)` | Advanced: read/seed the request context manually. |
| `NewClient(opts)` | Builds a non-singleton client. |

## Development

```bash
go build ./...
go vet ./...
go test -race ./...
```

The `core` package holds the runtime-agnostic logic (wire types, event
assembly, scrubbing, breadcrumb ring buffer, stack parsing, ids, transport),
mirroring the Node SDK's `src/core`. The root `reelay` package adds the
Go-specific instrumentation (request context, middleware, panic capture).

## Ingestion edge contract

SDK requests use `POST`, `Content-Type: application/json`, `X-Reelay-Token`,
and `X-Reelay-Node-ID`. The backend accepts only the current endpoint schema;
unknown fields and the old `X-Reelay-Ingest-Key` header are rejected. Rebuild
and repack the SDK after source changes so deployed applications use the same
contract as the source.

The production ALB-associated WAF applies one shared 1,000 requests/minute
budget across all customers and routes. Edge throttles return `429` with
`Retry-After: 60`; the transport backs off with a retry queue. This is
AWS's approximate rate protection, not an exact counter or a per-token budget.
