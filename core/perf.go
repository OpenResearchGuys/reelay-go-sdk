package core

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Performance-monitoring wire types + client-side helpers, mirroring
// reelay-backend's APM ingest contract exactly:
//
//	POST /api/ingest/transactions — sampled transactions with spans
//	POST /api/ingest/checkins     — cron-monitor check-ins
//	POST /api/ingest/profiles     — continuous-profiling chunks

// SpanPayload is one timed unit of work inside a transaction.
type SpanPayload struct {
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	Op           string `json:"op"`
	Description  string `json:"description,omitempty"`
	StartedAt    string `json:"started_at"` // RFC3339
	EndedAt      string `json:"ended_at"`
	Status       string `json:"status,omitempty"`
	// Data holds OpenTelemetry-style span attributes (db.system.name/db.query.summary,
	// http.request.method/url.full/http.response.status_code, …). Strings keep
	// the wire shape bounded and safe to display.
	Data map[string]string `json:"data,omitempty"`
}

// boundSpanData caps the attribute map (mirrors the backend limits).
func boundSpanData(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if len(out) >= 24 {
			break
		}
		if k == "" {
			continue
		}
		out[truncate(k, 64)] = truncate(v, 512)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// TransactionPayload is one sampled unit of work.
type TransactionPayload struct {
	EventID        string             `json:"event_id"`
	Release        string             `json:"release,omitempty"`
	CommitSHA      string             `json:"commit_sha,omitempty"`
	Name           string             `json:"name"`
	Op             string             `json:"op"`
	Kind           string             `json:"kind"`
	Platform       string             `json:"platform"`
	TraceID        string             `json:"trace_id,omitempty"`
	SpanID         string             `json:"span_id,omitempty"`
	ParentSpanID   string             `json:"parent_span_id,omitempty"`
	SessionID      string             `json:"session_id,omitempty"`
	Status         string             `json:"status"`
	HTTPStatus     int                `json:"http_status,omitempty"`
	RequestHeaders map[string]string  `json:"request_headers,omitempty"`
	SampleRate     float64            `json:"sample_rate"`
	StartedAt      string             `json:"started_at"`
	EndedAt        string             `json:"ended_at"`
	Spans          []SpanPayload      `json:"spans,omitempty"`
	Measurements   map[string]float64 `json:"measurements,omitempty"`
	DroppedSpans   int                `json:"dropped_spans_count,omitempty"`
}

// MonitorSchedule mirrors the backend's schedule shape.
type MonitorSchedule struct {
	Type    string `json:"type"` // "crontab" | "interval"
	Crontab string `json:"crontab,omitempty"`
	Every   int    `json:"every,omitempty"`
	Unit    string `json:"unit,omitempty"` // minute | hour | day
}

// MonitorConfig lets a check-in create/update its monitor in-band.
type MonitorConfig struct {
	Schedule          MonitorSchedule `json:"schedule"`
	CheckinMarginMin  int             `json:"checkin_margin_min,omitempty"`
	MaxRuntimeMin     int             `json:"max_runtime_min,omitempty"`
	Timezone          string          `json:"timezone,omitempty"`
	FailureThreshold  int             `json:"failure_threshold,omitempty"`
	RecoveryThreshold int             `json:"recovery_threshold,omitempty"`
}

// CheckInPayload is one cron-monitor check-in.
type CheckInPayload struct {
	Release       string         `json:"release,omitempty"`
	CommitSHA     string         `json:"commit_sha,omitempty"`
	MonitorSlug   string         `json:"monitor_slug"`
	Status        string         `json:"status"` // in_progress | ok | error
	CheckInID     string         `json:"check_in_id,omitempty"`
	DurationMS    float64        `json:"duration_ms,omitempty"`
	MonitorConfig *MonitorConfig `json:"monitor_config,omitempty"`
}

// ProfileFramePayload is one stack frame (root-first within a stack).
type ProfileFramePayload struct {
	Function string `json:"function"`
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	InApp    bool   `json:"in_app"`
}

// ProfileStackPayload is one unique call stack and its sample count.
type ProfileStackPayload struct {
	Frames []ProfileFramePayload `json:"frames"`
	Count  int64                 `json:"count"`
}

// ProfileChunkPayload is one continuous-profiling window.
type ProfileChunkPayload struct {
	ChunkID        string                `json:"chunk_id"`
	Release        string                `json:"release,omitempty"`
	CommitSHA      string                `json:"commit_sha,omitempty"`
	ProfileType    string                `json:"profile_type"` // cpu | wall
	Platform       string                `json:"platform"`
	SamplePeriodMS float64               `json:"sample_period_ms"`
	SampleCount    int64                 `json:"sample_count"`
	StartedAt      string                `json:"started_at"`
	EndedAt        string                `json:"ended_at"`
	Stacks         []ProfileStackPayload `json:"stacks"`
}

// --- Route parameterization ------------------------------------------------------

var (
	reNumericSeg = regexp.MustCompile(`^\d+$`)
	reUUIDSeg    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reHexSeg     = regexp.MustCompile(`^[0-9a-fA-F]{8,}$`)
	rePrefixedID = regexp.MustCompile(`^[A-Za-z]+_[0-9A-Za-z]{6,}$`)
)

// ParameterizePath collapses id-like path segments to {id} so raw URLs group
// into bounded transaction names. Mirrors the server's normalization (the
// server re-applies it as defense in depth).
func ParameterizePath(path string) string {
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	segs := strings.Split(path, "/")
	for i, seg := range segs {
		if seg == "" {
			continue
		}
		if reNumericSeg.MatchString(seg) || reUUIDSeg.MatchString(seg) ||
			reHexSeg.MatchString(seg) || rePrefixedID.MatchString(seg) {
			segs[i] = "{id}"
		}
	}
	return strings.Join(segs, "/")
}

// --- Active transaction -----------------------------------------------------------

const maxSpansPerTxn = 50

// ActiveTransaction accumulates one in-flight unit of work and its child
// spans, then serializes to the wire shape. Safe for concurrent use (a
// request may fan out across goroutines); every method is a no-op after
// Finish.
//
// There is deliberately no open-span handle: spans only ever enter the
// transaction in finished form, via AddSpan. Timed user spans go through
// reelay.WithSpan, which measures fn itself — a span the caller could forget
// to end cannot be represented at all.
type ActiveTransaction struct {
	mu             sync.Mutex
	name           string
	op             string
	traceID        string
	spanID         string
	parentSpanID   string
	status         string
	httpStatus     int
	requestHeaders map[string]string
	measurements   map[string]float64
	startedAt      time.Time
	spans          []SpanPayload
	droppedSpans   int
	finished       bool
}

// NewActiveTransaction opens a transaction. parentSpanID may be empty.
func NewActiveTransaction(name, op, traceID, spanID, parentSpanID string) *ActiveTransaction {
	return &ActiveTransaction{
		name:         name,
		op:           op,
		traceID:      traceID,
		spanID:       spanID,
		parentSpanID: parentSpanID,
		status:       "ok",
		startedAt:    time.Now().UTC(),
	}
}

// SetName overrides the transaction name (e.g. with a router pattern).
func (t *ActiveTransaction) SetName(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished && name != "" {
		t.name = name
	}
}

// SetStatus sets "ok" or "error".
func (t *ActiveTransaction) SetStatus(status string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished && (status == "ok" || status == "error") {
		t.status = status
	}
}

// SetHTTPStatus records the response status code (5xx implies error).
func (t *ActiveTransaction) SetHTTPStatus(code int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	t.httpStatus = code
	if code >= 500 {
		t.status = "error"
	}
}

// SetRequestHeaders attaches headers already restricted by middleware's safe
// allow-list. The backend applies the same allow-list again at ingest.
func (t *ActiveTransaction) SetRequestHeaders(headers map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished || len(headers) == 0 {
		return
	}
	t.requestHeaders = make(map[string]string, len(headers))
	for name, value := range headers {
		t.requestHeaders[truncate(name, 64)] = truncate(value, 512)
	}
}

// SetMeasurement attaches a numeric measurement (browser web vitals).
func (t *ActiveTransaction) SetMeasurement(name string, value float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	if t.measurements == nil {
		t.measurements = map[string]float64{}
	}
	t.measurements[name] = value
}

// AddSpan records a span with explicit timing + attributes — for
// auto-instrumentation (an outbound HTTP call, a DB query) that measures its
// own duration and knows exactly what it did. spanID may be empty (a random one
// is assigned); pass a specific id to link a downstream service's transaction.
func (t *ActiveTransaction) AddSpan(op, description string, startedAt, endedAt time.Time, status string, data map[string]string, spanID string) {
	t.AddChildSpan(op, description, startedAt, endedAt, status, data, spanID, "")
}

// AddChildSpan is AddSpan with an explicit in-process parent. An empty parent
// attaches to the transaction root.
func (t *ActiveTransaction) AddChildSpan(op, description string, startedAt, endedAt time.Time, status string, data map[string]string, spanID, parentSpanID string) {
	if endedAt.Before(startedAt) {
		return
	}
	if status != "ok" && status != "error" {
		status = ""
	}
	if spanID == "" {
		spanID = NewSpanID()
	}
	if parentSpanID == "" {
		parentSpanID = t.spanID
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished || len(t.spans) >= maxSpansPerTxn {
		if !t.finished && len(t.spans) >= maxSpansPerTxn {
			t.droppedSpans++
		}
		return
	}
	t.spans = append(t.spans, SpanPayload{
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		Op:           truncate(op, 64),
		Description:  truncate(description, 512),
		StartedAt:    startedAt.UTC().Format(time.RFC3339Nano),
		EndedAt:      endedAt.UTC().Format(time.RFC3339Nano),
		Status:       status,
		Data:         boundSpanData(data),
	})
}

// Finish freezes the transaction and returns its wire payload (nil when
// already finished).
func (t *ActiveTransaction) Finish(eventID, kind, platform, sessionID string, sampleRate float64) *TransactionPayload {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return nil
	}
	t.finished = true
	return &TransactionPayload{
		EventID:        eventID,
		Name:           t.name,
		Op:             t.op,
		Kind:           kind,
		Platform:       platform,
		TraceID:        t.traceID,
		SpanID:         t.spanID,
		ParentSpanID:   t.parentSpanID,
		SessionID:      sessionID,
		Status:         t.status,
		HTTPStatus:     t.httpStatus,
		RequestHeaders: t.requestHeaders,
		SampleRate:     sampleRate,
		StartedAt:      t.startedAt.Format(time.RFC3339Nano),
		EndedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		Spans:          t.spans,
		Measurements:   t.measurements,
		DroppedSpans:   t.droppedSpans,
	}
}

// --- Transaction buffer ------------------------------------------------------------

// TxnBuffer batches finished transactions and flushes them as one POST every
// few seconds (or when the batch fills), through the same resilient
// Transport the error path uses.
type TxnBuffer struct {
	transport *Transport
	maxBatch  int
	interval  time.Duration

	mu     sync.Mutex
	buffer []TransactionPayload
	timer  *time.Timer
}

// NewTxnBuffer wires a buffer to its transport. Zero values default to a
// 20-item batch flushed every second. This keeps distributed traces from
// appearing service-by-service for several seconds while still coalescing a
// burst of completed requests into one ingest call.
func NewTxnBuffer(transport *Transport, maxBatch int, interval time.Duration) *TxnBuffer {
	if maxBatch <= 0 {
		maxBatch = 20
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &TxnBuffer{transport: transport, maxBatch: maxBatch, interval: interval}
}

// Push queues one transaction; never blocks.
func (b *TxnBuffer) Push(txn TransactionPayload) {
	b.mu.Lock()
	b.buffer = append(b.buffer, txn)
	if len(b.buffer) >= b.maxBatch {
		b.flushLocked()
		b.mu.Unlock()
		return
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(b.interval, func() {
			b.mu.Lock()
			b.flushLocked()
			b.mu.Unlock()
		})
	}
	b.mu.Unlock()
}

// Flush sends whatever is buffered and drains the transport (blocking).
func (b *TxnBuffer) Flush() {
	b.mu.Lock()
	b.flushLocked()
	b.mu.Unlock()
	b.transport.Flush()
}

// Close hands the remaining batch to the transport, then closes it (bounded
// by ctx) — the buffer's graceful-shutdown hook. Returns true when the
// transport drained fully.
func (b *TxnBuffer) Close(ctx context.Context) bool {
	b.mu.Lock()
	b.flushLocked()
	b.mu.Unlock()
	return b.transport.Close(ctx)
}

func (b *TxnBuffer) flushLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.buffer) == 0 {
		return
	}
	batch := b.buffer
	b.buffer = nil
	b.transport.Send(map[string]any{"transactions": batch})
}
