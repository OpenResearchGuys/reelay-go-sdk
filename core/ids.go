package core

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Identifier generation without external dependencies. Uses crypto/rand and
// falls back to math/rand only if the system source is unavailable (matching
// the Node SDK's WebCrypto-with-fallback behavior).

func randomBytes(length int) []byte {
	b := make([]byte, length)
	if _, err := cryptorand.Read(b); err != nil {
		for i := range b {
			b[i] = byte(mathrand.Intn(256))
		}
	}
	return b
}

// NewTraceID returns a 32-hex-char W3C trace id.
func NewTraceID() string {
	return hex.EncodeToString(randomBytes(16))
}

// NewSpanID returns a 16-hex-char W3C span id.
func NewSpanID() string {
	return hex.EncodeToString(randomBytes(8))
}

// NewEventID returns an event id: evt_ + 26 hex chars (time-prefixed for rough
// ordering), identical in shape to the Node SDK.
func NewEventID() string {
	t := fmt.Sprintf("%012x", time.Now().UnixMilli())
	return "evt_" + t + hex.EncodeToString(randomBytes(7))
}

// NewSessionID returns a session id used to join replay chunks with traces.
func NewSessionID() string {
	return "sess_" + hex.EncodeToString(randomBytes(8))
}

// Traceparent builds a W3C traceparent header value.
func Traceparent(traceID, spanID string, sampled ...bool) string {
	flag := "01"
	if len(sampled) > 0 && !sampled[0] {
		flag = "00"
	}
	return "00-" + traceID + "-" + spanID + "-" + flag
}

var (
	traceIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	spanIDRe  = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// ParsedTraceparent is the trace/span pair extracted from a traceparent header.
type ParsedTraceparent struct {
	TraceID string
	SpanID  string
	Sampled bool
}

// ParseTraceparent parses a traceparent header value, returning nil when it is
// missing or malformed (including the all-zero invalid ids).
func ParseTraceparent(value string) *ParsedTraceparent {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[3]) != 2 {
		return nil
	}
	traceID, spanID := parts[1], parts[2]
	if !traceIDRe.MatchString(traceID) || !spanIDRe.MatchString(spanID) {
		return nil
	}
	if traceID == strings.Repeat("0", 32) || spanID == strings.Repeat("0", 16) {
		return nil
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil || len(flags) != 1 {
		return nil
	}
	return &ParsedTraceparent{TraceID: traceID, SpanID: spanID, Sampled: flags[0]&1 == 1}
}

// ShouldSampleTrace makes a deterministic trace-id-ratio decision from 52
// random bits. A distributed trace therefore gets one consistent root choice.
func ShouldSampleTrace(traceID string, rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	if !traceIDRe.MatchString(traceID) {
		return false
	}
	v, err := strconv.ParseUint(traceID[len(traceID)-13:], 16, 64)
	return err == nil && float64(v) < rate*float64(uint64(1)<<52)
}
