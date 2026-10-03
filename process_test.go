package reelay_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// TestCrashSemantics is the Go analogue of the Node SDK's process.test.ts
// end-to-end contract: a panic must be DELIVERED to ingest AND the process must
// still die (non-zero exit), exactly like an uninstrumented Go program. Here
// reelay.Recover provides that guarantee (capture + flush + re-panic).
func TestCrashSemantics(t *testing.T) {
	var mu sync.Mutex
	var received []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		received = append(received, body)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	cmd := exec.Command("go", "run", "./internal/crashprobe")
	cmd.Env = append(cmd.Environ(), "REELAY_ENDPOINT="+srv.URL)
	out, err := cmd.CombinedOutput()

	// The program must NOT exit cleanly: the panic re-propagates after capture.
	if err == nil {
		t.Fatalf("expected non-zero exit, got success; output:\n%s", out)
	}

	// Give the server a beat in case the body write raced the process exit.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	var event map[string]any
	for _, e := range received {
		if exc, ok := e["exception"].(map[string]any); ok && exc["value"] == "boom-crash" {
			event = e
		}
	}
	if event == nil {
		t.Fatalf("crash was not flushed before exit; received=%v output=%s", received, out)
	}
	exc := event["exception"].(map[string]any)
	if exc["mechanism"] != "uncaughtException" {
		t.Fatalf("unexpected mechanism: %v", exc["mechanism"])
	}
}
