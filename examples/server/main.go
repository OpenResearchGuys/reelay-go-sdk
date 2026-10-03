// Command server is a minimal example of wiring the Reelay Go SDK into a
// net/http service. Run it with REELAY_NODE_ID and REELAY_INGEST_KEY set.
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
		Endpoint:  "https://ingest.customer.example",
		NodeID:    os.Getenv("REELAY_NODE_ID"),
		IngestKey: os.Getenv("REELAY_INGEST_KEY"),
	})
	// Drain the in-memory queue on shutdown.
	defer reelay.Flush()

	mux := http.NewServeMux()

	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		// Timeline logs/spans recorded during the request are attached to any
		// error captured in that request.
		reelay.AppendLog(r.Context(), "info", "Starting checkout")

		// A panic is captured by the middleware, reported, and turned into a
		// 500 — without taking the whole server down.
		panic("Payment provider is unavailable")
	})

	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		// Manual capture for an error you catch and handle yourself.
		if err := runNightlyReport(); err != nil {
			id := reelay.CaptureException(r.Context(), err)
			log.Printf("reelay event: %s", id)
		}
		w.WriteHeader(http.StatusOK)
	})

	// Middleware() creates per-request context, adopts browser trace headers,
	// records the response status, and recovers panics.
	handler := reelay.Middleware()(mux)

	log.Println("listening on :3000")
	log.Fatal(http.ListenAndServe(":3000", handler))
}

func runNightlyReport() error { return nil }
