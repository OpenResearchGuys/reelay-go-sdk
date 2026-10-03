// Command crashprobe is a tiny instrumented program used by the crash-semantics
// test (process_test.go). It initializes the SDK, panics, and relies on
// reelay.Recover to deliver the crash and then re-panic so the process still
// dies — exactly like an uninstrumented Go program.
package main

import (
	"context"
	"os"

	reelay "github.com/OpenResearchGuys/reelay-go-sdk"
)

func main() {
	reelay.Init(reelay.Options{
		Endpoint:                    os.Getenv("REELAY_ENDPOINT"),
		Token:                       "test-token",
		AllowInsecureHTTPForTesting: true,
	})
	defer reelay.Recover(context.Background())
	panic("boom-crash")
}
