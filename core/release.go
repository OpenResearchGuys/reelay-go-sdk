package core

import (
	"os"
	"strings"
)

// ResolveRelease resolves an artifact version once at client construction.
// Explicit configuration wins, then REELAY_RELEASE, then the commit SHA.
func ResolveRelease(value, commitSHA string) string {
	release := strings.TrimSpace(value)
	if release == "" {
		release = strings.TrimSpace(os.Getenv("REELAY_RELEASE"))
	}
	if release == "" {
		release = commitSHA
	}
	if release == "" || len(release) > 200 || release == "." || release == ".." ||
		strings.ContainsAny(release, "\n\r\t/\\") {
		return ""
	}
	return release
}
