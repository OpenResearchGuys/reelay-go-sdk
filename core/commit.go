package core

import (
	"os"
	"strings"
)

// ResolveCommitSHA accepts only full Git object IDs. An explicit value wins;
// otherwise the deployment-provided REELAY_COMMIT_SHA is used.
func ResolveCommitSHA(value string) string {
	sha := strings.ToLower(strings.TrimSpace(value))
	if sha == "" {
		sha = strings.ToLower(strings.TrimSpace(os.Getenv("REELAY_COMMIT_SHA")))
	}
	if len(sha) != 40 && len(sha) != 64 {
		return ""
	}
	for _, c := range sha {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return ""
		}
	}
	return sha
}
