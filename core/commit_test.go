package core

import "testing"

func TestResolveCommitSHA(t *testing.T) {
	if got := ResolveCommitSHA("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); got != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("ResolveCommitSHA() = %q", got)
	}
	if got := ResolveCommitSHA("abc123"); got != "" {
		t.Fatalf("short SHA accepted: %q", got)
	}
}
