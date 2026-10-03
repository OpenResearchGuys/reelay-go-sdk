package core

import "testing"

func TestResolveRelease(t *testing.T) {
	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := ResolveRelease("api@1.4.0", sha); got != "api@1.4.0" {
		t.Fatalf("ResolveRelease explicit = %q", got)
	}
	if got := ResolveRelease("", sha); got != sha {
		t.Fatalf("ResolveRelease fallback = %q", got)
	}
	if got := ResolveRelease("../bad", sha); got != "" {
		t.Fatalf("ResolveRelease invalid = %q", got)
	}
}
