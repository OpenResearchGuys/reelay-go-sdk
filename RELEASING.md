# Releasing the Go SDK

The `Publish Go SDK` GitHub Actions workflow runs on pushes to `main` on the AWS CodeBuild runner `reelay-go-sdk-github-runner` in `eu-west-1`. It tests the module and pushes the tag in `VERSION` when that tag does not exist. The public Go module proxy then indexes `github.com/OpenResearchGuys/reelay-go-sdk` at that tag.

The GitHub repository must be public and its path must match `go.mod`. The workflow needs repository Actions permission to write contents; it uses the built-in `GITHUB_TOKEN` and needs no separate secret. Bump `VERSION` to a new semantic version, commit, and push `main` for each release. Tags are immutable: never move or reuse a published tag.
