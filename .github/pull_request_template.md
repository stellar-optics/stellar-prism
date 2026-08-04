<!--
Thanks for contributing.

Keep the title in Conventional Commits form, e.g.
  feat(lens): add ClaimableBalanceEntry to the type registry
  fix(format): stop emitting escape codes when stdout is a pipe
-->

## Linked issue

<!-- "Closes #123" / "Fixes #123". If there is no issue, say why one was not
     needed — a typo fix does not need one; a new command does. -->

Closes #

## What changed and why

<!-- The diff already says what. Explain why: what problem does this solve,
     and why this approach over the alternatives? -->

## Test evidence

<!-- Show that it works. Paste the actual command output — not a claim that
     you ran it. For a behaviour change, show before and after. -->

```console
$ go test -race ./...

```

```console
$ # the command your change affects, with its real output

```

## Checklist

- [ ] `go build ./...` passes
- [ ] `go test -race ./...` passes
- [ ] `golangci-lint run ./...` reports no issues
- [ ] `gofmt -l .` prints nothing
- [ ] New behaviour is covered by tests
- [ ] Public API changes carry doc comments
- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] No new dependency added (or the PR explains why one was necessary)
- [ ] **No network calls added to any test** — the suite must run offline
- [ ] If `pkg/stream/tail.go` changed: the mutation exercise in
      docs/architecture.md was re-run and the suite still fails when
      de-duplication or gap detection is broken

## Type of change

- [ ] Bug fix (no breaking change)
- [ ] New feature (no breaking change)
- [ ] Breaking change to the public `pkg/stream` or `pkg/sink` API
- [ ] Documentation only
- [ ] Tests or tooling only

## Notes for the reviewer

<!-- Anything that would help review: a tricky decision, a deliberate
     trade-off, or a part you would like a second opinion on. -->
