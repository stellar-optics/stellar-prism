# Contributing

Thanks for considering a contribution. prism is small, and the two most common
contributions — a new stream source and a new output sink — are each designed
to be a one-file change.

## Quick start

```sh
git clone https://github.com/stellar-optics/stellar-prism
cd stellar-prism
go build ./...
go test ./...
```

You need **Go 1.25 or later**, the floor required by
`github.com/stellar/go-stellar-sdk`.

**The test suite makes no network calls.** The RPC client sits behind an
interface and the streaming tests run against an in-memory fake, so
`go test ./...` works offline and gives the same result every run. Please keep
it that way.

## Running the checks CI runs

```sh
gofmt -l .                  # must print nothing
go vet ./...
go test -race ./...
golangci-lint run ./...
```

Install the linter if you need it:

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

For coverage:

```sh
go test -race -covermode=atomic -coverprofile=coverage.out \
  -coverpkg=./pkg/...,./internal/cli ./...
go tool cover -html=coverage.out
```

## Good first contributions

See [docs/architecture.md](docs/architecture.md) for the details.

### Add a stream source

Implement `stream.Source` and wire it into a command. `pkg/stream/ledgers.go`
is about 100 lines and is the shortest worked example. `getTransactions` is an
obvious next one — it is already a cursor-paginated API of the same shape.

### Add an output sink

Implement `stream.Sink`. A file rotator or a webhook poster would each be
small.

### Improve the fake

`internal/fakerpc` is where new failure modes get simulated. If you can think
of a way a real RPC misbehaves that prism does not yet survive, adding the
scenario is a genuinely valuable contribution even without a fix.

## Project conventions

### Go only

The whole project is Go — library, CLI, tests and tooling. Please do not
introduce another language or a JS build step. Shell is fine for trivial CI
glue.

Otherwise ordinary idiomatic Go: small interfaces, errors wrapped with `%w`,
`context.Context` threaded through anything doing I/O, no reflection-heavy
magic, no DI framework.

### Do not reimplement XDR decoding

Everything XDR goes through
[stellar-xdr-lens](https://github.com/stellar-optics/stellar-xdr-lens). If you
need decoding prism cannot currently do, the fix usually belongs in lens — open
an issue there, or here and we will work out which. Two tools that disagree
about what a value means would be worse than one that is missing a feature.

### The streaming invariants

If you touch `pkg/stream/tail.go`, you are touching the part everything else
rests on. Three rules:

1. **Retry must never advance the cursor.** This is what makes a dropped
   connection safe.
2. **`Record.ID` must stay fixed-width and zero-padded.** De-duplication is a
   lexicographic comparison; a variable-width ID breaks it silently.
3. **A gap must never be passed over.** Detect it and stop.

Please also re-run the mutation exercise described in
[docs/architecture.md](docs/architecture.md#mutation-checked): break
de-duplication and gap detection on purpose and confirm the suite still fails.
A test suite that cannot fail is not protecting anything.

### Tests

New behaviour needs a test. Table-driven is the house style:

```go
tests := []struct {
	name string
	// inputs and expectations
}{
	{name: "...", /* ... */},
}

for _, tc := range tests {
	t.Run(tc.name, func(t *testing.T) {
		t.Parallel()
		// ...
	})
}
```

For anything streaming, assert through the shared invariant helper rather than
on ad-hoc output, and give follow-loop tests a sleep budget so a regression
fails instead of hanging.

**Never add a test that makes a network call.**

### Commit messages

[Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short summary in the imperative mood>

<body explaining why, not what — the diff already says what>
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `ci`.
Scopes: `stream`, `sink`, `rpc`, `cli`, `docs`.

Examples:

```
feat(stream): add a transactions source over getTransactions
fix(cli): stop --topic splitting a filter on its own segment separator
docs(architecture): explain why retry reuses the cursor
```

Commit granularly. One logical change per commit.

### Branch naming

```
<type>/<short-description>
```

For example `feat/transactions-source`, `fix/cursor-advance-on-error`.

## Pull requests

Before opening one:

- [ ] `go build ./...` passes
- [ ] `go test -race ./...` passes
- [ ] `golangci-lint run ./...` reports no issues
- [ ] `gofmt -l .` prints nothing
- [ ] New behaviour has tests, and no test touches the network
- [ ] Public API changes carry doc comments
- [ ] Commits follow Conventional Commits

Include the linked issue, what changed and why, and **evidence it works** —
actual command output, not a claim that you ran it.

Please open an issue before starting anything large.

## Reporting bugs

For a streaming bug, the most useful report includes what you ran, what you
expected, and what you saw — plus, if you can, the sequence of RPC responses
that triggered it. If you can express it as a `fakerpc` scenario, that is a
reproduction we can put straight into the suite.

## Security

Do not report security issues in a public issue. See [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).

## Licence

By contributing, you agree that your contributions are licensed under
Apache-2.0, the same licence as the project.
