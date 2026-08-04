module github.com/stellar-optics/stellar-prism

go 1.25

replace github.com/stellar-optics/stellar-xdr-lens => /home/ade/stellar-xdr-lens

require (
	github.com/stellar-optics/stellar-xdr-lens v0.0.0-20260804095708-4447ace53358
	github.com/stellar/go-stellar-sdk v0.7.1
)

require (
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/creachadair/jrpc2 v1.2.0 // indirect
	github.com/creachadair/mds v0.13.4 // indirect
	github.com/klauspost/compress v1.17.6 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stellar/go-xdr v0.0.0-20260529210834-0bf8f4956364 // indirect
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d // indirect
	golang.org/x/sync v0.18.0 // indirect
)
