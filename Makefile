test:
	go test -v -race ./...

lint:
	golangci-lint run

fmt:
	go fmt ./...

build:
	go build ./...

