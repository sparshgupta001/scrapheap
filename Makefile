.PHONY: dash-dev test build

dash-dev:
	go run ./cmd/dashdev

test:
	go test -race ./...

build:
	go build -o bin/dashdev ./cmd/dashdev
