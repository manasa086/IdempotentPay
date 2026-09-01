.PHONY: build run test race vet cover clean

build:
	go build -o bin/idempotentpay ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./...

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

cover:
	go test -race -covermode=atomic -coverpkg=./internal/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

clean:
	rm -rf bin coverage.out
