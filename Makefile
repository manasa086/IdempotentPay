.PHONY: build run run-pg test race vet cover clean

# PostgreSQL used by run-pg and the database tests. Override on the command line,
# e.g. make race TEST_DATABASE_URL=postgres://user:pass@host:5432/db
DATABASE_URL ?= postgres://localhost:5432/idempotentpay?sslmode=disable
TEST_DATABASE_URL ?= postgres://localhost:5432/idempotentpay_test?sslmode=disable
export TEST_DATABASE_URL

build:
	go build -o bin/idempotentpay ./cmd/server

run:
	go run ./cmd/server

run-pg:
	go run ./cmd/server -store=postgres -database-url "$(DATABASE_URL)"

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
