.PHONY: test vet build broker benchmark
test:
	go test ./...
vet:
	go vet ./...
build:
	go build ./cmd/...
broker:
	go run ./cmd/broker
benchmark:
	go run ./cmd/benchmark --n 1000 --workers 4
