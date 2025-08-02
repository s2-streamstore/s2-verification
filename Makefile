.PHONY: build-go test-go clean-go run-go all

# Build the Go linearizability checker
build-go:
	cd golang/linearizability && go build -o ./bin/basic ./cmd/basic

# Run Go tests
test-go:
	cd golang/linearizability/cmd/basic && go test -v

# Clean Go build artifacts
clean-go:
	rm -f golang/linearizability/bin/basic


# Default target - build and test Go
all: build-go test-go