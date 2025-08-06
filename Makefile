.PHONY: build-go test-go clean-go install-go all

# Version can be overridden: make build-go VERSION=v1.2.3
# Defaults to reading from golang/VERSION file, falls back to dev build if file doesn't exist
VERSION ?= $(shell cat golang/VERSION 2>/dev/null || echo "dev-$(shell date +%Y%m%d-%H%M%S)")

# Build the s2-porcupine linearizability checker
build-go:
	cd golang/s2-porcupine && mkdir -p bin && go build -ldflags "-X main.Version=$(VERSION)" -o bin/s2-porcupine .

# Run Go tests
test-go:
	cd golang/s2-porcupine && go test -v

# Clean Go build artifacts
clean-go:
	rm -rf golang/s2-porcupine/bin

# Install s2-porcupine binary to $GOPATH/bin or $GOBIN
install-go:
	cd golang/s2-porcupine && go install -ldflags "-X main.Version=$(VERSION)" .

# Default target - build and test Go
all: build-go test-go