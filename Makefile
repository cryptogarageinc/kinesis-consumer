.PHONY: all
all: install format lint

# lib/app version
goimports_version = v0.1.11
golangci_lint_version = v1.48.0

.PHONY: install
install:
	$(eval BIN:=$(abspath .bin))
	mkdir -p ./.bin
	GOBIN="$(BIN)" go install golang.org/x/tools/cmd/goimports@${goimports_version}
	GOBIN="$(BIN)" go install github.com/golangci/golangci-lint/cmd/golangci-lint@${golangci_lint_version}

.PHONY: format
format:
	ls ./.bin/goimports && ./.bin/goimports -w . || goimports -w .
	go mod tidy

.PHONY: lint
lint:
	ls ./.bin/golangci-lint && ./.bin/golangci-lint run || golangci-lint run
	ls ./.bin/golangci-lint && cd pkg/zap && ../../.bin/golangci-lint run ||\
	cd pkg/zap && golangci-lint run
