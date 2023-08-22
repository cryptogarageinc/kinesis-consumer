.PHONY: all
all: install format lint

# lib/app version
golangci_version = v1.53.3
goimports_version = v0.12.0
yamlfmt_version = v0.9.0

.PHONY: format
format:
	go run golang.org/x/tools/cmd/goimports@${goimports_version} -w .
	go run github.com/google/yamlfmt/cmd/yamlfmt@${yamlfmt_version}
	go mod tidy

.PHONY: lint
lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@${golangci_version} run
	cd pkg/zap && go run github.com/golangci/golangci-lint/cmd/golangci-lint@${golangci_version} run
