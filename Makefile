# maat-go Makefile。

# ---------- 固定版本的工具 ----------
BUF_VERSION                   := v1.73.0
GOLANGCI_LINT_VERSION         := v2.14.0
PROTOC_GEN_GO_VERSION         := v1.36.12
# 与 go.mod 中 connectrpc.com/connect 的版本一致。
PROTOC_GEN_CONNECT_GO_VERSION := v1.19.1

BIN := $(CURDIR)/bin
export PATH := $(BIN):$(PATH)
export GOBIN := $(BIN)

GO ?= go

# 后端仓库（闭源）的本地检出，只用于同步 proto 与运行集成测试。
MAAT_DIR ?= ../maat
# SDK 用到的公开 API proto（maat.v1 的子集）。新功能需要其他文件时加在这里，然后 make sync-proto gen。
PROTO_FILES := common meta tool session event

.PHONY: tools
tools: ## 安装固定版本的工具到 ./bin
	$(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go@$(PROTOC_GEN_CONNECT_GO_VERSION)

.PHONY: sync-proto
sync-proto: ## 从后端仓库同步公开 API 的 proto（MAAT_DIR 指向后端仓库）
	@test -d $(MAAT_DIR)/proto/maat/v1 || { echo "MAAT_DIR=$(MAAT_DIR) is not a maat checkout"; exit 1; }
	rm -f proto/maat/v1/*.proto
	for f in $(PROTO_FILES); do cp $(MAAT_DIR)/proto/maat/v1/$$f.proto proto/maat/v1/; done

.PHONY: gen
gen: ## 生成 gen/ 下的代码（生成的代码必须提交）
	buf generate

.PHONY: gen-check
gen-check: gen ## 重新生成后无 diff（含未跟踪的新文件）
	@git add -N gen >/dev/null 2>&1 || true
	git diff --exit-code -- gen

.PHONY: lint
lint: ## buf lint + golangci-lint
	buf lint
	golangci-lint run ./...

.PHONY: fmt
fmt: ## 格式化 Go 代码
	golangci-lint fmt ./...

.PHONY: test
test: ## 单元测试
	$(GO) test -race ./...

.PHONY: test-integration
test-integration: ## 集成测试：需要先在后端仓库执行 make up
	MAAT_E2E_ENV_FILE=$${MAAT_E2E_ENV_FILE:-$(MAAT_DIR)/deploy/.env.e2e} $(GO) test -race -count=1 -tags=integration ./...

.PHONY: verify
verify: gen-check lint test ## 提交前必须通过

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'
