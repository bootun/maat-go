# AGENTS.md — maat-go（Go SDK）

## 必读
- 后端仓库（`bootun/maat`，闭源）的 `docs/spec.md` §11（事件与流式协议）、§14（SDK 设计）与 `docs/plan.md` 中的 SDK 任务（M1-18、M2-10、M3-06、M4-06）。
- 本仓库不依赖后端仓库：`proto/` 从后端同步，`gen/` 在本仓库生成（后端 ADR 0019）。

## 常用命令
- `make tools`：安装固定版本的工具到 ./bin
- `make sync-proto MAAT_DIR=../maat` + `make gen`：同步 proto 并重新生成（生成的代码必须提交）
- `make verify`：提交前必须通过（gen-check + lint + 单元测试）
- `make test-integration`：针对后端 compose 环境（先在后端执行 `make up`）

## 约定
- 只依赖 connect、protobuf、invopop/jsonschema（`SchemaFor`，后端 plan M2-10 指定）与标准库；新增依赖前先确认必要性，
  只选用依赖树中没有预发布版本的正式版本。最低 Go 版本 1.24。
- 公开 API 只暴露 SDK 自己的类型；原始事件通过 `RawEvent` 内嵌 `*maatv1.Event`。
- 平台错误统一转换为 `*MaatError`；只重试 `Retryable` 的错误；有副作用的调用必须带幂等键才能重试。
- 不打印输出、不使用标准库 `log`、不使用 `http.DefaultClient`；API Key 不得出现在错误信息中。
- 对账逻辑（`reconcile.go`）改动时同步更新 fixture（`reconcile_test.go`），TS SDK 与控制台共用同一套规则。

## 测试
- 单元测试：表驱动；RPC 用 `fake_test.go` 的假平台（httptest + 生成的 handler）。
- 集成测试：`//go:build integration`，用 `internal/testenv` 创建指向 fake LLM 的 Agent。

## 提交
- 提交信息 `[任务编号] 简述`，例如 `[M1-18] Go SDK v0`。
- 发布：打 tag `vX.Y.Z`（M1-18 为 v0.1.0，M2-10 为 v0.2.0），同时更新 `maat.go` 中的 `Version`。
- 工具执行：不记录工具参数与结果的内容；工具函数的 ctx 被执行器取消时不回传结果。
