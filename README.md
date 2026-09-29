# maat-go

maat 平台的 Go SDK（`package maat`），只面向服务端使用：平台 API Key 不得下发到浏览器。

- Go 1.24+
- 只需要两项配置：平台地址与平台 API Key。LLM 的地址、密钥与推理参数都在平台上配置，SDK 中只出现模型别名。

```sh
go get github.com/bootun/maat-go
```

## 快速上手

```go
c := maat.NewClient(
    maat.WithBaseURL(os.Getenv("MAAT_BASE_URL")), // 不设置时也会读取这两个环境变量
    maat.WithAPIKey(os.Getenv("MAAT_API_KEY")),
)

s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: "agt_01j9...", Model: "main-gpt"})
if err != nil {
    return fmt.Errorf("create session: %w", err)
}

run, err := s.Send(ctx, "总结一下 README")
if err != nil {
    return fmt.Errorf("send: %w", err)
}
for ev, err := range run.Stream(ctx) {
    if err != nil {
        return fmt.Errorf("stream: %w", err)
    }
    switch e := ev.(type) {
    case maat.TextEvent:        // e.Text 是该 step 已对账的完整文本，直接替换渲染即可
        render(e.StepID, e.Text)
    case maat.StepRewoundEvent: // 该 step 发生了重试：清空已渲染的内容
        clearRender(e.StepID)
    }
}
res, err := run.Wait(ctx) // {StopReason, Text, Usage, Error}
```

完整示例见 [`examples/basic`](examples/basic/main.go)：

```sh
MAAT_BASE_URL=http://localhost:8080 MAAT_API_KEY=... MAAT_AGENT_ID=agt_... go run ./examples/basic "你好"
```

## 工具

调用方工具由本进程执行：声明随会话提交给平台，模型发起调用后 SDK 自动**认领 → 执行 → 续约 → 回传**。

```go
type ReadFileArgs struct {
    Path string `json:"path" jsonschema:"description=工作区内的相对路径"`
}

readFile := maat.NewTool("read_file", "读取工作区内的文件", maat.SchemaFor[ReadFileArgs](),
    func(ctx context.Context, args ReadFileArgs) (string, error) {
        b, err := os.ReadFile(args.Path)
        if err != nil {
            return "", fmt.Errorf("read %s: %w", args.Path, err) // 自动转为 is_error 结果
        }
        return string(b), nil
    }, maat.Idempotent())

s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: "agt_...", Tools: []maat.Tool{readFile}})
run, err := s.Send(ctx, "总结一下 README")
for ev, err := range run.Stream(ctx) { // 默认自动执行已注册的工具
    if tc, ok := ev.(maat.ToolCallEvent); ok {
        log.Println(tc.Name, tc.Status)
    }
}
```

- **schema**：`json.RawMessage`、`map[string]any`，或 `maat.SchemaFor[T]()`（基于 [invopop/jsonschema](https://github.com/invopop/jsonschema)，
  没有 `omitempty` 的字段为必填）；`nil` 表示没有参数。
- **选项**：`Idempotent()`（执行器掉线后允许平台重新派发）、`Timeout(d)`（SDK 在 d 之后取消工具函数的 ctx）、
  `MaxModelResultBytes(n)`（送入模型的截断阈值）。
- **返回值**：`string` 作为文本；实现了 `ToolResulter` 的值（包括 `maat.ToolResult{Content, IsError}`）按其内容；
  其他值编码为 JSON（对象作为 JSON 内容，其他 JSON 值作为文本）。
- **错误**：返回 error 或 panic 时回传 `is_error` 结果，内容为错误信息（panic 附简短调用栈）。
  平台拒绝结果（例如超过 4MB）时改为回传说明原因的错误结果，保证调用得到解决。
- **取消**：调用被中断或取消、Run 结束、续约失败时取消工具函数的 ctx，`context.Cause(ctx)` 为
  `ErrToolCallCancelled`、`ErrRunEnded`、`ErrLeaseLost` 之一，此时结果不再回传。
- **并发**：每次 `Run.Stream` / `Attach` 最多同时执行 8 个调用（`WithToolConcurrency`）；等待空位的调用尚未认领，
  其他执行器可以先认领。
- `maat.SetExecutorStateRef(ctx, ref)`：在工具函数内调用，随结果回传 executor_state_ref（≤ 1KB）。
- `Send(..., maat.WithTools(...))` 替换这个 Run 的工具集；`Run.Stream(ctx, maat.WithAutoExecute(false))` 关闭自动执行；
  通过 `Sessions.Get` 取得的会话需要设置 `Session.Tools` 后，`Send` 返回的 Run 才会自动执行。

### Executor 模式

只执行工具、不发送消息，适合把工具执行部署在别处：

```go
err := c.Executor(readFile).Attach(ctx, "ses_...") // 阻塞到 ctx 结束
```

接入时先用 `ListPendingToolCalls` 补拉已经发起的调用，再订阅之后的事件；多个执行器同时接入时每个调用只有一个认领成功。
API Key 需要 `tools:execute` 权限。完整示例（`read_file`、`list_dir`，只能访问指定目录）见 [`examples/tools`](examples/tools/main.go)：

```sh
go run ./examples/tools -root . "README 里写了什么？"   # 发送消息并执行工具
go run ./examples/tools -root . -attach ses_...        # Executor 模式
```

## 概念

| API | 说明 |
|---|---|
| `NewClient(opts...)` | `WithBaseURL`、`WithAPIKey`、`WithHTTPClient`、`WithLogger`、`WithToolConcurrency`；缺少配置时每次调用返回 `ErrNoBaseURL` / `ErrNoAPIKey` |
| `Client.WhoAmI` | 返回当前 API Key 的身份，可用作连通性检查 |
| `Sessions.Create / Get / List` | 创建、读取、列出会话（`List` 自动翻页） |
| `Session.Send / SendInput` | 发送消息，返回处理它的 `Run`。线程运行中调用即为**插入消息**，返回正在运行的 Run（`Delivery == DeliveryInserted`） |
| `Session.Interrupt(ctx, opts...)` | 中断线程当前的 Run（`InterruptThread` 指定线程）；中断后立即发新消息用 `Send(..., WithInterrupt())` |
| `Session.History(ctx, afterSeq)` | 按 seq 读取已提交事件 |
| `Session.Stream(ctx, opts...)` | 先补齐历史（`AfterSeq`，默认 0），再接实时流 |
| `Run.Stream(ctx, opts...)` | 只产出该 Run 的事件，直到 `RunCompletedEvent` / `RunFailedEvent`；默认自动执行工具 |
| `Run.Wait(ctx, opts...)` | 等待 Run 结束。Run 失败不算调用错误：`err == nil`，`Result.Error` 有值 |
| `NewTool`、`SchemaFor` | 声明由本进程执行的工具（见[工具](#工具)） |
| `Client.Executor(tools...).Attach(ctx, sessionID)` | Executor 模式：只执行工具调用 |

### 事件

`Stream` 产出按平台的 marker 与回退规则对账后的高层事件：

- `TextEvent{StepID, Text, Final}`：`Text` 是该 step 到目前为止的**完整文本**；`Final` 表示已提交的权威内容。
- `StepRewoundEvent`：该 step 的预览作废（模型调用重试，或流被重置），应清空已渲染的内容。
- `ToolCallEvent`：工具调用的状态变化（pending、claimed、completed、failed、cancelled；重新开放时回到 pending）。
- `StatusEvent`：会话或线程状态变化。
- `RunCompletedEvent`、`RunFailedEvent`：Run 结束。

`WithRawEvents()` 会在高层事件之前把每条原始事件以 `RawEvent`（内嵌 `*maatv1.Event`）一并产出。
自行拼接历史与实时流时，可以直接使用 `NewReconciler()`。

### 断线续传

事件流断开后自动携带 resume token 重连（指数退避 0.5s → 10s，收到事件后重置）；服务端优雅关闭前发送的
closing 心跳会触发立即重连；已提交事件按 seq 去重，不会重复或遗漏。只有 ctx 结束或不可重试的错误
（认证失败、无权限、会话不存在等）会结束 `Stream`。

### 错误

平台返回的错误是 `*maat.MaatError{Code, Reason, Message, Retryable, RequestID}`：

```go
var me *maat.MaatError
if errors.As(err, &me) && me.Reason == "session_archived" { ... }
```

SDK 内部只重试 `Retryable` 的错误。`Send` 与 `Sessions.Create` 自动带上幂等键（也可以用
`WithClientMessageID`、`CreateSessionParams.IdempotencyKey` 指定），内部重试不会重复发送或创建。
向平台方反馈问题时请附上 `RequestID`。

## 生成代码

`gen/` 下是从 `proto/` 生成的 Connect 客户端（`github.com/bootun/maat-go/gen/maat/v1`）。`proto/` 是平台
公开 API（`maat.v1`）中 SDK 用到的文件，从后端仓库同步，**不要手改**：

```sh
make tools                          # 安装固定版本的 buf、protoc-gen-go、protoc-gen-connect-go、golangci-lint
make sync-proto MAAT_DIR=../maat    # 从后端仓库复制 proto（后端仓库不公开，SDK 不依赖它）
make gen                            # 重新生成，生成的代码必须提交
```

注意：同一个程序里不要同时链接本 SDK 与后端仓库生成的 `maat.v1` 代码，两者会注册同名的 proto 文件。

## 开发

```sh
make verify             # 生成代码检查 + lint + 单元测试，提交前必须通过
make test-integration   # 集成测试：先在后端仓库执行 make up
```

集成测试（`//go:build integration`）针对后端的 docker compose 环境运行，覆盖创建、发送、流式、插入、断线续传、
工具往返、Executor 模式与中断。
连接信息默认取 `$(MAAT_DIR)/deploy/.env.e2e`，也可以用环境变量 `MAAT_E2E_BASE_URL`、
`MAAT_E2E_ADMIN_API_KEY`、`MAAT_E2E_SDK_API_KEY` 指定；未配置时跳过。

CI 的集成测试需要仓库 secret `MAAT_BACKEND_TOKEN`（对 `bootun/maat` 有只读权限的 token），用来检出后端仓库并启动环境；
未配置时跳过该 job。
