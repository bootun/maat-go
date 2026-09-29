package maat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// Tool 是由本进程执行的工具：声明（名字、描述、参数 schema）加上实现，用 NewTool 构造。
// 把它传给 CreateSessionParams.Tools、WithTools 或 Client.Executor 后，SDK 在收到对应的工具调用时
// 自动认领、执行、续约并回传结果（spec §14.4）。
type Tool struct {
	def     *maatv1.ToolDefinition
	timeout time.Duration
	call    func(ctx context.Context, args json.RawMessage) ToolResult
	// err 是构造时的错误；非空时使用该工具的调用（创建会话、发送消息、Attach）返回它。
	err error
}

// Name 返回工具名。
func (t Tool) Name() string { return t.def.GetName() }

// check 返回构造错误；零值 Tool（没有经过 NewTool）同样视为错误。
func (t Tool) check() error {
	switch {
	case t.err != nil:
		return t.err
	case t.def == nil:
		return errors.New("maat: tool must be constructed with maat.NewTool")
	}
	return nil
}

// ToolOption 配置 NewTool。
type ToolOption func(*toolOptions)

type toolOptions struct {
	idempotent     bool
	timeout        time.Duration
	maxResultBytes int
}

// Idempotent 声明工具是幂等的：执行器在返回结果前掉线时，平台把调用重新开放给其他执行器
// （最多 3 次），而不是直接以 executor_lost 失败（spec §6.5）。
func Idempotent() ToolOption { return func(o *toolOptions) { o.idempotent = true } }

// Timeout 设置执行超时：SDK 在 d 之后取消传给工具函数的 ctx；d 同时作为建议超时声明给平台
// （按秒向上取整，上限 1 小时）。工具函数在超时后返回的结果照常回传。
func Timeout(d time.Duration) ToolOption { return func(o *toolOptions) { o.timeout = d } }

// MaxModelResultBytes 设置送入模型的结果截断阈值（平台默认 32KB，范围 1KB～1MB）。
// 超出的部分由平台存为 Blob，模型看到的是头部、尾部与截断说明（spec §6.3）。
func MaxModelResultBytes(n int) ToolOption { return func(o *toolOptions) { o.maxResultBytes = n } }

// ToolResult 是工具结果的完整形式：多段内容，以及是否为错误结果。
type ToolResult struct {
	Content []ContentPart
	// IsError 为真时模型把结果视为工具执行失败。
	IsError bool
}

// ToolResult 实现 ToolResulter，工具函数可以直接返回 ToolResult。
func (r ToolResult) ToolResult() ToolResult { return r }

// ToolResulter 由需要自定义结果内容（多段内容、IsError）的返回值实现。
type ToolResulter interface {
	ToolResult() ToolResult
}

// TextResult 构造只有一段文本的结果。
func TextResult(text string) ToolResult { return ToolResult{Content: []ContentPart{{Text: text}}} }

// ErrorResult 构造只有一段文本的错误结果。
func ErrorResult(text string) ToolResult {
	return ToolResult{Content: []ContentPart{{Text: text}}, IsError: true}
}

// NewTool 构造一个工具。
//
//   - schema 是参数的 JSON Schema（顶层 type: object）：json.RawMessage、map[string]any，或 SchemaFor[Args]()；
//     nil 表示工具没有参数。
//   - fn 的参数由模型给出的 JSON 解码为 Args；解码失败时不调用 fn，回传错误结果。
//   - 返回值：string 作为文本；实现了 ToolResulter 的值（包括 ToolResult）使用其内容与 IsError；
//     其他值编码为 JSON：对象作为 JSON 内容，其他 JSON 值（数组、数字等）作为文本。
//   - fn 返回 error 或 panic 时回传 IsError 的结果，内容为错误信息（panic 附简短的调用栈）。
//
// fn 的 ctx 在调用被中断、取消、Run 结束或超时时取消，工具函数应当及时返回。
// 构造错误（schema 无效等）不会立即返回，而是在使用该工具时（创建会话、发送消息、Attach）返回。
func NewTool[Args, Res any](name, description string, schema any, fn func(ctx context.Context, args Args) (Res, error),
	opts ...ToolOption) Tool {
	var o toolOptions
	for _, f := range opts {
		f(&o)
	}
	def := &maatv1.ToolDefinition{Name: name, Description: description, Idempotent: o.idempotent}
	t := Tool{def: def, timeout: o.timeout}
	params, err := schemaStruct(schema)
	switch {
	case err != nil:
		t.err = fmt.Errorf("maat: tool %s: %w", name, err)
	case fn == nil:
		t.err = fmt.Errorf("maat: tool %s: function is nil", name)
	case o.timeout < 0 || o.timeout > time.Hour:
		t.err = fmt.Errorf("maat: tool %s: timeout must be in [0, 1h]", name)
	case o.maxResultBytes < 0 || o.maxResultBytes > 1<<20:
		t.err = fmt.Errorf("maat: tool %s: max model result bytes must be in [0, 1MB]", name)
	}
	def.Parameters = params
	def.TimeoutSeconds = uint32((o.timeout + time.Second - 1) / time.Second)
	def.MaxModelResultBytes = uint32(o.maxResultBytes)
	t.call = func(ctx context.Context, raw json.RawMessage) ToolResult {
		var args Args
		if err := decodeArgs(raw, &args); err != nil {
			return ErrorResult(fmt.Sprintf("invalid arguments for tool %s: %v", name, err))
		}
		return invoke(ctx, func(ctx context.Context) (any, error) { return fn(ctx, args) })
	}
	return t
}

// decodeArgs 把参数解码为 Args；没有参数时按空对象解码。
func decodeArgs(raw json.RawMessage, args any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	return json.Unmarshal(raw, args)
}

// invoke 执行工具函数并转换结果；panic 转为错误结果。
func invoke(ctx context.Context, fn func(context.Context) (any, error)) (res ToolResult) {
	defer func() {
		if r := recover(); r != nil {
			res = ErrorResult(fmt.Sprintf("panic: %v\n%s", r, stackSummary()))
		}
	}()
	v, err := fn(ctx)
	if err != nil {
		return ErrorResult(err.Error())
	}
	return resultOf(v)
}

// resultOf 把工具函数的返回值转换为结果。
func resultOf(v any) ToolResult {
	switch x := v.(type) {
	case string:
		return TextResult(x)
	case ToolResulter:
		return x.ToolResult()
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ErrorResult(fmt.Sprintf("encode tool result: %v", err))
	}
	if bytes.HasPrefix(b, []byte("{")) {
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			return ErrorResult(fmt.Sprintf("encode tool result: %v", err))
		}
		return ToolResult{Content: []ContentPart{{JSON: m}}}
	}
	return TextResult(string(b))
}

// stackFrames 是 panic 调用栈摘要最多包含的帧数。
const stackFrames = 5

// sdkPackage 是本 SDK 的包路径：调用栈摘要到 SDK 自身的帧为止。
const sdkPackage = "github.com/bootun/maat-go."

// stackSummary 返回从 panic 位置到工具函数入口的调用栈摘要（跳过 runtime 的帧，遇到 SDK 的帧为止，
// 最多 stackFrames 帧；文件只保留文件名）。它必须在 recover 所在的 defer 函数中直接调用。
func stackSummary() string {
	pcs := make([]uintptr, 32)
	// 跳过 runtime.Callers、stackSummary 与 defer 函数。
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var b strings.Builder
	for count := 0; count < stackFrames; {
		f, more := frames.Next()
		if strings.HasPrefix(f.Function, sdkPackage) {
			break
		}
		if !strings.HasPrefix(f.Function, "runtime.") {
			fmt.Fprintf(&b, "  at %s (%s:%d)\n", f.Function, filepath.Base(f.File), f.Line)
			count++
		}
		if !more {
			break
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// toProto 转换结果；内容无法表示为 proto 时返回错误。
func (r ToolResult) toProto() (*maatv1.ToolResult, error) {
	msg, err := MessageInput{Parts: r.Content}.toProto()
	if err != nil {
		return nil, err
	}
	return &maatv1.ToolResult{Content: msg.GetParts(), IsError: r.IsError}, nil
}

// invocation 是一次工具执行的上下文，工具函数通过 ctx 访问。
type invocation struct {
	mu       sync.Mutex
	stateRef string
}

type invocationKey struct{}

// maxExecutorStateRefBytes 是 executor_state_ref 的上限（spec §8.4）。
const maxExecutorStateRefBytes = 1 << 10

// ErrNotInTool 表示在工具函数之外调用了只能在工具函数内使用的函数。
var ErrNotInTool = errors.New("maat: not called from a tool function")

// SetExecutorStateRef 在工具函数内调用，设置随结果一起回传的 executor_state_ref（不透明字符串，≤ 1KB）。
// 平台把它记录在并入这些结果时生成的 checkpoint 上，调用方可以据此把自己的状态（例如工作区快照）
// 与会话进度对应起来（spec §8.4）。多次调用时以最后一次为准。
func SetExecutorStateRef(ctx context.Context, ref string) error {
	inv, ok := ctx.Value(invocationKey{}).(*invocation)
	if !ok {
		return ErrNotInTool
	}
	if err := checkExecutorStateRef(ref); err != nil {
		return err
	}
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.stateRef = ref
	return nil
}

func checkExecutorStateRef(ref string) error {
	if len(ref) > maxExecutorStateRefBytes {
		return fmt.Errorf("maat: executor state ref exceeds %d bytes", maxExecutorStateRefBytes)
	}
	return nil
}

func (inv *invocation) ref() string {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.stateRef
}

// toolDefinitions 校验工具并返回它们的声明；工具名重复时报错。
func toolDefinitions(tools []Tool) ([]*maatv1.ToolDefinition, error) {
	defs := make([]*maatv1.ToolDefinition, 0, len(tools))
	seen := make(map[string]bool, len(tools))
	for _, t := range tools {
		if err := t.check(); err != nil {
			return nil, err
		}
		if seen[t.Name()] {
			return nil, fmt.Errorf("maat: duplicate tool name %s", t.Name())
		}
		seen[t.Name()] = true
		defs = append(defs, t.def)
	}
	return defs, nil
}
