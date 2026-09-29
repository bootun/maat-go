package maat

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type readFileArgs struct {
	Path  string `json:"path" jsonschema:"description=workspace-relative path"`
	Limit int    `json:"limit,omitempty"`
}

type point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type customResult struct{ n int }

func (c customResult) ToolResult() ToolResult {
	return ToolResult{Content: []ContentPart{{Text: "n="}, {JSON: map[string]any{"n": c.n}}}, IsError: c.n < 0}
}

func TestSchemaStruct(t *testing.T) {
	tests := []struct {
		name    string
		schema  any
		want    map[string]any
		wantErr string
	}{
		{name: "nil 表示没有参数", schema: nil},
		{name: "nil RawMessage", schema: json.RawMessage(nil)},
		{
			name:   "json.RawMessage",
			schema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`),
			want:   map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		},
		{
			name:   "map[string]any",
			schema: map[string]any{"type": "object", "required": []string{"a"}},
			want:   map[string]any{"type": "object", "required": []any{"a"}},
		},
		{name: "顶层不是 object", schema: json.RawMessage(`{"type":"string"}`), wantErr: `"type": "object"`},
		{name: "不是 JSON 对象", schema: json.RawMessage(`[1]`), wantErr: "not a JSON object"},
		{name: "多余的数据", schema: json.RawMessage(`{"type":"object"} {}`), wantErr: "trailing data"},
		{name: "不支持的类型", schema: `{"type":"object"}`, wantErr: "unsupported schema type string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := schemaStruct(tt.schema)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("schema = %v, want nil", got)
				}
				return
			}
			if !reflect.DeepEqual(got.AsMap(), tt.want) {
				t.Fatalf("schema = %v, want %v", got.AsMap(), tt.want)
			}
		})
	}
}

func TestSchemaFor(t *testing.T) {
	st, err := schemaStruct(SchemaFor[readFileArgs]())
	if err != nil {
		t.Fatal(err)
	}
	m := st.AsMap()
	if m["type"] != "object" || m["additionalProperties"] != false || m["$schema"] != nil || m["$id"] != nil {
		t.Fatalf("schema = %v", m)
	}
	props, _ := m["properties"].(map[string]any)
	path, _ := props["path"].(map[string]any)
	if path["type"] != "string" || path["description"] != "workspace-relative path" {
		t.Fatalf("path = %v", path)
	}
	if req, _ := m["required"].([]any); !reflect.DeepEqual(req, []any{"path"}) {
		t.Fatalf("required = %v", m["required"])
	}
	// 指针、map 与匿名结构体同样得到 object schema。
	for name, s := range map[string]any{
		"pointer": SchemaFor[*readFileArgs](), "map": SchemaFor[map[string]any](), "anonymous": SchemaFor[struct{ A int }](),
	} {
		if st, err := schemaStruct(s); err != nil || st.AsMap()["type"] != "object" {
			t.Errorf("%s schema = %v, %v", name, st, err)
		}
	}
}

func TestNewToolDefinition(t *testing.T) {
	tool := NewTool("read_file", "read a file", SchemaFor[readFileArgs](),
		func(context.Context, readFileArgs) (string, error) { return "", nil },
		Idempotent(), Timeout(1500*time.Millisecond), MaxModelResultBytes(4096))
	if err := tool.check(); err != nil {
		t.Fatal(err)
	}
	d := tool.def
	if tool.Name() != "read_file" || d.GetDescription() != "read a file" || !d.GetIdempotent() || d.GetTimeoutSeconds() != 2 ||
		d.GetMaxModelResultBytes() != 4096 || d.GetParameters().AsMap()["type"] != "object" {
		t.Fatalf("definition = %v", d)
	}
	if tool.timeout != 1500*time.Millisecond {
		t.Fatalf("timeout = %v", tool.timeout)
	}
}

func TestNewToolErrors(t *testing.T) {
	noop := func(context.Context, readFileArgs) (string, error) { return "", nil }
	tests := []struct {
		name string
		tool Tool
		want string
	}{
		{name: "schema 无效", tool: NewTool("t", "", json.RawMessage(`{"type":"array"}`), noop), want: "tool t:"},
		{name: "函数为 nil", tool: NewTool[readFileArgs, string]("t", "", nil, nil), want: "function is nil"},
		{name: "超时为负", tool: NewTool("t", "", nil, noop, Timeout(-time.Second)), want: "timeout"},
		{name: "超时超过 1 小时", tool: NewTool("t", "", nil, noop, Timeout(2*time.Hour)), want: "timeout"},
		{name: "截断阈值超过 1MB", tool: NewTool("t", "", nil, noop, MaxModelResultBytes(2<<20)), want: "max model result bytes"},
		{name: "零值", tool: Tool{}, want: "maat.NewTool"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.tool.check(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			if _, err := toolDefinitions([]Tool{tt.tool}); err == nil {
				t.Fatal("toolDefinitions accepted an invalid tool")
			}
		})
	}
	a := NewTool("dup", "", nil, noop)
	if _, err := toolDefinitions([]Tool{a, a}); err == nil || !strings.Contains(err.Error(), "duplicate tool name dup") {
		t.Fatalf("duplicate: %v", err)
	}
}

func TestToolResultConversion(t *testing.T) {
	text := func(s string) ToolResult { return TextResult(s) }
	tests := []struct {
		name string
		tool Tool
		args string
		want ToolResult
	}{
		{
			name: "string 作为文本",
			tool: NewTool("t", "", nil, func(_ context.Context, a readFileArgs) (string, error) { return "read " + a.Path, nil }),
			args: `{"path":"a.txt"}`,
			want: text("read a.txt"),
		},
		{
			name: "没有参数时按空对象解码",
			tool: NewTool("t", "", nil, func(_ context.Context, a readFileArgs) (string, error) { return "path=" + a.Path, nil }),
			want: text("path="),
		},
		{
			name: "ToolResult 原样使用",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) (ToolResult, error) { return ErrorResult("nope"), nil }),
			args: `{}`,
			want: ToolResult{Content: []ContentPart{{Text: "nope"}}, IsError: true},
		},
		{
			name: "实现 ToolResulter 的类型",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) (customResult, error) { return customResult{n: -1}, nil }),
			want: ToolResult{Content: []ContentPart{{Text: "n="}, {JSON: map[string]any{"n": -1}}}, IsError: true},
		},
		{
			name: "结构体编码为 JSON 对象",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) (point, error) { return point{X: 1, Y: 2}, nil }),
			want: ToolResult{Content: []ContentPart{{JSON: map[string]any{"x": float64(1), "y": float64(2)}}}},
		},
		{
			name: "非对象的 JSON 值作为文本",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) ([]int, error) { return []int{1, 2}, nil }),
			want: text("[1,2]"),
		},
		{
			name: "nil 指针作为 null",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) (*point, error) { return nil, nil }),
			want: text("null"),
		},
		{
			name: "error 转为错误结果",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) (string, error) {
				return "ignored", errors.New("read a.txt: permission denied")
			}),
			want: ErrorResult("read a.txt: permission denied"),
		},
		{
			name: "参数类型不符时不调用函数",
			tool: NewTool("t", "", nil, func(context.Context, readFileArgs) (string, error) { panic("must not be called") }),
			args: `{"path":1}`,
			want: ErrorResult("invalid arguments for tool t: json: cannot unmarshal number into Go struct field readFileArgs.path of type string"),
		},
		{
			name: "无法编码的返回值",
			tool: NewTool("t", "", nil, func(context.Context, map[string]any) (func(), error) { return func() {}, nil }),
			want: ErrorResult("encode tool result: json: unsupported type: func()"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.tool.check(); err != nil {
				t.Fatal(err)
			}
			got := tt.tool.call(context.Background(), json.RawMessage(tt.args))
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("result = %#v\nwant     %#v", got, tt.want)
			}
		})
	}
}

func TestToolResultToProto(t *testing.T) {
	pr, err := ToolResult{Content: []ContentPart{{Text: "a"}, {JSON: map[string]any{"k": "v"}}}, IsError: true}.toProto()
	if err != nil {
		t.Fatal(err)
	}
	if !pr.GetIsError() || len(pr.GetContent()) != 2 || pr.GetContent()[0].GetText() != "a" ||
		pr.GetContent()[1].GetJson().AsMap()["k"] != "v" {
		t.Fatalf("proto = %v", pr)
	}
	if _, err := (ToolResult{Content: []ContentPart{{JSON: map[string]any{"f": func() {}}}}}).toProto(); err == nil {
		t.Fatal("expected an error for a non-JSON value")
	}
}

func TestSetExecutorStateRef(t *testing.T) {
	if err := SetExecutorStateRef(context.Background(), "ref"); !errors.Is(err, ErrNotInTool) {
		t.Fatalf("outside a tool: %v", err)
	}
	inv := &invocation{}
	ctx := context.WithValue(context.Background(), invocationKey{}, inv)
	if err := SetExecutorStateRef(ctx, strings.Repeat("x", 1025)); err == nil {
		t.Fatal("expected an error for a ref over 1KB")
	}
	for _, ref := range []string{"first", "second"} {
		if err := SetExecutorStateRef(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	if inv.ref() != "second" {
		t.Fatalf("ref = %q", inv.ref())
	}
}

func TestLeaseSeconds(t *testing.T) {
	for _, tt := range []struct {
		lease time.Duration
		want  uint32
	}{{30 * time.Millisecond, 10}, {60 * time.Second, 60}, {1500 * time.Millisecond, 10}, {61500 * time.Millisecond, 62}, {time.Hour, 600}} {
		if got := (config{toolLease: tt.lease}).leaseSeconds(); got != tt.want {
			t.Errorf("leaseSeconds(%v) = %d, want %d", tt.lease, got, tt.want)
		}
	}
	if d := defaultConfig(); d.toolConcurrency != 8 || d.toolLease != time.Minute {
		t.Fatalf("defaults = %d, %v", d.toolConcurrency, d.toolLease)
	}
}
