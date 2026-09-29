package maat_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/bootun/maat-go"
)

type panicArgs struct {
	N int `json:"n"`
}

func explode(n int) string {
	xs := make([]string, n)
	return xs[n] // 越界：运行时 panic
}

// panic 转为错误结果，内容附带从 panic 位置到工具函数的调用栈摘要（不含 runtime 与 SDK 自身的帧）。
func TestToolPanicBecomesErrorResult(t *testing.T) {
	tool := maat.NewTool("explode", "", nil, func(_ context.Context, a panicArgs) (string, error) {
		return explode(a.N), nil
	})
	res := maat.CallTool(context.Background(), tool, `{"n":1}`)
	if !res.IsError || len(res.Content) != 1 {
		t.Fatalf("result = %+v", res)
	}
	text := res.Content[0].Text
	lines := strings.Split(text, "\n")
	if lines[0] != "panic: runtime error: index out of range [1] with length 1" {
		t.Fatalf("first line = %q", lines[0])
	}
	if len(lines) < 3 || !strings.Contains(lines[1], "maat-go_test.explode (tool_panic_test.go:") ||
		!strings.Contains(lines[2], "maat-go_test.TestToolPanicBecomesErrorResult.func1 (tool_panic_test.go:") {
		t.Fatalf("stack summary = %q", text)
	}
	frame := regexp.MustCompile(`^  at \S+ \([^/()]+\.go:\d+\)$`)
	for _, l := range lines[1:] {
		if !frame.MatchString(l) || strings.Contains(l, "runtime.") || strings.Contains(l, "maat-go.") {
			t.Fatalf("stack summary leaks runtime, SDK frames or directories: %q", text)
		}
	}
}

// 显式 panic 的值原样出现在结果中。
func TestToolPanicValue(t *testing.T) {
	tool := maat.NewTool("boom", "", nil, func(context.Context, map[string]any) (string, error) { panic("boom") })
	res := maat.CallTool(context.Background(), tool, `{}`)
	if !res.IsError || !strings.HasPrefix(res.Content[0].Text, "panic: boom\n  at ") {
		t.Fatalf("result = %+v", res)
	}
}
