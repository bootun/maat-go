package maat

import (
	"context"
	"encoding/json"
)

// CallTool 在测试中直接执行工具函数（不经过执行器）。
func CallTool(ctx context.Context, t Tool, args string) ToolResult {
	return t.call(ctx, json.RawMessage(args))
}
