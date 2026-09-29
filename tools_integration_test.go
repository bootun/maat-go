//go:build integration

package maat_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bootun/maat-go"
	"github.com/bootun/maat-go/internal/testenv"
)

type fileArgs struct {
	Path string `json:"path"`
}

func readFileTool(prefix string) maat.Tool {
	return maat.NewTool("read_file", "read a file", maat.SchemaFor[fileArgs](),
		func(_ context.Context, a fileArgs) (string, error) { return prefix + a.Path, nil }, maat.Idempotent())
}

// 真实的工具往返：两个并行调用，一个成功、一个返回错误；Run.Stream 自动认领、执行、回传，
// 第二个 step 按模型给出的顺序看到两个结果。
func TestIntegration_ToolRoundTrip(t *testing.T) {
	e := testenv.Load(t)
	agent := e.NewAgent(t, "script", testenv.Script(
		`{"tool_calls":[{"name":"read_file","args":{"path":"a.txt"}},{"name":"fail","args":{}}]}`,
		`{"text":"result: {{tool_results}}"}`,
	))
	fail := maat.NewTool("fail", "always fails", nil, func(context.Context, map[string]any) (string, error) {
		return "", errors.New("boom")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := newClient(e).Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Tools: []maat.Tool{readFileTool("content of "), fail}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "go")
	if err != nil {
		t.Fatal(err)
	}
	completed := map[string]bool{}
	for ev, err := range run.Stream(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if tc, ok := ev.(maat.ToolCallEvent); ok && tc.Status == maat.ToolCallCompleted {
			completed[tc.Name] = tc.IsError
		}
	}
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if isErr, ok := completed["read_file"]; !ok || isErr {
		t.Fatalf("read_file completed = %v, %v", ok, isErr)
	}
	if isErr, ok := completed["fail"]; !ok || !isErr {
		t.Fatalf("fail completed = %v, %v", ok, isErr)
	}
	if !strings.HasPrefix(res.Text, "result: content of a.txt|") || !strings.Contains(res.Text, "boom") || res.StopReason != maat.StopEndTurn {
		t.Fatalf("result = %+v", res)
	}
}

// Executor 模式：发送消息的一方不执行工具；调用发起之后才接入的 Executor 通过补拉认领并完成它。
func TestIntegration_ExecutorAttach(t *testing.T) {
	e := testenv.Load(t)
	agent := e.NewAgent(t, "script", testenv.Script(
		`{"tool_calls":[{"name":"read_file","args":{"path":"b.txt"}}]}`,
		`{"text":"result: {{tool_results}}"}`,
	))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := newClient(e).Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Tools: []maat.Tool{readFileTool("unused ")}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "go")
	if err != nil {
		t.Fatal(err)
	}
	actx, stop := context.WithCancel(ctx)
	defer stop()
	attached := make(chan error, 1)
	started := false
	var claimedBy string
	for ev, err := range run.Stream(ctx, maat.WithAutoExecute(false)) {
		if err != nil {
			t.Fatal(err)
		}
		tc, ok := ev.(maat.ToolCallEvent)
		switch {
		case ok && tc.Status == maat.ToolCallPending && !started:
			// 调用已经发起：这时才接入 Executor（另一个 Client，相当于另一个进程）。
			started = true
			go func() { attached <- newClient(e).Executor(readFileTool("remote ")).Attach(actx, s.ID) }()
		case ok && tc.Status == maat.ToolCallClaimed:
			claimedBy = tc.ExecutorID
		}
	}
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != "result: remote b.txt" || !strings.HasPrefix(claimedBy, "exe_") {
		t.Fatalf("result = %+v, claimed by %q", res, claimedBy)
	}
	stop()
	if err := <-attached; !errors.Is(err, context.Canceled) {
		t.Fatalf("Attach = %v", err)
	}
}

// 工具执行期间中断：工具函数的 ctx 以 ErrToolCallCancelled 取消，Run 以 interrupted 结束。
func TestIntegration_InterruptCancelsTool(t *testing.T) {
	e := testenv.Load(t)
	agent := e.NewAgent(t, "script", testenv.Script(`{"tool_calls":[{"name":"wait","args":{}}]}`))
	started, causes := make(chan struct{}), make(chan error, 1)
	wait := maat.NewTool("wait", "waits until cancelled", nil, func(ctx context.Context, _ map[string]any) (string, error) {
		close(started)
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return "", ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := newClient(e).Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Tools: []maat.Tool{wait}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "go")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		select {
		case <-started:
			if err := s.Interrupt(ctx); err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
		}
	}()
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.StopReason != maat.StopInterrupted {
		t.Fatalf("result = %+v", res)
	}
	select {
	case cause := <-causes:
		if !errors.Is(cause, maat.ErrToolCallCancelled) {
			t.Fatalf("cause = %v", cause)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the tool was not cancelled")
	}
}
