//go:build integration

package maat_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootun/maat-go"
	"github.com/bootun/maat-go/internal/testenv"
)

// fork 与 executor_state_ref 往返（plan M3-06）：工具回传 executor_state_ref → 出现在并入结果的 checkpoint 上
// → 事后标注 → 从稳定 checkpoint fork 并继续运行 → include_ancestors 的历史顺序 → Executor.OnFork 拿到 ref。
func TestIntegration_ForkAndExecutorStateRef(t *testing.T) {
	e := testenv.Load(t)
	agent := e.NewAgent(t, "script", testenv.Script(
		`{"tool_calls":[{"name":"write","args":{"path":"a.txt"}}]}`,
		`{"text":"result: {{tool_results}} after {{last_user}}"}`,
	))
	write := maat.NewTool("write", "write a file", maat.SchemaFor[fileArgs](),
		func(ctx context.Context, a fileArgs) (string, error) {
			if err := maat.SetExecutorStateRef(ctx, "git:"+a.Path); err != nil {
				return "", err
			}
			return "wrote " + a.Path, nil
		})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := newClient(e)
	s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Tools: []maat.Tool{write}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "go")
	if err != nil {
		t.Fatal(err)
	}
	if res, err := run.Wait(ctx); err != nil || res.Text != "result: wrote a.txt after go" {
		t.Fatalf("source run = %+v, %v", res, err)
	}

	// 工具结果并入时生成的稳定 checkpoint 带着工具设置的 ref。
	var withRef, last maat.Checkpoint
	for cp, err := range c.Checkpoints.List(ctx, s.ID, maat.ListCheckpointsParams{StableOnly: true}) {
		if err != nil {
			t.Fatal(err)
		}
		if cp.ExecutorStateRef == "git:a.txt" {
			withRef = cp
		}
		last = cp
	}
	if withRef.ID == "" || last.Kind != maat.CheckpointTurn || last.ExecutorStateRef != "" {
		t.Fatalf("checkpoints: with ref %+v, last %+v", withRef, last)
	}
	annotated, err := c.Checkpoints.Annotate(ctx, last.ID, "git:annotated")
	if err != nil || annotated.ExecutorStateRef != "git:annotated" {
		t.Fatalf("annotate = %+v, %v", annotated, err)
	}

	// 从工具结果并入后的 checkpoint fork 并附带消息：模型看到源会话的工具结果与新消息。
	forked, err := c.Sessions.Fork(ctx, maat.ForkSessionParams{
		CheckpointID: withRef.ID, Message: maat.TextMessage("again"), Tools: []maat.Tool{write},
	})
	if err != nil {
		t.Fatal(err)
	}
	if forked.ExecutorStateRef != "git:a.txt" || forked.Run == nil || forked.Session.ForkedFrom == nil ||
		forked.Session.ForkedFrom.CheckpointID != withRef.ID {
		t.Fatalf("fork = %+v", forked)
	}
	if res, err := forked.Run.Wait(ctx); err != nil || res.Text != "result: wrote a.txt after again" {
		t.Fatalf("forked run = %+v, %v", res, err)
	}

	// include_ancestors：先是源会话 seq ≤ fork 点的事件，再是新会话的事件。
	var chain []maat.RawEvent
	for ev, err := range forked.Session.History(ctx, 0, maat.IncludeAncestors()) {
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, ev)
	}
	boundary := 0
	for i, ev := range chain {
		if ev.GetSessionId() == s.ID {
			if ev.GetSeq() > withRef.Seq || i != boundary {
				t.Fatalf("ancestor event %d = %s/%d", i, ev.GetSessionId(), ev.GetSeq())
			}
			boundary++
		}
	}
	if boundary != int(withRef.Seq) || chain[boundary].GetType() != "session.forked" || chain[boundary].GetSessionId() != forked.Session.ID {
		t.Fatalf("chain boundary at %d (fork seq %d): %v", boundary, withRef.Seq, chain[boundary].GetType())
	}
	for _, err := range forked.Session.History(ctx, 1, maat.IncludeAncestors()) {
		if !errors.Is(err, maat.ErrAncestorsWithAfterSeq) {
			t.Fatalf("afterSeq with ancestors: %v", err)
		}
	}

	// GetBlob：通过 fork 出的会话读取源会话中 agent.message 引用的完整内容。
	var contentRef string
	for _, ev := range chain[:boundary] {
		if m := ev.GetAgentMessage(); m != nil && m.GetContentRef() != "" {
			contentRef = m.GetContentRef()
		}
	}
	if contentRef == "" {
		t.Fatal("no agent.message content ref in ancestor events")
	}
	blob, err := forked.Session.GetBlob(ctx, contentRef)
	if err != nil || blob.Kind != "message" || len(blob.Content) == 0 {
		t.Fatalf("get ancestor blob = %+v, %v", blob, err)
	}

	// 不带消息 fork 出的会话：Executor 接入时先回调 OnFork。从最后一个 checkpoint fork，ref 是事后标注的值。
	idle, err := c.Sessions.Fork(ctx, maat.ForkSessionParams{CheckpointID: last.ID})
	if err != nil || idle.Run != nil || idle.ExecutorStateRef != "git:annotated" {
		t.Fatalf("idle fork = %+v, %v", idle, err)
	}
	got := make(chan maat.ForkInfo, 1)
	attachCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- c.Executor(write).OnFork(func(_ context.Context, info maat.ForkInfo) error {
			got <- info
			return nil
		}).Attach(attachCtx, idle.Session.ID)
	}()
	select {
	case info := <-got:
		if info.ExecutorStateRef != "git:annotated" || info.From.SessionID != s.ID || info.From.CheckpointID != last.ID {
			t.Fatalf("fork info = %+v", info)
		}
	case err := <-done:
		t.Fatalf("attach returned early: %v", err)
	case <-ctx.Done():
		t.Fatal("OnFork was not called")
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("attach = %v", err)
	}
}
