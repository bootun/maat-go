//go:build integration

package maat_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bootun/maat-go"
	"github.com/bootun/maat-go/internal/testenv"
)

// SubAgent 往返（plan M4-06）：主线程 spawn 一个前台子线程，子线程调用客户端工具；Run.Stream 自动执行子线程的
// 工具调用，WithSubthreads 时产出子线程的事件；Session.Threads 列出两个线程；Send(WithThread) 直接追问子线程，
// 该 Run 结束时不向主线程投递。
func TestIntegration_SubAgent(t *testing.T) {
	e := testenv.Load(t)
	agent := e.NewAgentWithSubagents(t, "script", testenv.Script(
		`{"tool_calls":[{"name":"spawn_agent","args":{"agent":"reader","task":"read c.txt"}}]}`,
		`{"text":"parent: {{tool_results}}"}`,
	), testenv.SubAgent{Name: "reader", Tools: []string{"read_file"}, Script: testenv.Script(
		`{"tool_calls":[{"name":"read_file","args":{"path":"c.txt"}}]}`,
		`{"text":"child read {{tool_results}}"}`,
		`{"text":"follow-up: {{last_user}}"}`,
	)})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := newClient(e)
	s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Tools: []maat.Tool{readFileTool("content of ")}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "go")
	if err != nil {
		t.Fatal(err)
	}
	var (
		child     maat.ThreadCreatedEvent
		childText string
		toolOn    string
	)
	for ev, err := range run.Stream(ctx, maat.WithSubthreads()) {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case maat.ThreadCreatedEvent:
			child = e
		case maat.TextEvent:
			if e.Final && e.ThreadID == child.ThreadID {
				childText = e.Text
			}
		case maat.ToolCallEvent:
			if e.Name == "read_file" && e.Status == maat.ToolCallCompleted {
				toolOn = e.ThreadID
			}
		}
	}
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if child.ThreadID == "" || child.ParentThreadID != s.PrimaryThreadID || child.AgentName != "reader" ||
		child.Mode != maat.ThreadForeground || child.Depth != 1 {
		t.Fatalf("thread.created = %+v", child)
	}
	if toolOn != child.ThreadID || childText != "child read content of c.txt" {
		t.Fatalf("child tool on %q, text %q", toolOn, childText)
	}
	if !strings.HasPrefix(res.Text, "parent: ") || !strings.Contains(res.Text, "child read content of c.txt") {
		t.Fatalf("parent result = %+v", res)
	}

	threads, err := s.Threads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(threads) != 2 || threads[0].ID != s.PrimaryThreadID || threads[1].ID != child.ThreadID ||
		threads[1].AgentName != "reader" || threads[1].Status != maat.ThreadIdle || threads[1].StopReason != maat.StopEndTurn {
		t.Fatalf("threads = %+v", threads)
	}

	// 直接追问子线程：新 Run 属于子线程，结束时不向主线程投递。
	s, err = c.Sessions.Get(ctx, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	s.Tools = []maat.Tool{readFileTool("content of ")}
	before := s.LastSeq
	follow, err := s.Send(ctx, "and Y?", maat.WithThread(child.ThreadID))
	if err != nil {
		t.Fatal(err)
	}
	if follow.ThreadID != child.ThreadID || follow.Delivery != maat.DeliveryNewRun {
		t.Fatalf("follow-up run = %+v", follow)
	}
	if fres, err := follow.Wait(ctx); err != nil || fres.Text != "follow-up: and Y?" || fres.StopReason != maat.StopEndTurn {
		t.Fatalf("follow-up = %+v, %v", fres, err)
	}
	for ev, err := range s.History(ctx, before) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.GetThreadId() == s.PrimaryThreadID {
			t.Fatalf("follow-up delivered to the primary thread: %s", ev.GetType())
		}
	}
}
