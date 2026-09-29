// basic 演示最小用法：创建会话、发送消息、流式打印回复、等待结果。
//
//	export MAAT_BASE_URL=http://localhost:8080
//	export MAAT_API_KEY=...          # 需要 sessions:read、sessions:write
//	export MAAT_AGENT_ID=agt_...
//	go run ./examples/basic "介绍一下你自己"
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/bootun/maat-go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	agent := os.Getenv("MAAT_AGENT_ID")
	if agent == "" {
		return errors.New("MAAT_AGENT_ID is not set")
	}
	prompt := "Hello!"
	if len(os.Args) > 1 {
		prompt = strings.Join(os.Args[1:], " ")
	}

	c := maat.NewClient() // 读取 MAAT_BASE_URL、MAAT_API_KEY
	me, err := c.WhoAmI(ctx)
	if err != nil {
		return fmt.Errorf("who am i: %w", err)
	}
	fmt.Printf("connected as %s (project %s)\n", me.Name, me.ProjectID)

	s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Title: "maat-go example"})
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	fmt.Printf("session %s\n\n", s.ID)

	r, err := s.Send(ctx, prompt)
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	// 终端只能追加输出：记录每个 step 已打印的文本，新文本以它为前缀时只打印增量，
	// 否则（发生了回退，或权威内容与预览不同）另起一行重新打印。
	printed := map[string]string{}
	for ev, err := range r.Stream(ctx) {
		if err != nil {
			return fmt.Errorf("stream: %w", err)
		}
		switch e := ev.(type) {
		case maat.TextEvent:
			prev := printed[e.StepID]
			if strings.HasPrefix(e.Text, prev) {
				fmt.Print(e.Text[len(prev):])
			} else {
				fmt.Print("\n" + e.Text)
			}
			printed[e.StepID] = e.Text
		case maat.StepRewoundEvent:
			if printed[e.StepID] != "" {
				fmt.Print("\n[retrying step]\n")
			}
			printed[e.StepID] = ""
		}
	}

	res, err := r.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	if res.Error != nil {
		return fmt.Errorf("run failed: %s: %s", res.Error.Code, res.Error.Message)
	}
	fmt.Printf("\n\n[%s] input %d tokens, output %d tokens\n", res.StopReason, res.Usage.InputTokens, res.Usage.OutputTokens)
	return nil
}
