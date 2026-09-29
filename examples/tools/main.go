// tools 演示调用方工具：在本进程中声明并执行 read_file 与 list_dir（只能访问 -root 指定的目录）。
//
//	export MAAT_BASE_URL=http://localhost:8080
//	export MAAT_API_KEY=...          # 需要 sessions:read、sessions:write、tools:execute
//	export MAAT_AGENT_ID=agt_...
//	go run ./examples/tools -root . "README 里写了什么？"
//
// Executor 模式：只为已有会话执行工具，不发送消息（可以部署在另一台机器上）：
//
//	go run ./examples/tools -root . -attach ses_...
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
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

// ReadFileArgs 是 read_file 的参数，schema 由 maat.SchemaFor 生成。
type ReadFileArgs struct {
	Path string `json:"path" jsonschema:"description=相对于工作区根目录的路径"`
}

// ListDirArgs 是 list_dir 的参数。
type ListDirArgs struct {
	Path string `json:"path,omitempty" jsonschema:"description=相对于工作区根目录的目录，默认为根目录"`
}

// Entry 是 list_dir 返回的目录项（结构体结果以 JSON 回传）。
type Entry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

// maxFileBytes 是 read_file 读取的上限；平台对单个结果的上限是 4MB。
const maxFileBytes = 1 << 20

// workspaceTools 返回只能访问 root 目录的工具（os.Root 拒绝 ".." 与指向目录外的符号链接）。
func workspaceTools(root *os.Root) []maat.Tool {
	readFile := maat.NewTool("read_file", "读取工作区内的文本文件", maat.SchemaFor[ReadFileArgs](),
		func(ctx context.Context, a ReadFileArgs) (string, error) {
			f, err := root.Open(a.Path)
			if err != nil {
				return "", fmt.Errorf("open %s: %w", a.Path, err) // 错误作为 is_error 结果交给模型
			}
			defer f.Close()
			b, err := io.ReadAll(io.LimitReader(f, maxFileBytes))
			if err != nil {
				return "", fmt.Errorf("read %s: %w", a.Path, err)
			}
			return string(b), nil
		}, maat.Idempotent())

	listDir := maat.NewTool("list_dir", "列出工作区内的目录", maat.SchemaFor[ListDirArgs](),
		func(ctx context.Context, a ListDirArgs) (map[string][]Entry, error) {
			dir := a.Path
			if dir == "" {
				dir = "."
			}
			entries, err := fs.ReadDir(root.FS(), dir)
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", dir, err)
			}
			out := make([]Entry, 0, len(entries))
			for _, e := range entries {
				info, err := e.Info()
				if err != nil {
					continue
				}
				out = append(out, Entry{Name: e.Name(), IsDir: e.IsDir(), Size: info.Size()})
			}
			return map[string][]Entry{"entries": out}, nil
		}, maat.Idempotent())
	return []maat.Tool{readFile, listDir}
}

func run() error {
	rootDir := flag.String("root", ".", "工具可以访问的目录")
	attach := flag.String("attach", "", "Executor 模式：为这个会话执行工具，不发送消息")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	root, err := os.OpenRoot(*rootDir)
	if err != nil {
		return fmt.Errorf("open root: %w", err)
	}
	defer root.Close()
	tools := workspaceTools(root)
	c := maat.NewClient() // 读取 MAAT_BASE_URL、MAAT_API_KEY

	if *attach != "" {
		fmt.Printf("executing tools for session %s (Ctrl-C to stop)\n", *attach)
		if err := c.Executor(tools...).Attach(ctx, *attach); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("attach: %w", err)
		}
		return nil
	}

	agent := os.Getenv("MAAT_AGENT_ID")
	if agent == "" {
		return errors.New("MAAT_AGENT_ID is not set")
	}
	prompt := "列出工作区的文件，并总结 README 的内容。"
	if flag.NArg() > 0 {
		prompt = strings.Join(flag.Args(), " ")
	}
	s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Title: "maat-go tools example", Tools: tools})
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	fmt.Printf("session %s\n\n", s.ID)

	r, err := s.Send(ctx, prompt)
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	// Stream 自动执行模型发起的工具调用；这里只打印已提交的文本与工具调用的状态。
	for ev, err := range r.Stream(ctx, maat.WithoutDeltas()) {
		if err != nil {
			return fmt.Errorf("stream: %w", err)
		}
		switch e := ev.(type) {
		case maat.TextEvent:
			fmt.Println(e.Text)
		case maat.ToolCallEvent:
			line := fmt.Sprintf("[tool %s %s", e.Name, e.Status)
			if e.Reason != "" {
				line += ": " + e.Reason
			}
			if e.IsError {
				line += " (error)"
			}
			fmt.Println(line + "]")
		}
	}
	res, err := r.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	if res.Error != nil {
		return fmt.Errorf("run failed: %s: %s", res.Error.Code, res.Error.Message)
	}
	fmt.Printf("\n[%s] input %d tokens, output %d tokens\n", res.StopReason, res.Usage.InputTokens, res.Usage.OutputTokens)
	return nil
}
