package maat

import (
	"context"
	"errors"
	"iter"
	"sync"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// Run 是线程从一次触发到重新空闲的一段工作，由 Session.Send 返回。
type Run struct {
	ID        string
	SessionID string
	ThreadID  string
	// MessageID 是触发（或插入）这个 Run 的消息。
	MessageID string
	Delivery  Delivery

	c *Client
	// afterSeq 是订阅的起点：发送消息前会话快照的 LastSeq，Run 的事件都在它之后。
	afterSeq uint64
	// tools 是 Stream 自动执行的工具。
	tools []Tool

	mu     sync.Mutex
	result *Result
}

// Result 是 Run 的结果。
type Result struct {
	RunID string
	// StopReason 是结束原因；Run 失败时为 StopError，Error 有值。
	StopReason StopReason
	// Text 是本 Run 最后一条非空的 assistant 文本（已提交内容）。
	Text string
	// TextTruncated 为真表示 Text 超过 16KB，只是预览，完整内容见 TextRef。
	TextTruncated bool
	TextRef       string
	Usage         Usage
	Error         *RunError
}

// Stream 订阅这个 Run 的事件，产出对账后的高层事件（见 Event），直到该 Run 的 RunCompletedEvent 或
// RunFailedEvent（包含该事件）后结束；断线后自动续传。只产出属于该 Run 的事件：其他 Run 与会话级的事件被跳过，
// 流控制事件只在 WithRawEvents 时以 RawEvent 产出。WithSubthreads 时还产出该 Run 经 spawn_agent 创建的
// 子线程（及其后代）的事件。
//
// 对于插入到运行中 Run 的消息（DeliveryInserted），Stream 从发送前的会话位置开始，
// 此前已产生的事件不会重放。
//
// 迭代期间，Stream 自动执行该 Run 及其子线程中注册了实现的工具调用（Session.Tools 或 WithTools）：认领、执行、
// 续约、回传（WithAutoExecute(false) 关闭）。工具在后台执行，不必等待迭代；迭代结束时（Run 结束或提前退出）
// 取消仍在执行的工具并等待它们返回。Run 结束后仍在运行的后台子线程的工具调用需要由其他执行器
// （例如 Client.Executor）处理。
//
// 子线程按 thread.created 事件识别，因此用 WithResumeToken 从 Run 中途续订时，续传位置之前创建的子线程不会被跟踪。
func (r *Run) Stream(ctx context.Context, opts ...StreamOption) iter.Seq2[Event, error] {
	o := newStreamOptions(opts)
	return func(yield func(Event, error) bool) {
		if len(r.tools) > 0 && !o.noAutoExecute {
			if r.c.err != nil {
				yield(nil, r.c.err)
				return
			}
			ex, err := newExecutor(ctx, r.c, r.tools)
			if err != nil {
				yield(nil, err)
				return
			}
			defer ex.close()
			o.executor = ex
		}
		rec := NewReconciler()
		acc := Result{RunID: r.ID}
		scope := newRunScope(r.ID)
		for raw, err := range r.c.subscribe(ctx, subscription{
			sessionID: r.SessionID, afterSeq: r.afterSeq, token: o.resumeToken, includeDeltas: !o.noDeltas,
			subthreadDeltas: o.subthreads && !o.noDeltas,
		}) {
			if err != nil {
				yield(nil, err)
				return
			}
			own, sub := scope.observe(raw.Event)
			if !own && !sub && !isStreamControl(raw.Event) {
				continue
			}
			if o.executor != nil {
				o.executor.handle(raw.Event)
			}
			if sub && !o.subthreads {
				continue
			}
			if o.raw && !yield(raw, nil) {
				return
			}
			for _, ev := range rec.Apply(raw.Event) {
				done := own && acc.observe(ev)
				if done {
					r.setResult(acc)
				}
				if !yield(ev, nil) || done {
					return
				}
			}
		}
	}
}

// spawnAgentTool 是平台注入的 spawn_agent 工具（spec §7.2）。
const spawnAgentTool = "spawn_agent"

// runScope 识别属于一个 Run 的事件：Run 自己的事件，以及它（递归地）经 spawn_agent 创建的子线程的事件。
type runScope struct {
	runID string
	// spawns 是范围内发起的 spawn_agent 调用。
	spawns map[string]bool
	// threads 是由这些调用创建的子线程。
	threads map[string]bool
}

func newRunScope(runID string) *runScope {
	return &runScope{runID: runID, spawns: map[string]bool{}, threads: map[string]bool{}}
}

// observe 更新范围并返回事件是否属于 Run 自身（own）或它的子线程（sub）。
func (s *runScope) observe(e *maatv1.Event) (own, sub bool) {
	if c := e.GetThreadCreated(); c != nil && s.spawns[c.GetParentToolCallId()] {
		s.threads[e.GetThreadId()] = true
	}
	own = e.GetRunId() == s.runID
	sub = !own && e.GetThreadId() != "" && s.threads[e.GetThreadId()]
	if tc := e.GetAgentToolCall().GetToolCall(); (own || sub) && tc.GetKind() == maatv1.ToolCallKind_TOOL_CALL_KIND_PLATFORM &&
		tc.GetName() == spawnAgentTool {
		s.spawns[tc.GetId()] = true
	}
	return own, sub
}

// observe 用高层事件更新结果，返回 Run 是否已结束。
func (res *Result) observe(ev Event) bool {
	switch e := ev.(type) {
	case TextEvent:
		if e.Final && e.Text != "" {
			res.Text, res.TextTruncated, res.TextRef = e.Text, e.Truncated, e.ContentRef
		}
	case RunCompletedEvent:
		res.StopReason, res.Usage = e.StopReason, e.Usage
		if res.Text == "" {
			res.Text = e.FinalTextPreview
		}
		return true
	case RunFailedEvent:
		res.StopReason, res.Error = StopError, &e.Error
		return true
	}
	return false
}

func (r *Run) setResult(res Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.result = &res
}

func (r *Run) cachedResult() (Result, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.result == nil {
		return Result{}, false
	}
	return *r.result, true
}

// Wait 等待 Run 结束并返回结果。Run 失败（run.failed）不算调用错误：err 为 nil，Result.Error 有值；
// err 只表示订阅本身失败（ctx 结束、认证失败等）。Stream 已经读到结束时直接返回缓存的结果。
// 与 Stream 一样默认自动执行工具调用，可以传入 WithAutoExecute(false)。
func (r *Run) Wait(ctx context.Context, opts ...StreamOption) (Result, error) {
	if res, ok := r.cachedResult(); ok {
		return res, nil
	}
	for _, err := range r.Stream(ctx, append([]StreamOption{WithoutDeltas()}, opts...)...) {
		if err != nil {
			return Result{}, err
		}
	}
	if res, ok := r.cachedResult(); ok {
		return res, nil
	}
	return Result{}, errors.New("maat: event stream ended before the run finished")
}
