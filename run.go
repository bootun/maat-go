package maat

import (
	"context"
	"errors"
	"iter"
	"sync"
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

// Stream 订阅这个 Run 的事件，产出对账后的高层事件（见 Event），直到 RunCompletedEvent 或 RunFailedEvent
// （包含该事件）后结束；断线后自动续传。只产出属于该 Run 的事件：其他 Run 与会话级的事件被跳过，
// 流控制事件只在 WithRawEvents 时以 RawEvent 产出。
//
// 对于插入到运行中 Run 的消息（DeliveryInserted），Stream 从发送前的会话位置开始，
// 此前已产生的事件不会重放。
func (r *Run) Stream(ctx context.Context, opts ...StreamOption) iter.Seq2[Event, error] {
	o := newStreamOptions(opts)
	return func(yield func(Event, error) bool) {
		rec := NewReconciler()
		acc := Result{RunID: r.ID}
		for raw, err := range r.c.subscribe(ctx, subscription{
			sessionID: r.SessionID, afterSeq: r.afterSeq, token: o.resumeToken, includeDeltas: !o.noDeltas,
		}) {
			if err != nil {
				yield(nil, err)
				return
			}
			if raw.GetRunId() != r.ID && !isStreamControl(raw.Event) {
				continue
			}
			if o.raw && !yield(raw, nil) {
				return
			}
			for _, ev := range rec.Apply(raw.Event) {
				done := acc.observe(ev)
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
func (r *Run) Wait(ctx context.Context) (Result, error) {
	if res, ok := r.cachedResult(); ok {
		return res, nil
	}
	for _, err := range r.Stream(ctx, WithoutDeltas()) {
		if err != nil {
			return Result{}, err
		}
	}
	if res, ok := r.cachedResult(); ok {
		return res, nil
	}
	return Result{}, errors.New("maat: event stream ended before the run finished")
}
