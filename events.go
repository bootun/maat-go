package maat

import (
	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// SessionStatus 是会话状态（全部线程状态的聚合）。
type SessionStatus string

// 会话状态。
const (
	SessionIdle           SessionStatus = "idle"
	SessionRunning        SessionStatus = "running"
	SessionRequiresAction SessionStatus = "requires_action"
)

// ThreadStatus 是线程状态。
type ThreadStatus string

// 线程状态。
const (
	ThreadIdle           ThreadStatus = "idle"
	ThreadRunning        ThreadStatus = "running"
	ThreadRequiresAction ThreadStatus = "requires_action"
	ThreadWaiting        ThreadStatus = "waiting"
)

// StopReason 是 Run 结束的原因。
type StopReason string

// Run 结束的原因。
const (
	StopEndTurn     StopReason = "end_turn"
	StopInterrupted StopReason = "interrupted"
	StopMaxSteps    StopReason = "max_steps"
	StopError       StopReason = "error"
	// StopCancelled 表示会话被归档或删除。
	StopCancelled StopReason = "cancelled"
)

// Delivery 是消息被接收时的投递方式。
type Delivery string

// 投递方式。
const (
	// DeliveryNewRun：线程空闲，消息开启了新 Run。
	DeliveryNewRun Delivery = "new_run"
	// DeliveryInserted：线程运行中，消息并入当前 Run，在下一个 step 开始前投递。
	DeliveryInserted Delivery = "inserted"
	// DeliveryQueuedAfterToolResults：当前 Run 在等待工具结果，消息在全部 tool 消息之后投递。
	DeliveryQueuedAfterToolResults Delivery = "queued_after_tool_results"
)

// Usage 是模型调用的 token 用量。
type Usage struct {
	InputTokens       uint64
	OutputTokens      uint64
	CachedInputTokens uint64
	ReasoningTokens   uint64
}

func usageOf(u *maatv1.Usage) Usage {
	return Usage{
		InputTokens: u.GetInputTokens(), OutputTokens: u.GetOutputTokens(),
		CachedInputTokens: u.GetCachedInputTokens(), ReasoningTokens: u.GetReasoningTokens(),
	}
}

// Event 是 Stream 产出的事件，具体类型为：
//   - TextEvent、StepRewoundEvent：按 spec §11.5 对账后的文本视图；
//   - ToolCallEvent：工具调用的状态变化；
//   - StatusEvent、RunCompletedEvent、RunFailedEvent：状态变化；
//   - RawEvent：原始事件（需要 WithRawEvents）。
type Event interface{ isEvent() }

// TextEvent 表示某个 step 的 assistant 文本有更新。
type TextEvent struct {
	ThreadID string
	RunID    string
	StepID   string
	// Text 是该 step 到目前为止的完整文本（不是增量），直接替换已渲染的内容即可。
	Text string
	// Final 为真表示 Text 是已提交的权威内容，之后不会再变化。
	Final bool
	// Truncated 为真表示已提交的文本超过 16KB，Text 只是预览，完整内容见 ContentRef。
	Truncated  bool
	ContentRef string
}

// StepRewoundEvent 表示某个 step 未提交的预览作废（step 发生重试，或收到 stream.reset），
// 应清空已渲染的内容，之后的 TextEvent 会重新给出文本。
type StepRewoundEvent struct {
	ThreadID string
	RunID    string
	StepID   string
	// Attempt 是新的 attempt；因 stream.reset 作废时为 0。
	Attempt uint32
}

// ToolCallStatus 是工具调用的状态。
type ToolCallStatus string

// 工具调用的状态。
const (
	ToolCallPending   ToolCallStatus = "pending"
	ToolCallClaimed   ToolCallStatus = "claimed"
	ToolCallRunning   ToolCallStatus = "running"
	ToolCallCompleted ToolCallStatus = "completed"
	ToolCallFailed    ToolCallStatus = "failed"
	ToolCallCancelled ToolCallStatus = "cancelled"
)

// ToolCallEvent 表示工具调用的状态变化：模型发起调用（pending）、被认领（claimed）、回传结果（completed）、
// 失败（failed，例如 executor_lost、timeout、unknown_tool）、被取消（cancelled，例如中断），
// 以及执行器掉线后重新开放（pending，DispatchAttempt 增加）。
type ToolCallEvent struct {
	ThreadID   string
	RunID      string
	StepID     string
	ToolCallID string
	// Name 是工具名；订阅从调用发起之后开始时可能为空。
	Name   string
	Status ToolCallStatus
	// Args 是模型给出的参数（pending 且参数不超过 64KB 时有值）；较大的参数只有 ArgsRef。
	Args    map[string]any
	ArgsRef string
	// ExecutorID 是认领者（claimed 时有值）。
	ExecutorID      string
	DispatchAttempt uint32
	// IsError 表示回传的是错误结果（completed 时有值）。
	IsError bool
	// Reason 与 Message 是失败或取消的原因（failed、cancelled 时有值）。
	Reason  string
	Message string
}

// StatusEvent 表示会话或线程的状态变化。
type StatusEvent struct {
	// ThreadID 为空表示会话状态变化（Session 有值），否则为该线程的状态变化（Thread 有值）。
	ThreadID string
	RunID    string
	Session  SessionStatus
	Thread   ThreadStatus
	// StopReason 是线程变为空闲的原因。
	StopReason StopReason
	// PendingToolCalls 是会话中等待结果的工具调用数（会话状态变化时有值）。
	PendingToolCalls uint32
}

// RunCompletedEvent 表示 Run 结束（包括被中断、达到步数上限）。
type RunCompletedEvent struct {
	ThreadID   string
	RunID      string
	StopReason StopReason
	Usage      Usage
	// FinalTextPreview 是最后一条 assistant 文本的前 512 个字符。
	FinalTextPreview string
}

// RunFailedEvent 表示 Run 因错误结束。
type RunFailedEvent struct {
	ThreadID string
	RunID    string
	Error    RunError
}

// RunError 是 Run 失败的原因（上游错误已脱敏）。
type RunError struct {
	Code    string
	Message string
}

// RawEvent 是平台推送的原始事件，字段与方法来自 maatv1.Event（例如 GetType、GetSeq、GetAgentMessage）。
// 已提交事件的 Seq 在会话内严格递增；瞬时事件（delta、marker、心跳等）的 Seq 为 0。
type RawEvent struct {
	*maatv1.Event
	// ResumeToken 是处理完这条事件后的续传位置，可以传给 WithResumeToken。History 返回的事件没有该值。
	ResumeToken string
}

func (TextEvent) isEvent()         {}
func (StepRewoundEvent) isEvent()  {}
func (ToolCallEvent) isEvent()     {}
func (StatusEvent) isEvent()       {}
func (RunCompletedEvent) isEvent() {}
func (RunFailedEvent) isEvent()    {}
func (RawEvent) isEvent()          {}

// isStreamControl 判断事件是否为流控制事件（心跳、reset、lagged），它们不属于任何 Run。
func isStreamControl(e *maatv1.Event) bool {
	return e.GetStreamHeartbeat() != nil || e.GetStreamReset() != nil || e.GetStreamLagged() != nil
}
