package maat

import (
	"strings"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// Reconciler 实现 spec §11.5 的 marker 与回退渲染规则，把原始事件转换为高层事件。
// 它为每个 step 维护 {attempt, buffer, committed}：
//
//   - stream.marker(step, a)：a > attempt 时 attempt = a 并清空 buffer；buffer 原本非空时产出 StepRewoundEvent；
//   - agent.message.delta(step, a)：step 已提交或 a < attempt（旧 attempt 的"僵尸"写入）时忽略；
//     a > attempt 视为隐式 marker；否则追加到 buffer，产出 TextEvent；
//   - agent.message(step)：标记为已提交，产出 Final 的 TextEvent（权威内容）；
//   - stream.reset：丢弃所有未提交的 buffer，对非空的 step 产出 StepRewoundEvent。
//
// 同时把状态类事件转换为 StatusEvent、RunCompletedEvent、RunFailedEvent。
// Run.Stream 与 Session.Stream 内部使用它；自行处理原始事件（例如 History 与实时流拼接）时也可以直接使用。
// Reconciler 不是并发安全的。
type Reconciler struct {
	steps map[string]*stepState
	// order 是 step 首次出现的顺序，使 stream.reset 产出的事件顺序确定。
	order []string
}

type stepState struct {
	threadID  string
	runID     string
	attempt   uint32
	buf       strings.Builder
	committed bool
}

// NewReconciler 构造 Reconciler。
func NewReconciler() *Reconciler { return &Reconciler{steps: map[string]*stepState{}} }

func (r *Reconciler) step(e *maatv1.Event) *stepState {
	id := e.GetStepId()
	st, ok := r.steps[id]
	if !ok {
		st = &stepState{threadID: e.GetThreadId(), runID: e.GetRunId()}
		r.steps[id] = st
		r.order = append(r.order, id)
	}
	return st
}

// Apply 处理一条原始事件，返回由它产生的高层事件（可能为空）。
func (r *Reconciler) Apply(e *maatv1.Event) []Event {
	switch p := e.GetPayload().(type) {
	case *maatv1.Event_StreamMarker:
		st := r.step(e)
		if st.committed {
			return nil
		}
		return r.advance(e, st)
	case *maatv1.Event_AgentMessageDelta:
		st := r.step(e)
		if st.committed || e.GetAttempt() < st.attempt {
			return nil
		}
		out := r.advance(e, st)
		if p.AgentMessageDelta.GetText() == "" {
			return out
		}
		st.buf.WriteString(p.AgentMessageDelta.GetText())
		return append(out, TextEvent{
			ThreadID: st.threadID, RunID: st.runID, StepID: e.GetStepId(), Text: st.buf.String(),
		})
	case *maatv1.Event_AgentMessage:
		st := r.step(e)
		if st.committed {
			return nil
		}
		hadPreview := st.buf.Len() > 0
		st.committed = true
		st.buf.Reset()
		m := p.AgentMessage
		if m.GetText() == "" && !hadPreview {
			return nil // 只有工具调用、没有文本的 step
		}
		return []Event{TextEvent{
			ThreadID: e.GetThreadId(), RunID: e.GetRunId(), StepID: e.GetStepId(), Text: m.GetText(), Final: true,
			Truncated: m.GetTextTruncated(), ContentRef: m.GetContentRef(),
		}}
	case *maatv1.Event_StreamReset:
		var out []Event
		for _, id := range r.order {
			st := r.steps[id]
			if st.committed || st.buf.Len() == 0 {
				continue
			}
			st.buf.Reset()
			out = append(out, StepRewoundEvent{ThreadID: st.threadID, RunID: st.runID, StepID: id})
		}
		return out
	case *maatv1.Event_SessionStatusChanged:
		return []Event{StatusEvent{
			Session:          enumOf[SessionStatus](p.SessionStatusChanged.GetStatus(), "SESSION_STATUS_"),
			PendingToolCalls: p.SessionStatusChanged.GetPendingToolCalls(),
		}}
	case *maatv1.Event_ThreadStatusChanged:
		return []Event{StatusEvent{
			ThreadID: e.GetThreadId(), RunID: e.GetRunId(),
			Thread:     enumOf[ThreadStatus](p.ThreadStatusChanged.GetStatus(), "THREAD_STATUS_"),
			StopReason: enumOf[StopReason](p.ThreadStatusChanged.GetStopReason(), "STOP_REASON_"),
		}}
	case *maatv1.Event_RunCompleted:
		return []Event{RunCompletedEvent{
			ThreadID: e.GetThreadId(), RunID: e.GetRunId(),
			StopReason:       enumOf[StopReason](p.RunCompleted.GetStopReason(), "STOP_REASON_"),
			Usage:            usageOf(p.RunCompleted.GetUsage()),
			FinalTextPreview: p.RunCompleted.GetFinalTextPreview(),
		}}
	case *maatv1.Event_RunFailed:
		return []Event{RunFailedEvent{
			ThreadID: e.GetThreadId(), RunID: e.GetRunId(),
			Error: RunError{Code: p.RunFailed.GetCode(), Message: p.RunFailed.GetMessage()},
		}}
	}
	return nil
}

// advance 把 step 的游标推进到事件的 attempt：buffer 非空时产出 StepRewoundEvent。
func (r *Reconciler) advance(e *maatv1.Event, st *stepState) []Event {
	a := e.GetAttempt()
	if a <= st.attempt {
		return nil
	}
	rewound := st.buf.Len() > 0
	st.attempt = a
	st.buf.Reset()
	if !rewound {
		return nil
	}
	return []Event{StepRewoundEvent{ThreadID: st.threadID, RunID: st.runID, StepID: e.GetStepId(), Attempt: a}}
}
