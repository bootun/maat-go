package maat

import (
	"reflect"
	"testing"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// 对账器的 fixture（spec §11.5）：正常、回退、僵尸 delta、reset、乱序的已提交事件。
func TestReconciler(t *testing.T) {
	const r = "run_1"
	text := func(step, s string) Event { return TextEvent{RunID: r, StepID: step, Text: s} }
	final := func(step, s string) Event { return TextEvent{RunID: r, StepID: step, Text: s, Final: true} }
	rewound := func(step string, a uint32) Event { return StepRewoundEvent{RunID: r, StepID: step, Attempt: a} }

	tests := []struct {
		name string
		in   []*maatv1.Event
		want []Event
	}{
		{
			name: "正常：delta 累积为完整文本，提交后给出权威内容",
			in: []*maatv1.Event{
				marker(r, "s1", 1), delta(r, "s1", 1, "Hel"), delta(r, "s1", 1, "lo"), message(1, r, "s1", "Hello"),
			},
			want: []Event{text("s1", "Hel"), text("s1", "Hello"), final("s1", "Hello")},
		},
		{
			name: "回退：更大的 attempt 清空预览并通知重新渲染",
			in: []*maatv1.Event{
				marker(r, "s1", 1), delta(r, "s1", 1, "abc"), marker(r, "s1", 2), delta(r, "s1", 2, "xy"),
				message(1, r, "s1", "xyz"),
			},
			want: []Event{text("s1", "abc"), rewound("s1", 2), text("s1", "xy"), final("s1", "xyz")},
		},
		{
			name: "第一次 marker 与重复的 marker 不产生回退",
			in:   []*maatv1.Event{marker(r, "s1", 1), marker(r, "s1", 1), delta(r, "s1", 1, "a")},
			want: []Event{text("s1", "a")},
		},
		{
			name: "僵尸 delta：旧 attempt 的写入被忽略",
			in: []*maatv1.Event{
				marker(r, "s1", 1), delta(r, "s1", 1, "a"), marker(r, "s1", 2), delta(r, "s1", 1, "zombie"),
				delta(r, "s1", 2, "b"),
			},
			want: []Event{text("s1", "a"), rewound("s1", 2), text("s1", "b")},
		},
		{
			name: "隐式 marker：delta 的 attempt 更大",
			in:   []*maatv1.Event{delta(r, "s1", 1, "a"), delta(r, "s1", 3, "b")},
			want: []Event{text("s1", "a"), rewound("s1", 3), text("s1", "b")},
		},
		{
			name: "reset：丢弃所有未提交的预览，已提交的不受影响",
			in: []*maatv1.Event{
				delta(r, "s1", 1, "one"), message(1, r, "s1", "one"), delta(r, "s2", 1, "two"), marker(r, "s3", 1),
				delta(r, "s4", 1, "four"), reset(), delta(r, "s2", 1, "-more"),
			},
			want: []Event{
				text("s1", "one"), final("s1", "one"), text("s2", "two"), text("s4", "four"),
				StepRewoundEvent{RunID: r, StepID: "s2"}, StepRewoundEvent{RunID: r, StepID: "s4"}, text("s2", "-more"),
			},
		},
		{
			name: "乱序：已提交事件先到，之后的 delta 与 marker 都被忽略",
			in: []*maatv1.Event{
				message(1, r, "s1", "Final"), delta(r, "s1", 1, "Fi"), marker(r, "s1", 2), delta(r, "s1", 2, "x"),
				message(1, r, "s1", "Final"),
			},
			want: []Event{final("s1", "Final")},
		},
		{
			name: "只有工具调用的 step 不产生文本事件；预览被清空时仍给出空的权威内容",
			in:   []*maatv1.Event{message(1, r, "s1", ""), delta(r, "s2", 1, "x"), message(2, r, "s2", "")},
			want: []Event{text("s2", "x"), final("s2", "")},
		},
		{
			name: "状态与 Run 事件",
			in: []*maatv1.Event{
				{Seq: 1, Type: "session.status_changed", Payload: &maatv1.Event_SessionStatusChanged{SessionStatusChanged: &maatv1.SessionStatusChanged{
					Status: maatv1.SessionStatus_SESSION_STATUS_REQUIRES_ACTION, PendingToolCalls: 2,
				}}},
				threadStatus(2, r, maatv1.ThreadStatus_THREAD_STATUS_IDLE, maatv1.StopReason_STOP_REASON_MAX_STEPS),
				runCompleted(3, r),
				runFailed(4, "run_2", "upstream_error", "boom"),
			},
			want: []Event{
				StatusEvent{Session: SessionRequiresAction, PendingToolCalls: 2},
				StatusEvent{ThreadID: "thr_1", RunID: r, Thread: ThreadIdle, StopReason: StopMaxSteps},
				RunCompletedEvent{ThreadID: "thr_1", RunID: r, StopReason: StopEndTurn,
					Usage: Usage{InputTokens: 10, OutputTokens: 5}, FinalTextPreview: "preview"},
				RunFailedEvent{ThreadID: "thr_1", RunID: "run_2", Error: RunError{Code: "upstream_error", Message: "boom"}},
			},
		},
		{
			name: "截断的已提交文本带上 ref",
			in: []*maatv1.Event{{Seq: 1, Type: "agent.message", RunId: r, StepId: "s1",
				Payload: &maatv1.Event_AgentMessage{AgentMessage: &maatv1.AgentMessage{Text: "pre", TextTruncated: true, ContentRef: "sha256:ab"}}}},
			want: []Event{TextEvent{RunID: r, StepID: "s1", Text: "pre", Final: true, Truncated: true, ContentRef: "sha256:ab"}},
		},
		{
			name: "心跳与 lagged 不产生高层事件",
			in:   []*maatv1.Event{heartbeat("t", false), {Type: "stream.lagged", Payload: &maatv1.Event_StreamLagged{StreamLagged: &maatv1.StreamLagged{DroppedCount: 3}}}},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := NewReconciler()
			var got []Event
			for _, e := range tt.in {
				got = append(got, rec.Apply(e)...)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("events:\n got  %#v\n want %#v", got, tt.want)
			}
		})
	}
}

// 工具调用事件转换为 ToolCallEvent；后续事件不带工具名，由对账器按调用 ID 补上。
func TestReconcilerToolCalls(t *testing.T) {
	const r = "run_1"
	ev := func(seq uint64, e *maatv1.Event) *maatv1.Event {
		e.Seq, e.ThreadId, e.RunId, e.StepId = seq, "thr_1", r, "stp_1"
		return e
	}
	in := []*maatv1.Event{
		toolCall(1, r, "tc_1", "read_file", map[string]any{"path": "a"}, 0),
		ev(2, &maatv1.Event{Type: "tool_call.claimed", Payload: &maatv1.Event_ToolCallClaimed{ToolCallClaimed: &maatv1.ToolCallClaimed{ToolCallId: "tc_1", ExecutorId: "exe_1"}}}),
		ev(3, &maatv1.Event{Type: "tool_call.reopened", Payload: &maatv1.Event_ToolCallReopened{ToolCallReopened: &maatv1.ToolCallReopened{ToolCallId: "tc_1", DispatchAttempt: 1}}}),
		ev(4, &maatv1.Event{Type: "tool_call.failed", Payload: &maatv1.Event_ToolCallFailed{ToolCallFailed: &maatv1.ToolCallFailed{ToolCallId: "tc_1", Reason: "executor_lost", Message: "gone"}}}),
		ev(5, &maatv1.Event{Type: "tool_call.completed", Payload: &maatv1.Event_ToolCallCompleted{ToolCallCompleted: &maatv1.ToolCallCompleted{ToolCallId: "tc_2", IsError: true}}}),
		ev(6, &maatv1.Event{Type: "tool_call.cancelled", Payload: &maatv1.Event_ToolCallCancelled{ToolCallCancelled: &maatv1.ToolCallCancelled{ToolCallId: "tc_1", Reason: "interrupted"}}}),
		ev(7, &maatv1.Event{Type: "tool_calls.resolved", Payload: &maatv1.Event_ToolCallsResolved{ToolCallsResolved: &maatv1.ToolCallsResolved{StepId: "stp_1"}}}),
		ev(8, &maatv1.Event{Type: "agent.tool_call", Payload: &maatv1.Event_AgentToolCall{AgentToolCall: &maatv1.AgentToolCall{ToolCall: &maatv1.ToolCall{
			Id: "tc_3", Name: "big", ArgsRef: "sha256:args"}}}}),
	}
	base := ToolCallEvent{ThreadID: "thr_1", RunID: r, StepID: "stp_1", ToolCallID: "tc_1", Name: "read_file"}
	with := func(f func(*ToolCallEvent)) Event {
		e := base
		f(&e)
		return e
	}
	want := []Event{
		with(func(e *ToolCallEvent) { e.Status, e.Args = ToolCallPending, map[string]any{"path": "a"} }),
		with(func(e *ToolCallEvent) { e.Status, e.ExecutorID = ToolCallClaimed, "exe_1" }),
		with(func(e *ToolCallEvent) { e.Status, e.DispatchAttempt = ToolCallPending, 1 }),
		with(func(e *ToolCallEvent) { e.Status, e.Reason, e.Message = ToolCallFailed, "executor_lost", "gone" }),
		with(func(e *ToolCallEvent) {
			e.ToolCallID, e.Name, e.Status, e.IsError = "tc_2", "", ToolCallCompleted, true
		}),
		with(func(e *ToolCallEvent) { e.Status, e.Reason = ToolCallCancelled, "interrupted" }),
		with(func(e *ToolCallEvent) {
			e.ToolCallID, e.Name, e.Status, e.ArgsRef = "tc_3", "big", ToolCallPending, "sha256:args"
		}),
	}
	rec := NewReconciler()
	var got []Event
	for _, e := range in {
		got = append(got, rec.Apply(e)...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events:\n got  %#v\n want %#v", got, want)
	}
}

func TestEnumOf(t *testing.T) {
	if got := enumOf[SessionStatus](maatv1.SessionStatus_SESSION_STATUS_UNSPECIFIED, "SESSION_STATUS_"); got != "" {
		t.Fatalf("unspecified = %q", got)
	}
	if got := enumOf[Delivery](maatv1.Delivery_DELIVERY_QUEUED_AFTER_TOOL_RESULTS, "DELIVERY_"); got != DeliveryQueuedAfterToolResults {
		t.Fatalf("delivery = %q", got)
	}
	// 平台新增、SDK 尚不认识的取值按数字原样保留，不会被误判为已知状态。
	if got := enumOf[ThreadStatus](maatv1.ThreadStatus(99), "THREAD_STATUS_"); got != "99" {
		t.Fatalf("unknown = %q", got)
	}
}
