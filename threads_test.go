package maat

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// 以下是子线程相关的事件：thread 为事件所属线程。

func threadToolCall(seq uint64, thread, run, id, name string, kind maatv1.ToolCallKind) *maatv1.Event {
	args, err := structpb.NewStruct(map[string]any{})
	if err != nil {
		panic(err)
	}
	return &maatv1.Event{Seq: seq, Type: "agent.tool_call", ThreadId: thread, RunId: run, StepId: "stp_" + id,
		Payload: &maatv1.Event_AgentToolCall{AgentToolCall: &maatv1.AgentToolCall{ToolCall: &maatv1.ToolCall{
			Id: id, RunId: run, ThreadId: thread, StepId: "stp_" + id, Name: name, Kind: kind, Args: args,
			Status: maatv1.ToolCallStatus_TOOL_CALL_STATUS_PENDING,
		}}}}
}

func spawnCall(seq uint64, thread, run, id string) *maatv1.Event {
	return threadToolCall(seq, thread, run, id, spawnAgentTool, maatv1.ToolCallKind_TOOL_CALL_KIND_PLATFORM)
}

func threadCreated(seq uint64, thread, parent, parentCall string, depth uint32) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "thread.created", ThreadId: thread,
		Payload: &maatv1.Event_ThreadCreated{ThreadCreated: &maatv1.ThreadCreated{
			ParentThreadId: parent, ParentToolCallId: parentCall, AgentName: "researcher",
			Mode: maatv1.ThreadMode_THREAD_MODE_FOREGROUND, Depth: depth, Path: []string{"thr_1", thread},
		}}}
}

func threadMessage(seq uint64, thread, run, step, text string) *maatv1.Event {
	e := message(seq, run, step, text)
	e.ThreadId = thread
	return e
}

func threadRunCompleted(seq uint64, thread, run string) *maatv1.Event {
	e := runCompleted(seq, run)
	e.ThreadId = thread
	return e
}

// subthreadCalls 是一个 Run（run_1、thr_1）经 spawn_agent 创建子线程 thr_2、thr_2 再创建孙线程 thr_3 的事件；
// thr_2 与 thr_3 各发起一个 read_file 调用。另有一个不属于该 Run 的线程 thr_9 也发起了 read_file 调用。
func subthreadCalls() []*maatv1.Event {
	client := maatv1.ToolCallKind_TOOL_CALL_KIND_CLIENT
	return []*maatv1.Event{
		spawnCall(1, "thr_1", "run_1", "tc_s1"),
		threadCreated(2, "thr_2", "thr_1", "tc_s1", 1),
		spawnCall(3, "thr_2", "run_2", "tc_s2"),
		threadCreated(4, "thr_3", "thr_2", "tc_s2", 2),
		threadToolCall(5, "thr_2", "run_2", "tc_c2", "read_file", client),
		threadToolCall(6, "thr_3", "run_3", "tc_c3", "read_file", client),
		threadToolCall(7, "thr_9", "run_9", "tc_c9", "read_file", client),
		threadCreated(8, "thr_8", "thr_9", "tc_other", 1),
	}
}

// subthreadEnds 是工具结果回传之后，孙线程、子线程与 run_1 依次结束的事件。
func subthreadEnds() []*maatv1.Event {
	done := func(seq uint64, thread, run, id string) *maatv1.Event {
		e := toolCompleted(seq, run, id)
		e.ThreadId = thread
		return e
	}
	return []*maatv1.Event{
		done(9, "thr_3", "run_3", "tc_c3"),
		threadMessage(10, "thr_3", "run_3", "stp_g", "grandchild done"),
		threadRunCompleted(11, "thr_3", "run_3"),
		done(12, "thr_2", "run_2", "tc_c2"),
		threadMessage(13, "thr_2", "run_2", "stp_c", "child done"),
		threadRunCompleted(14, "thr_2", "run_2"),
		threadMessage(15, "thr_1", "run_1", "stp_p", "parent done"),
		runCompleted(16, "run_1"),
	}
}

// 子线程发起的工具调用无论是否 WithSubthreads 都自动执行；其他线程的调用不执行。子线程的 Run 结束不会结束 Stream。
// 不设置 WithSubthreads 时只产出 Run 自己的事件。
func TestRunStreamExecutesSubthreadTools(t *testing.T) {
	h := newToolHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(readFileTool()))
	h.push(subthreadCalls()...)
	got := map[string]bool{}
	for range 2 {
		got[h.waitSubmit().GetToolCallId()] = true
	}
	if !got["tc_c2"] || !got["tc_c3"] {
		t.Fatalf("submitted = %v", got)
	}
	h.push(subthreadEnds()...)
	evts, err := wait()
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evts {
		if th := eventThread(ev); th != "thr_1" {
			t.Fatalf("event of another thread: %#v", ev)
		}
	}
	if claims, _, _ := h.counts(); claims != 2 {
		t.Fatalf("claims = %d", claims)
	}
	if req := h.f.streams()[0]; req.GetIncludeSubthreadDeltas() {
		t.Fatalf("stream request = %v", req)
	}
}

// WithSubthreads：同时产出子线程与孙线程的事件（包括 ThreadCreatedEvent），订阅子线程的实时文本；
// Run 的结果只取自身的文本。
func TestRunStreamWithSubthreads(t *testing.T) {
	h := newToolHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := h.run(readFileTool())
	wait := h.stream(ctx, r, WithSubthreads())
	h.push(subthreadCalls()...)
	h.waitSubmit()
	h.waitSubmit()
	h.push(subthreadEnds()...)
	evts, err := wait()
	if err != nil {
		t.Fatal(err)
	}
	var (
		created []ThreadCreatedEvent
		texts   []string
		ends    []string
	)
	for _, ev := range evts {
		switch e := ev.(type) {
		case ThreadCreatedEvent:
			created = append(created, e)
		case TextEvent:
			texts = append(texts, e.ThreadID+":"+e.Text)
		case RunCompletedEvent:
			ends = append(ends, e.RunID)
		}
		if th := eventThread(ev); th == "thr_9" || th == "thr_8" {
			t.Fatalf("event of an unrelated thread: %#v", ev)
		}
	}
	wantCreated := []ThreadCreatedEvent{
		{ThreadID: "thr_2", ParentThreadID: "thr_1", ParentToolCallID: "tc_s1", AgentName: "researcher", Mode: ThreadForeground, Depth: 1, Path: []string{"thr_1", "thr_2"}},
		{ThreadID: "thr_3", ParentThreadID: "thr_2", ParentToolCallID: "tc_s2", AgentName: "researcher", Mode: ThreadForeground, Depth: 2, Path: []string{"thr_1", "thr_3"}},
	}
	if !reflect.DeepEqual(created, wantCreated) {
		t.Fatalf("created = %+v", created)
	}
	if !reflect.DeepEqual(texts, []string{"thr_3:grandchild done", "thr_2:child done", "thr_1:parent done"}) {
		t.Fatalf("texts = %q", texts)
	}
	if !reflect.DeepEqual(ends, []string{"run_3", "run_2", "run_1"}) {
		t.Fatalf("run ends = %v", ends)
	}
	res, ok := r.cachedResult()
	if !ok || res.Text != "parent done" || res.RunID != "run_1" {
		t.Fatalf("result = %+v", res)
	}
	if req := h.f.streams()[0]; !req.GetIncludeSubthreadDeltas() || !req.GetIncludeDeltas() {
		t.Fatalf("stream request = %v", req)
	}
}

// eventThread 返回高层事件所属的线程（会话状态变化为空）。
func eventThread(ev Event) string {
	switch e := ev.(type) {
	case TextEvent:
		return e.ThreadID
	case StepRewoundEvent:
		return e.ThreadID
	case ToolCallEvent:
		return e.ThreadID
	case StatusEvent:
		return e.ThreadID
	case RunCompletedEvent:
		return e.ThreadID
	case RunFailedEvent:
		return e.ThreadID
	case ThreadCreatedEvent:
		return e.ThreadID
	}
	return ""
}

func TestSessionThreads(t *testing.T) {
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	var reqs []*maatv1.ListThreadsRequest
	f := &fakeBackend{threads: func(req *maatv1.ListThreadsRequest) (*maatv1.ListThreadsResponse, error) {
		reqs = append(reqs, req)
		if req.GetPage().GetPageToken() == "" {
			return &maatv1.ListThreadsResponse{
				Threads: []*maatv1.Thread{{Id: "thr_1", Path: []string{"thr_1"}, AgentName: "main", Status: maatv1.ThreadStatus_THREAD_STATUS_WAITING}},
				Page:    &maatv1.PageResponse{NextPageToken: "p2"},
			}, nil
		}
		return &maatv1.ListThreadsResponse{Threads: []*maatv1.Thread{{
			Id: "thr_2", ParentThreadId: "thr_1", ParentToolCallId: "tc_s", Path: []string{"thr_1", "thr_2"}, Depth: 1,
			AgentName: "researcher", Mode: maatv1.ThreadMode_THREAD_MODE_BACKGROUND, Status: maatv1.ThreadStatus_THREAD_STATUS_IDLE,
			StopReason: maatv1.StopReason_STOP_REASON_END_TURN, LatestCheckpointId: "ckp_1", StepCount: 2,
			Usage: &maatv1.Usage{InputTokens: 7, OutputTokens: 3}, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now),
		}}, Page: &maatv1.PageResponse{}}, nil
	}}
	c := newTestClient(t, f)
	s := &Session{ID: "ses_1", c: c}
	got, err := s.Threads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Thread{
		{ID: "thr_1", Path: []string{"thr_1"}, AgentName: "main", Status: ThreadWaiting,
			CreatedAt: time.Unix(0, 0).UTC(), UpdatedAt: time.Unix(0, 0).UTC()},
		{ID: "thr_2", ParentThreadID: "thr_1", ParentToolCallID: "tc_s", Path: []string{"thr_1", "thr_2"}, Depth: 1,
			AgentName: "researcher", Mode: ThreadBackground, Status: ThreadIdle, StopReason: StopEndTurn, LatestCheckpointID: "ckp_1",
			StepCount: 2, Usage: Usage{InputTokens: 7, OutputTokens: 3}, CreatedAt: now, UpdatedAt: now},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("threads = %+v", got)
	}
	if len(reqs) != 2 || reqs[0].GetSessionId() != "ses_1" || reqs[1].GetPage().GetPageToken() != "p2" {
		t.Fatalf("requests = %v", reqs)
	}
}

// Send(WithThread) 把消息发给子线程，返回的 Run 属于该线程。
func TestSendToThread(t *testing.T) {
	var got *maatv1.SendMessageRequest
	f := &fakeBackend{send: func(req *maatv1.SendMessageRequest) (*maatv1.SendMessageResponse, error) {
		got = req
		return &maatv1.SendMessageResponse{MessageId: "msg_1", RunId: "run_2", Delivery: maatv1.Delivery_DELIVERY_NEW_RUN}, nil
	}}
	c := newTestClient(t, f)
	s := &Session{ID: "ses_1", PrimaryThreadID: "thr_1", c: c}
	r, err := s.Send(context.Background(), "and Y?", WithThread("thr_2"))
	if err != nil {
		t.Fatal(err)
	}
	if got.GetThreadId() != "thr_2" || r.ThreadID != "thr_2" || r.ID != "run_2" || r.Delivery != DeliveryNewRun {
		t.Fatalf("request = %v, run = %+v", got, r)
	}
}
