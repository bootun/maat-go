package maat

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

func protoSession(id string, lastSeq uint64) *maatv1.Session {
	return &maatv1.Session{
		Id: id, AgentId: "agt_1", AgentVersion: 2, Model: "main", Status: maatv1.SessionStatus_SESSION_STATUS_IDLE,
		Lifecycle: maatv1.Lifecycle_LIFECYCLE_ACTIVE, Title: "t", Metadata: map[string]string{"ticket": "ENG-42"},
		PrimaryThreadId: "thr_1", LastSeq: lastSeq, CreatedAt: timestamppb.New(time.Unix(100, 0)),
	}
}

func TestCreateSessionIsIdempotentAcrossRetries(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	f := &fakeBackend{create: func(req *maatv1.CreateSessionRequest) (*maatv1.CreateSessionResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		keys = append(keys, req.GetIdempotencyKey())
		if req.GetAgentId() != "agt_1" || req.GetModel() != "main" || req.GetMetadata()["ticket"] != "ENG-42" {
			return nil, platformError(connect.CodeInvalidArgument, "bad_request", "unexpected request")
		}
		if len(keys) == 1 {
			return nil, platformError(connect.CodeUnavailable, "unavailable", "try later")
		}
		return &maatv1.CreateSessionResponse{Session: protoSession("ses_1", 1), ThreadId: "thr_1"}, nil
	}}
	c := newTestClient(t, f)
	s, err := c.Sessions.Create(context.Background(), CreateSessionParams{
		Agent: "agt_1", Model: "main", Metadata: map[string]string{"ticket": "ENG-42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("idempotency keys = %v", keys)
	}
	if s.ID != "ses_1" || s.Status != SessionIdle || s.PrimaryThreadID != "thr_1" || s.AgentVersion != 2 ||
		s.CreatedAt.Unix() != 100 || s.Archived {
		t.Fatalf("session = %+v", s)
	}
}

func TestSessionsList(t *testing.T) {
	f := &fakeBackend{list: func(req *maatv1.ListSessionsRequest) (*maatv1.ListSessionsResponse, error) {
		if got := req.GetFilter().GetStatuses(); len(got) != 1 || got[0] != maatv1.SessionStatus_SESSION_STATUS_RUNNING {
			return nil, platformError(connect.CodeInvalidArgument, "bad_filter", "unexpected filter")
		}
		if req.GetPage().GetPageToken() == "" {
			return &maatv1.ListSessionsResponse{Sessions: []*maatv1.Session{protoSession("ses_1", 1), protoSession("ses_2", 1)},
				Page: &maatv1.PageResponse{NextPageToken: "p2"}}, nil
		}
		return &maatv1.ListSessionsResponse{Sessions: []*maatv1.Session{protoSession("ses_3", 1)}, Page: &maatv1.PageResponse{}}, nil
	}}
	c := newTestClient(t, f)
	var ids []string
	for s, err := range c.Sessions.List(context.Background(), ListSessionsParams{Statuses: []SessionStatus{SessionRunning}}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, s.ID)
	}
	if !reflect.DeepEqual(ids, []string{"ses_1", "ses_2", "ses_3"}) {
		t.Fatalf("ids = %v", ids)
	}
}

// runBackend 模拟一个 Run：会话快照的 LastSeq 为 5，事件流中混有其他 Run 的事件。
func runBackend(t *testing.T, final *maatv1.Event) (*fakeBackend, func() []*maatv1.SendMessageRequest) {
	t.Helper()
	var mu sync.Mutex
	sends := &[]*maatv1.SendMessageRequest{}
	recorded := func() []*maatv1.SendMessageRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]*maatv1.SendMessageRequest(nil), *sends...)
	}
	f := &fakeBackend{
		get: func(*maatv1.GetSessionRequest) (*maatv1.GetSessionResponse, error) {
			return &maatv1.GetSessionResponse{Session: protoSession("ses_1", 5)}, nil
		},
		send: func(req *maatv1.SendMessageRequest) (*maatv1.SendMessageResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			*sends = append(*sends, req)
			if len(*sends) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("try later"))
			}
			return &maatv1.SendMessageResponse{MessageId: "msg_1", RunId: "run_1", Delivery: maatv1.Delivery_DELIVERY_NEW_RUN}, nil
		},
	}
	f.conns = append(f.conns, func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
		evts := []*maatv1.Event{
			delta("run_0", "s0", 1, "other run"),
			threadStatus(6, "run_1", maatv1.ThreadStatus_THREAD_STATUS_RUNNING, 0),
			marker("run_1", "s1", 1), delta("run_1", "s1", 1, "Hel"), heartbeat("t", false), delta("run_1", "s1", 1, "lo"),
			message(7, "run_1", "s1", "Hello"),
			message(8, "run_0", "s0", "other run"),
			final,
			message(10, "run_2", "s9", "after the run"),
		}
		for _, e := range evts {
			if err := send(st, "t", e); err != nil {
				return err
			}
		}
		return nil
	})
	return f, recorded
}

func TestRunStreamAndWait(t *testing.T) {
	f, sends := runBackend(t, runCompleted(9, "run_1"))
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := c.Sessions.Get(ctx, "ses_1")
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "hi", WithModel("fast"))
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != "run_1" || run.MessageID != "msg_1" || run.ThreadID != "thr_1" || run.Delivery != DeliveryNewRun {
		t.Fatalf("run = %+v", run)
	}
	// 内部重试使用同一个 client_message_id。
	if r := sends(); len(r) != 2 || r[0].GetClientMessageId() == "" || r[0].GetClientMessageId() != r[1].GetClientMessageId() ||
		r[1].GetModel() != "fast" || r[1].GetMessage().GetParts()[0].GetText() != "hi" {
		t.Fatalf("send requests = %v", r)
	}

	var got []Event
	for ev, err := range run.Stream(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ev)
	}
	want := []Event{
		StatusEvent{ThreadID: "thr_1", RunID: "run_1", Thread: ThreadRunning},
		TextEvent{RunID: "run_1", StepID: "s1", Text: "Hel"},
		TextEvent{RunID: "run_1", StepID: "s1", Text: "Hello"},
		TextEvent{RunID: "run_1", StepID: "s1", Text: "Hello", Final: true},
		RunCompletedEvent{ThreadID: "thr_1", RunID: "run_1", StopReason: StopEndTurn, Usage: Usage{InputTokens: 10, OutputTokens: 5}, FinalTextPreview: "preview"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events:\n got  %#v\n want %#v", got, want)
	}
	if reqs := f.streams(); len(reqs) != 1 || reqs[0].GetAfterSeq() != 5 || reqs[0].GetSessionId() != "ses_1" {
		t.Fatalf("stream requests = %v", reqs)
	}

	// Stream 已经读到结束：Wait 直接返回缓存的结果，不再订阅。
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.RunID != "run_1" || res.StopReason != StopEndTurn || res.Text != "Hello" || res.Usage.OutputTokens != 5 || res.Error != nil {
		t.Fatalf("result = %+v", res)
	}
	if n := len(f.streams()); n != 1 {
		t.Fatalf("stream requests = %d", n)
	}
}

func TestRunWaitFailed(t *testing.T) {
	f, _ := runBackend(t, runFailed(9, "run_1", "upstream_error", "upstream returned 500"))
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := c.Sessions.Get(ctx, "ses_1")
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "hi")
	if err != nil {
		t.Fatal(err)
	}
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.StopReason != StopError || res.Error == nil || res.Error.Code != "upstream_error" || res.Text != "Hello" {
		t.Fatalf("result = %+v", res)
	}
	// Wait 不需要瞬时事件。
	if reqs := f.streams(); len(reqs) != 1 || reqs[0].GetIncludeDeltas() {
		t.Fatalf("stream requests = %v", reqs)
	}
}

func TestRunStreamRawEvents(t *testing.T) {
	f, _ := runBackend(t, runCompleted(9, "run_1"))
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run := &Run{ID: "run_1", SessionID: "ses_1", c: c, afterSeq: 5}
	var kinds []string
	for ev, err := range run.Stream(ctx, WithRawEvents()) {
		if err != nil {
			t.Fatal(err)
		}
		if raw, ok := ev.(RawEvent); ok {
			kinds = append(kinds, "raw:"+raw.GetType())
			continue
		}
		kinds = append(kinds, reflect.TypeOf(ev).Name())
	}
	want := []string{
		"raw:thread.status_changed", "StatusEvent",
		"raw:stream.marker",
		"raw:agent.message.delta", "TextEvent",
		"raw:stream.heartbeat",
		"raw:agent.message.delta", "TextEvent",
		"raw:agent.message", "TextEvent",
		"raw:run.completed", "RunCompletedEvent",
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("kinds:\n got  %v\n want %v", kinds, want)
	}
}

func TestSessionHistoryAndStream(t *testing.T) {
	f := &fakeBackend{listEvents: func(req *maatv1.ListSessionEventsRequest) (*maatv1.ListSessionEventsResponse, error) {
		if req.GetAfterSeq() != 2 || req.GetPage().GetPageSize() != historyPageSize {
			return nil, platformError(connect.CodeInvalidArgument, "bad_request", "unexpected request")
		}
		if req.GetPage().GetPageToken() == "" {
			return &maatv1.ListSessionEventsResponse{Events: []*maatv1.Event{message(3, "run_1", "s1", "a")},
				Page: &maatv1.PageResponse{NextPageToken: "3"}}, nil
		}
		return &maatv1.ListSessionEventsResponse{Events: []*maatv1.Event{runCompleted(4, "run_1")}, Page: &maatv1.PageResponse{}}, nil
	}}
	f.conns = append(f.conns, func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
		for _, e := range []*maatv1.Event{delta("run_2", "s2", 1, "x"), message(5, "run_2", "s2", "xy")} {
			if err := send(st, "t", e); err != nil {
				return err
			}
		}
		return nil
	})
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := c.sessionOf(protoSession("ses_1", 2))

	var seqs []uint64
	for e, err := range s.History(ctx, 2) {
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, e.GetSeq())
	}
	if !reflect.DeepEqual(seqs, []uint64{3, 4}) {
		t.Fatalf("history seqs = %v", seqs)
	}

	// 用历史末尾的 seq 订阅实时流。Session.Stream 不会自行结束，这里读到提交后停止。
	var got []Event
	for ev, err := range s.Stream(ctx, AfterSeq(4)) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ev)
		if te, ok := ev.(TextEvent); ok && te.Final {
			break
		}
	}
	want := []Event{TextEvent{RunID: "run_2", StepID: "s2", Text: "x"}, TextEvent{RunID: "run_2", StepID: "s2", Text: "xy", Final: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v", got)
	}
	if reqs := f.streams(); len(reqs) != 1 || reqs[0].GetAfterSeq() != 4 || !reqs[0].GetIncludeDeltas() {
		t.Fatalf("stream requests = %v", reqs)
	}
}

func TestSendInputWithJSON(t *testing.T) {
	reqs := make(chan *maatv1.SendMessageRequest, 1)
	f := &fakeBackend{send: func(req *maatv1.SendMessageRequest) (*maatv1.SendMessageResponse, error) {
		reqs <- req
		return &maatv1.SendMessageResponse{RunId: "run_1", Delivery: maatv1.Delivery_DELIVERY_INSERTED}, nil
	}}
	c := newTestClient(t, f)
	s := c.sessionOf(protoSession("ses_1", 1))
	run, err := s.SendInput(context.Background(), MessageInput{Parts: []ContentPart{
		{Text: "看看这个"}, {JSON: map[string]any{"path": "README.md", "lines": []any{1.0, 2.0}}},
	}}, WithThread("thr_2"), WithClientMessageID("cm_1"))
	if err != nil {
		t.Fatal(err)
	}
	got := <-reqs
	parts := got.GetMessage().GetParts()
	if len(parts) != 2 || parts[0].GetText() != "看看这个" || parts[1].GetJson().GetFields()["path"].GetStringValue() != "README.md" ||
		got.GetThreadId() != "thr_2" || got.GetClientMessageId() != "cm_1" {
		t.Fatalf("request = %v", got)
	}
	if run.ThreadID != "thr_2" || run.Delivery != DeliveryInserted {
		t.Fatalf("run = %+v", run)
	}

	// 不能表示为 JSON 的值在发送前报错。
	if _, err := s.SendInput(context.Background(), MessageInput{Parts: []ContentPart{{JSON: map[string]any{"f": func() {}}}}}); err == nil {
		t.Fatal("expected an error for a non-JSON value")
	}
}
