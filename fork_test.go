package maat

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

func forkedSession(id string) *maatv1.Session {
	s := protoSession(id, 3)
	s.ForkedFrom = &maatv1.ForkOrigin{SessionId: "ses_src", CheckpointId: "ckp_1", Seq: 42}
	return s
}

func TestFork(t *testing.T) {
	var reqs []*maatv1.ForkSessionRequest
	f := &fakeBackend{fork: func(req *maatv1.ForkSessionRequest) (*maatv1.ForkSessionResponse, error) {
		reqs = append(reqs, req)
		res := &maatv1.ForkSessionResponse{Session: forkedSession("ses_new"), ThreadId: "thr_new", ExecutorStateRef: "git:abc"}
		if req.GetMessage() != nil {
			res.RunId, res.MessageId = "run_1", "msg_1"
		}
		return res, nil
	}}
	c := newTestClient(t, f)
	ctx := context.Background()
	tool := NewTool("echo", "echo", nil, func(context.Context, struct{}) (string, error) { return "", nil })

	res, err := c.Sessions.Fork(ctx, ForkSessionParams{
		CheckpointID: "ckp_1", Message: TextMessage("try again"), Model: "fast", Tools: []Tool{tool},
		Title: "forked", Metadata: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := reqs[0]
	if req.GetCheckpointId() != "ckp_1" || req.GetMessage().GetParts()[0].GetText() != "try again" || req.GetModel() != "fast" ||
		len(req.GetTools()) != 1 || req.GetTitle() != "forked" || req.GetMetadata()["k"] != "v" ||
		!strings.HasPrefix(req.GetIdempotencyKey(), "sdk_") || !strings.HasPrefix(req.GetClientMessageId(), "sdk_") {
		t.Fatalf("request = %v", req)
	}
	s := res.Session
	if s.ID != "ses_new" || s.ForkedFrom == nil || *s.ForkedFrom != (ForkOrigin{SessionID: "ses_src", CheckpointID: "ckp_1", Seq: 42}) ||
		len(s.Tools) != 1 || res.ExecutorStateRef != "git:abc" {
		t.Fatalf("fork = %+v, session = %+v", res, s)
	}
	if r := res.Run; r == nil || r.ID != "run_1" || r.SessionID != "ses_new" || r.ThreadID != "thr_new" || r.MessageID != "msg_1" ||
		r.Delivery != DeliveryNewRun || r.afterSeq != 0 || len(r.tools) != 1 {
		t.Fatalf("run = %+v", res.Run)
	}

	// 不带消息：不生成 client_message_id，没有 Run；工具集继承源会话。
	res, err = c.Sessions.Fork(ctx, ForkSessionParams{CheckpointID: "ckp_1", IdempotencyKey: "idem"})
	if err != nil || res.Run != nil || len(res.Session.Tools) != 0 {
		t.Fatalf("fork without message = %+v, %v", res, err)
	}
	if req := reqs[1]; req.GetMessage() != nil || req.GetClientMessageId() != "" || req.GetIdempotencyKey() != "idem" || len(req.GetTools()) != 0 {
		t.Fatalf("request without message = %v", req)
	}

	// 平台错误原样转换为 MaatError。
	f.fork = func(*maatv1.ForkSessionRequest) (*maatv1.ForkSessionResponse, error) {
		return nil, platformError(connect.CodeFailedPrecondition, "checkpoint_not_stable", "not stable")
	}
	var me *MaatError
	if _, err := c.Sessions.Fork(ctx, ForkSessionParams{CheckpointID: "ckp_2"}); !errors.As(err, &me) || me.Reason != "checkpoint_not_stable" {
		t.Fatalf("unstable checkpoint: %v", err)
	}
}

func TestHistoryOptions(t *testing.T) {
	var got *maatv1.ListSessionEventsRequest
	f := &fakeBackend{listEvents: func(req *maatv1.ListSessionEventsRequest) (*maatv1.ListSessionEventsResponse, error) {
		got = req
		ev := message(1, "run_1", "s1", "a")
		ev.SessionId = "ses_src"
		return &maatv1.ListSessionEventsResponse{Events: []*maatv1.Event{ev}, Page: &maatv1.PageResponse{}}, nil
	}}
	c := newTestClient(t, f)
	s := c.sessionOf(forkedSession("ses_new"))
	var from []string
	for e, err := range s.History(context.Background(), 0, IncludeAncestors(), ExpandRefs(), HistoryTypes("agent.message"), HistoryThreads("thr_1")) {
		if err != nil {
			t.Fatal(err)
		}
		from = append(from, e.GetSessionId())
	}
	if !got.GetIncludeAncestors() || !got.GetExpandRefs() || !reflect.DeepEqual(got.GetTypes(), []string{"agent.message"}) ||
		!reflect.DeepEqual(got.GetThreadIds(), []string{"thr_1"}) || !reflect.DeepEqual(from, []string{"ses_src"}) {
		t.Fatalf("request = %v, sessions = %v", got, from)
	}
	got = nil
	for _, err := range s.History(context.Background(), 5, IncludeAncestors()) {
		if !errors.Is(err, ErrAncestorsWithAfterSeq) {
			t.Fatalf("afterSeq with ancestors: %v", err)
		}
	}
	if got != nil {
		t.Fatal("invalid options must not reach the platform")
	}
}

func TestCheckpoints(t *testing.T) {
	var listReqs []*maatv1.ListCheckpointsRequest
	f := &fakeBackend{
		ckpts: func(req *maatv1.ListCheckpointsRequest) (*maatv1.ListCheckpointsResponse, error) {
			listReqs = append(listReqs, req)
			if req.GetPage().GetPageToken() == "" {
				return &maatv1.ListCheckpointsResponse{
					Checkpoints: []*maatv1.Checkpoint{{Id: "ckp_1", Seq: 3, Kind: maatv1.CheckpointKind_CHECKPOINT_KIND_STEP, Stable: true}},
					Page:        &maatv1.PageResponse{NextPageToken: "next"},
				}, nil
			}
			return &maatv1.ListCheckpointsResponse{Checkpoints: []*maatv1.Checkpoint{{
				Id: "ckp_2", SessionId: "ses_1", ThreadId: "thr_1", RunId: "run_1", Seq: 7, Kind: maatv1.CheckpointKind_CHECKPOINT_KIND_TURN,
				Stable: true, ContextRoot: "sha256:r", ExecutorStateRef: "git:1", Label: "l", CreatedAt: timestamppb.New(time.Unix(5, 0)),
			}}, Page: &maatv1.PageResponse{}}, nil
		},
		annotate: func(req *maatv1.AnnotateCheckpointRequest) (*maatv1.AnnotateCheckpointResponse, error) {
			return &maatv1.AnnotateCheckpointResponse{Checkpoint: &maatv1.Checkpoint{Id: req.GetCheckpointId(), ExecutorStateRef: req.GetExecutorStateRef()}}, nil
		},
	}
	c := newTestClient(t, f)
	ctx := context.Background()
	var all []Checkpoint
	for cp, err := range c.Checkpoints.List(ctx, "ses_1", ListCheckpointsParams{Kind: CheckpointTurn, StableOnly: true, ThreadID: "thr_1"}) {
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, cp)
	}
	want := Checkpoint{
		ID: "ckp_2", SessionID: "ses_1", ThreadID: "thr_1", RunID: "run_1", Seq: 7, Kind: CheckpointTurn, Stable: true,
		ContextRoot: "sha256:r", ExecutorStateRef: "git:1", Label: "l", CreatedAt: time.Unix(5, 0).UTC(),
	}
	if len(all) != 2 || all[0].Kind != CheckpointStep || !reflect.DeepEqual(all[1], want) {
		t.Fatalf("checkpoints = %+v", all)
	}
	if r := listReqs[0]; r.GetSessionId() != "ses_1" || r.GetKind() != maatv1.CheckpointKind_CHECKPOINT_KIND_TURN || !r.GetStableOnly() ||
		r.GetThreadId() != "thr_1" || listReqs[1].GetPage().GetPageToken() != "next" {
		t.Fatalf("list requests = %v", listReqs)
	}

	cp, err := c.Checkpoints.Annotate(ctx, "ckp_2", "git:2")
	if err != nil || cp.ID != "ckp_2" || cp.ExecutorStateRef != "git:2" {
		t.Fatalf("annotate = %+v, %v", cp, err)
	}
	if _, err := c.Checkpoints.Annotate(ctx, "ckp_2", strings.Repeat("x", 1025)); err == nil {
		t.Fatal("oversized executor state ref must be rejected")
	}
}

func TestGetBlob(t *testing.T) {
	f := &fakeBackend{getBlob: func(req *maatv1.GetBlobRequest) (*maatv1.GetBlobResponse, error) {
		if req.GetRef() == "sha256:big" {
			return &maatv1.GetBlobResponse{
				Ref: req.GetRef(), Kind: "tool_result", Size: 2 << 20, ContentType: "application/json",
				Body: &maatv1.GetBlobResponse_DownloadUrl{DownloadUrl: "https://s3/x"}, DownloadUrlExpiresAt: timestamppb.New(time.Unix(9, 0)),
			}, nil
		}
		if req.GetSessionId() != "ses_1" {
			return nil, platformError(connect.CodeNotFound, "blob_not_found", "not found")
		}
		return &maatv1.GetBlobResponse{
			Ref: req.GetRef(), Kind: "message", Size: 2, ContentType: "application/json", Body: &maatv1.GetBlobResponse_Content{Content: []byte("{}")},
		}, nil
	}}
	c := newTestClient(t, f)
	ctx := context.Background()
	s := c.sessionOf(protoSession("ses_1", 0))
	b, err := s.GetBlob(ctx, "sha256:small")
	if err != nil || string(b.Content) != "{}" || b.Kind != "message" || b.DownloadURL != "" || !b.ExpiresAt.IsZero() {
		t.Fatalf("small blob = %+v, %v", b, err)
	}
	b, err = s.GetBlob(ctx, "sha256:big")
	if err != nil || b.Content != nil || b.DownloadURL != "https://s3/x" || !b.ExpiresAt.Equal(time.Unix(9, 0)) || b.Size != 2<<20 {
		t.Fatalf("big blob = %+v, %v", b, err)
	}
	other := c.sessionOf(protoSession("ses_2", 0))
	var me *MaatError
	if _, err := other.GetBlob(ctx, "sha256:small"); !errors.As(err, &me) || me.Code != connect.CodeNotFound {
		t.Fatalf("unreferenced blob: %v", err)
	}
}

func TestExecutorOnFork(t *testing.T) {
	pending := 0
	f := &fakeBackend{
		get: func(req *maatv1.GetSessionRequest) (*maatv1.GetSessionResponse, error) {
			if req.GetSessionId() == "ses_plain" {
				return &maatv1.GetSessionResponse{Session: protoSession("ses_plain", 0)}, nil
			}
			return &maatv1.GetSessionResponse{Session: forkedSession(req.GetSessionId())}, nil
		},
		listEvents: func(req *maatv1.ListSessionEventsRequest) (*maatv1.ListSessionEventsResponse, error) {
			if !reflect.DeepEqual(req.GetTypes(), []string{"session.forked"}) || req.GetPage().GetPageSize() != 1 {
				return nil, platformError(connect.CodeInvalidArgument, "bad_request", "unexpected request")
			}
			return &maatv1.ListSessionEventsResponse{Events: []*maatv1.Event{{Seq: 1, Type: "session.forked",
				Payload: &maatv1.Event_SessionForked{SessionForked: &maatv1.SessionForked{ExecutorStateRef: "git:abc"}}}}}, nil
		},
		pending: func(*maatv1.ListPendingToolCallsRequest) (*maatv1.ListPendingToolCallsResponse, error) {
			pending++
			return &maatv1.ListPendingToolCallsResponse{}, nil
		},
	}
	c := newTestClient(t, f)
	var infos []ForkInfo
	x := c.Executor().OnFork(func(_ context.Context, info ForkInfo) error {
		infos = append(infos, info)
		if info.Session.ID == "ses_bad" {
			return errors.New("checkout failed")
		}
		return nil
	})
	attach := func(id string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		return x.Attach(ctx, id)
	}
	if err := attach("ses_new"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("attach = %v", err)
	}
	if len(infos) != 1 || infos[0].ExecutorStateRef != "git:abc" || infos[0].From.CheckpointID != "ckp_1" ||
		infos[0].Session.ID != "ses_new" || pending != 1 {
		t.Fatalf("fork infos = %+v, pending = %d", infos, pending)
	}
	// 回调失败时 Attach 直接返回，不补拉调用。
	if err := attach("ses_bad"); err == nil || !strings.Contains(err.Error(), "checkout failed") || pending != 1 {
		t.Fatalf("attach with failing callback = %v (pending %d)", err, pending)
	}
	// 不是 fork 出的会话不调用回调。
	if err := attach("ses_plain"); !errors.Is(err, context.DeadlineExceeded) || len(infos) != 2 {
		t.Fatalf("attach plain session = %v, infos = %d", err, len(infos))
	}
}
