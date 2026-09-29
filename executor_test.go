package maat

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// toolHarness 是执行器测试的假平台：事件流由测试逐条推送；工具 RPC 被记录，行为可以替换。
type toolHarness struct {
	t      *testing.T
	c      *Client
	f      *fakeBackend
	events chan *maatv1.Event

	// 以下函数在启动前设置；为 nil 时使用默认行为（认领成功、续约成功、回传成功）。
	claimFn  func(n int, req *maatv1.ClaimToolCallRequest) (*maatv1.ClaimToolCallResponse, error)
	renewFn  func(n int, req *maatv1.RenewToolCallLeaseRequest) (*maatv1.RenewToolCallLeaseResponse, error)
	submitFn func(n int, req *maatv1.SubmitToolResultRequest) (*maatv1.SubmitToolResultResponse, error)

	mu        sync.Mutex
	claims    []*maatv1.ClaimToolCallRequest
	renews    []*maatv1.RenewToolCallLeaseRequest
	submits   []*maatv1.SubmitToolResultRequest
	submitted chan *maatv1.SubmitToolResultRequest
}

func newToolHarness(t *testing.T) *toolHarness {
	t.Helper()
	h := &toolHarness{t: t, events: make(chan *maatv1.Event, 64), submitted: make(chan *maatv1.SubmitToolResultRequest, 64)}
	h.f = &fakeBackend{
		claim: func(req *maatv1.ClaimToolCallRequest) (*maatv1.ClaimToolCallResponse, error) {
			h.mu.Lock()
			h.claims = append(h.claims, req)
			n := len(h.claims)
			h.mu.Unlock()
			if h.claimFn != nil {
				return h.claimFn(n, req)
			}
			return claimOK(req), nil
		},
		renew: func(req *maatv1.RenewToolCallLeaseRequest) (*maatv1.RenewToolCallLeaseResponse, error) {
			h.mu.Lock()
			h.renews = append(h.renews, req)
			n := len(h.renews)
			h.mu.Unlock()
			if h.renewFn != nil {
				return h.renewFn(n, req)
			}
			return &maatv1.RenewToolCallLeaseResponse{}, nil
		},
		submit: func(req *maatv1.SubmitToolResultRequest) (*maatv1.SubmitToolResultResponse, error) {
			h.mu.Lock()
			h.submits = append(h.submits, req)
			n := len(h.submits)
			h.mu.Unlock()
			var err error
			if h.submitFn != nil {
				_, err = h.submitFn(n, req)
			}
			if err == nil {
				h.submitted <- req
			}
			return &maatv1.SubmitToolResultResponse{}, err
		},
	}
	h.f.conns = append(h.f.conns, func(ctx context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
		for {
			select {
			case e := <-h.events:
				if err := send(st, "t", e); err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	})
	h.c = newTestClient(t, h.f)
	return h
}

// claimOK 是默认的认领结果：租约 token 为 tok_<id>，参数为 {"path": "<id>.txt"}。
func claimOK(req *maatv1.ClaimToolCallRequest) *maatv1.ClaimToolCallResponse {
	args, err := structpb.NewStruct(map[string]any{"path": req.GetToolCallId() + ".txt"})
	if err != nil {
		panic(err)
	}
	return &maatv1.ClaimToolCallResponse{LeaseToken: "tok_" + req.GetToolCallId(), Args: args,
		ToolCall: &maatv1.ToolCall{Id: req.GetToolCallId(), Status: maatv1.ToolCallStatus_TOOL_CALL_STATUS_CLAIMED}}
}

func (h *toolHarness) push(evts ...*maatv1.Event) {
	for _, e := range evts {
		h.events <- e
	}
}

func (h *toolHarness) run(tools ...Tool) *Run {
	return &Run{ID: "run_1", SessionID: "ses_1", ThreadID: "thr_1", c: h.c, tools: tools}
}

// stream 在后台迭代 Run.Stream，返回等待迭代结束并取得全部事件的函数。
func (h *toolHarness) stream(ctx context.Context, r *Run, opts ...StreamOption) func() ([]Event, error) {
	done := make(chan struct{})
	var (
		evts []Event
		serr error
	)
	go func() {
		defer close(done)
		for ev, err := range r.Stream(ctx, opts...) {
			if err != nil {
				serr = err
				return
			}
			evts = append(evts, ev)
		}
	}()
	return func() ([]Event, error) {
		h.t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			h.t.Fatal("stream did not end")
		}
		return evts, serr
	}
}

func (h *toolHarness) waitSubmit() *maatv1.SubmitToolResultRequest {
	h.t.Helper()
	select {
	case req := <-h.submitted:
		return req
	case <-time.After(5 * time.Second):
		h.t.Fatal("no tool result was submitted")
		return nil
	}
}

func (h *toolHarness) counts() (claims, renews, submits int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.claims), len(h.renews), len(h.submits)
}

// waitUntil 轮询直到 cond 成立。
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func resultText(r *maatv1.ToolResult) string {
	var parts []string
	for _, p := range r.GetContent() {
		parts = append(parts, p.GetText())
	}
	return strings.Join(parts, "|")
}

// readFileTool 返回 "content of <path>"，并把 executor_state_ref 设为 "state-<path>"。
func readFileTool() Tool {
	return NewTool("read_file", "read a file", SchemaFor[readFileArgs](), func(ctx context.Context, a readFileArgs) (string, error) {
		if err := SetExecutorStateRef(ctx, "state-"+a.Path); err != nil {
			return "", err
		}
		return "content of " + a.Path, nil
	})
}

// blockingTool 阻塞到 ctx 结束，把 context.Cause 发到 causes。
func blockingTool(started chan<- string, causes chan<- error) Tool {
	return NewTool("read_file", "", nil, func(ctx context.Context, a readFileArgs) (string, error) {
		started <- a.Path
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return "too late", nil
	})
}

func TestRunStreamExecutesRegisteredTools(t *testing.T) {
	h := newToolHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(readFileTool()))
	h.push(
		toolCall(1, "run_1", "tc_1", "read_file", map[string]any{"path": "ignored"}, 0),
		toolCall(2, "run_1", "tc_2", "not_mine", map[string]any{}, 0),
	)
	got := h.waitSubmit()
	if got.GetToolCallId() != "tc_1" || got.GetLeaseToken() != "tok_tc_1" || got.GetExecutorId() != h.c.executorID ||
		!strings.HasPrefix(got.GetExecutorId(), "exe_") || got.GetExecutorStateRef() != "state-tc_1.txt" ||
		resultText(got.GetResult()) != "content of tc_1.txt" || got.GetResult().GetIsError() {
		t.Fatalf("submit = %v", got)
	}
	h.push(toolCompleted(3, "run_1", "tc_1"), runCompleted(4, "run_1"))
	evts, err := wait()
	if err != nil {
		t.Fatal(err)
	}
	var tools []ToolCallEvent
	for _, ev := range evts {
		if tc, ok := ev.(ToolCallEvent); ok {
			tools = append(tools, tc)
		}
	}
	want := []ToolCallEvent{
		{ThreadID: "thr_1", RunID: "run_1", StepID: "stp_1", ToolCallID: "tc_1", Name: "read_file", Status: ToolCallPending, Args: map[string]any{"path": "ignored"}},
		{ThreadID: "thr_1", RunID: "run_1", StepID: "stp_1", ToolCallID: "tc_2", Name: "not_mine", Status: ToolCallPending, Args: map[string]any{}},
		{ThreadID: "thr_1", RunID: "run_1", StepID: "stp_1", ToolCallID: "tc_1", Name: "read_file", Status: ToolCallCompleted},
	}
	if !reflect.DeepEqual(tools, want) {
		t.Fatalf("tool events = %+v", tools)
	}
	if _, ok := evts[len(evts)-1].(RunCompletedEvent); !ok {
		t.Fatalf("last event = %#v", evts[len(evts)-1])
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.claims) != 1 || h.claims[0].GetToolCallId() != "tc_1" || h.claims[0].GetExecutorId() != h.c.executorID ||
		h.claims[0].GetLeaseSeconds() != 60 {
		t.Fatalf("claims = %v", h.claims)
	}
}

func TestClaimConflictSkipsExecution(t *testing.T) {
	h := newToolHarness(t)
	h.claimFn = func(int, *maatv1.ClaimToolCallRequest) (*maatv1.ClaimToolCallResponse, error) {
		return nil, platformErrorMeta(connect.CodeAlreadyExists, "already_claimed", "tool call is already claimed", map[string]string{"claimed_by": "exe_other"})
	}
	var called atomic.Bool
	tool := NewTool("read_file", "", nil, func(context.Context, readFileArgs) (string, error) {
		called.Store(true)
		return "", nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(tool))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	waitUntil(t, "the claim", func() bool { n, _, _ := h.counts(); return n == 1 })
	h.push(runCompleted(2, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	if claims, _, submits := h.counts(); claims != 1 || submits != 0 || called.Load() {
		t.Fatalf("claims=%d submits=%d called=%v", claims, submits, called.Load())
	}
}

func TestToolCallCancelledCancelsTool(t *testing.T) {
	h := newToolHarness(t)
	started, causes := make(chan string, 1), make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(blockingTool(started, causes)))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	<-started
	h.push(toolCancelled(2, "run_1", "tc_1"))
	if cause := <-causes; !errors.Is(cause, ErrToolCallCancelled) {
		t.Fatalf("cause = %v", cause)
	}
	h.push(runCompleted(3, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	if _, _, submits := h.counts(); submits != 0 {
		t.Fatalf("a cancelled call was submitted %d times", submits)
	}
}

func TestRunEndCancelsRunningTools(t *testing.T) {
	h := newToolHarness(t)
	started, causes := make(chan string, 1), make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(blockingTool(started, causes)))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	<-started
	h.push(runCompleted(2, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	// Stream 返回前等待工具函数结束，因此 cause 已经送达。
	select {
	case cause := <-causes:
		if !errors.Is(cause, ErrRunEnded) {
			t.Fatalf("cause = %v", cause)
		}
	default:
		t.Fatal("Stream returned before the tool function")
	}
	if _, _, submits := h.counts(); submits != 0 {
		t.Fatalf("submits = %d", submits)
	}
}

// 续约每 lease/3 一次；暂时性错误留到下一次，续约返回 tool_call_cancelled 时取消工具函数。
func TestRenewLease(t *testing.T) {
	h := newToolHarness(t)
	h.c.cfg.toolLease = 30 * time.Millisecond
	h.renewFn = func(n int, _ *maatv1.RenewToolCallLeaseRequest) (*maatv1.RenewToolCallLeaseResponse, error) {
		switch {
		case n <= 3: // 第一次续约连同内部重试全部失败
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("try later"))
		case n < 6:
			return &maatv1.RenewToolCallLeaseResponse{}, nil
		}
		return nil, platformError(connect.CodeFailedPrecondition, "tool_call_cancelled", "tool call has been cancelled")
	}
	started, causes := make(chan string, 1), make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(blockingTool(started, causes)))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	<-started
	if cause := <-causes; !errors.Is(cause, ErrToolCallCancelled) {
		t.Fatalf("cause = %v", cause)
	}
	h.push(runCompleted(2, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.renews) != 6 || len(h.submits) != 0 {
		t.Fatalf("renews=%d submits=%d", len(h.renews), len(h.submits))
	}
	for _, r := range h.renews {
		if r.GetToolCallId() != "tc_1" || r.GetLeaseToken() != "tok_tc_1" || r.GetLeaseSeconds() != 10 {
			t.Fatalf("renew = %v", r)
		}
	}
}

// 续约被拒绝（租约已不属于本执行器）时以 ErrLeaseLost 取消。
func TestRenewLeaseLost(t *testing.T) {
	h := newToolHarness(t)
	h.c.cfg.toolLease = 30 * time.Millisecond
	h.renewFn = func(int, *maatv1.RenewToolCallLeaseRequest) (*maatv1.RenewToolCallLeaseResponse, error) {
		return nil, platformError(connect.CodeFailedPrecondition, "lease_mismatch", "lease token does not match")
	}
	started, causes := make(chan string, 1), make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(blockingTool(started, causes)))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	if cause := <-causes; !errors.Is(cause, ErrLeaseLost) {
		t.Fatalf("cause = %v", cause)
	}
	h.push(runCompleted(2, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitRetriesAndFallsBack(t *testing.T) {
	tests := []struct {
		name  string
		fail  error
		want  []string
		isErr bool
	}{
		{
			name: "暂时性错误重试相同内容",
			fail: connect.NewError(connect.CodeUnavailable, errors.New("try later")),
			want: []string{"content of tc_1.txt", "content of tc_1.txt"},
		},
		{
			name:  "结果被拒绝时改为回传错误结果",
			fail:  platformError(connect.CodeInvalidArgument, "tool_result_too_large", "tool result exceeds 4194304 bytes"),
			want:  []string{"content of tc_1.txt", "maat: the platform rejected the tool result: tool result exceeds 4194304 bytes"},
			isErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newToolHarness(t)
			h.submitFn = func(n int, _ *maatv1.SubmitToolResultRequest) (*maatv1.SubmitToolResultResponse, error) {
				if n == 1 {
					return nil, tt.fail
				}
				return &maatv1.SubmitToolResultResponse{}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			wait := h.stream(ctx, h.run(readFileTool()))
			h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
			last := h.waitSubmit()
			h.push(runCompleted(2, "run_1"))
			if _, err := wait(); err != nil {
				t.Fatal(err)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			var texts []string
			for _, s := range h.submits {
				texts = append(texts, resultText(s.GetResult()))
			}
			if !reflect.DeepEqual(texts, tt.want) || last.GetResult().GetIsError() != tt.isErr ||
				(tt.isErr && last.GetExecutorStateRef() != "") {
				t.Fatalf("submits = %q, last = %v", texts, last)
			}
		})
	}
}

// 已被取消或已解决的调用回传失败时不再重试。
func TestSubmitIgnoresResolvedCalls(t *testing.T) {
	h := newToolHarness(t)
	h.submitFn = func(int, *maatv1.SubmitToolResultRequest) (*maatv1.SubmitToolResultResponse, error) {
		return nil, platformError(connect.CodeFailedPrecondition, "tool_call_cancelled", "tool call has been cancelled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(readFileTool()))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	waitUntil(t, "the submit", func() bool { _, _, n := h.counts(); return n == 1 })
	h.push(runCompleted(2, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	if _, _, n := h.counts(); n != 1 {
		t.Fatalf("submits = %d", n)
	}
}

func TestToolConcurrencyLimit(t *testing.T) {
	h := newToolHarness(t)
	h.c.cfg.toolConcurrency = 2
	var active, peak, started atomic.Int32
	release := make(chan struct{})
	tool := NewTool("read_file", "", nil, func(context.Context, readFileArgs) (string, error) {
		started.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		return "ok", nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(tool))
	for i, id := range []string{"tc_1", "tc_2", "tc_3", "tc_4"} {
		h.push(toolCall(uint64(i+1), "run_1", id, "read_file", nil, 0))
	}
	waitUntil(t, "two running tools", func() bool { return started.Load() == 2 })
	time.Sleep(50 * time.Millisecond)
	// 等待空位的调用还没有认领，其他执行器可以先认领它们。
	if claims, _, _ := h.counts(); started.Load() != 2 || claims != 2 {
		t.Fatalf("started=%d claims=%d with a limit of 2", started.Load(), claims)
	}
	close(release)
	for range 4 {
		h.waitSubmit()
	}
	h.push(runCompleted(5, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency = %d", peak.Load())
	}
}

func TestAutoExecuteDisabled(t *testing.T) {
	h := newToolHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(readFileTool()), WithAutoExecute(false))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0), runCompleted(2, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	if claims, _, _ := h.counts(); claims != 0 {
		t.Fatalf("claims = %d", claims)
	}
}

// 执行器掉线后平台重新开放调用：新的派发再次认领；同一次派发不重复执行。
func TestReopenedCallIsDispatchedAgain(t *testing.T) {
	h := newToolHarness(t)
	h.claimFn = func(n int, req *maatv1.ClaimToolCallRequest) (*maatv1.ClaimToolCallResponse, error) {
		if n == 1 {
			return nil, platformErrorMeta(connect.CodeAlreadyExists, "already_claimed", "claimed", map[string]string{"claimed_by": "exe_other"})
		}
		return claimOK(req), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := h.stream(ctx, h.run(readFileTool()))
	h.push(toolCall(1, "run_1", "tc_1", "read_file", nil, 0))
	waitUntil(t, "the first claim", func() bool { n, _, _ := h.counts(); return n == 1 })
	h.push(toolReopened(2, "run_1", "tc_1", 1))
	if got := h.waitSubmit(); got.GetToolCallId() != "tc_1" {
		t.Fatalf("submit = %v", got)
	}
	h.push(toolReopened(3, "run_1", "tc_1", 1), toolCompleted(4, "run_1", "tc_1"), runCompleted(5, "run_1"))
	if _, err := wait(); err != nil {
		t.Fatal(err)
	}
	if claims, _, submits := h.counts(); claims != 2 || submits != 1 {
		t.Fatalf("claims=%d submits=%d", claims, submits)
	}
}

func TestExecutorAttach(t *testing.T) {
	h := newToolHarness(t)
	var pendingReqs []*maatv1.ListPendingToolCallsRequest
	h.f.get = func(*maatv1.GetSessionRequest) (*maatv1.GetSessionResponse, error) {
		return &maatv1.GetSessionResponse{Session: protoSession("ses_1", 7)}, nil
	}
	pending := func(id string, status maatv1.ToolCallStatus) *maatv1.ToolCall {
		return &maatv1.ToolCall{Id: id, RunId: "run_1", Name: "read_file", Kind: maatv1.ToolCallKind_TOOL_CALL_KIND_CLIENT, Status: status}
	}
	h.f.pending = func(req *maatv1.ListPendingToolCallsRequest) (*maatv1.ListPendingToolCallsResponse, error) {
		h.mu.Lock()
		pendingReqs = append(pendingReqs, req)
		h.mu.Unlock()
		if req.GetPage().GetPageToken() == "" {
			return &maatv1.ListPendingToolCallsResponse{
				ToolCalls: []*maatv1.ToolCall{pending("tc_1", maatv1.ToolCallStatus_TOOL_CALL_STATUS_PENDING)},
				Page:      &maatv1.PageResponse{NextPageToken: "p2"},
			}, nil
		}
		return &maatv1.ListPendingToolCallsResponse{
			ToolCalls: []*maatv1.ToolCall{pending("tc_2", maatv1.ToolCallStatus_TOOL_CALL_STATUS_CLAIMED)},
		}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.c.Executor(readFileTool()).Attach(ctx, "ses_1") }()

	if got := h.waitSubmit(); got.GetToolCallId() != "tc_1" {
		t.Fatalf("backfilled submit = %v", got)
	}
	// 事件流中新发起的调用，以及补拉时被他人认领、之后重新开放的调用。
	h.push(toolCall(8, "run_1", "tc_3", "read_file", nil, 0), toolReopened(9, "run_1", "tc_2", 1))
	ids := map[string]bool{}
	for range 2 {
		ids[h.waitSubmit().GetToolCallId()] = true
	}
	if !ids["tc_2"] || !ids["tc_3"] {
		t.Fatalf("submitted = %v", ids)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Attach = %v", err)
	}
	streams := h.f.streams()
	if len(streams) != 1 || streams[0].GetAfterSeq() != 7 || streams[0].GetIncludeDeltas() {
		t.Fatalf("stream requests = %v", streams)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(pendingReqs) != 2 || pendingReqs[0].GetSessionId() != "ses_1" || !reflect.DeepEqual(pendingReqs[0].GetNames(), []string{"read_file"}) ||
		pendingReqs[1].GetPage().GetPageToken() != "p2" {
		t.Fatalf("list pending = %v", pendingReqs)
	}
}

func TestExecutorAttachInvalidTool(t *testing.T) {
	h := newToolHarness(t)
	err := h.c.Executor(Tool{}).Attach(context.Background(), "ses_1")
	if err == nil || !strings.Contains(err.Error(), "maat.NewTool") {
		t.Fatalf("Attach = %v", err)
	}
}

func TestCreateAndSendDeclareTools(t *testing.T) {
	var (
		creates []*maatv1.CreateSessionRequest
		sends   []*maatv1.SendMessageRequest
	)
	f := &fakeBackend{
		create: func(req *maatv1.CreateSessionRequest) (*maatv1.CreateSessionResponse, error) {
			creates = append(creates, req)
			return &maatv1.CreateSessionResponse{Session: protoSession("ses_1", 3)}, nil
		},
		send: func(req *maatv1.SendMessageRequest) (*maatv1.SendMessageResponse, error) {
			sends = append(sends, req)
			return &maatv1.SendMessageResponse{MessageId: "msg_1", RunId: "run_1", Delivery: maatv1.Delivery_DELIVERY_NEW_RUN}, nil
		},
	}
	c := newTestClient(t, f)
	ctx := context.Background()
	if _, err := c.Sessions.Create(ctx, CreateSessionParams{Agent: "agt_1", Tools: []Tool{{}}}); err == nil || len(creates) != 0 {
		t.Fatalf("invalid tool: err=%v creates=%d", err, len(creates))
	}
	readFile := readFileTool()
	s, err := c.Sessions.Create(ctx, CreateSessionParams{Agent: "agt_1", Tools: []Tool{readFile}})
	if err != nil {
		t.Fatal(err)
	}
	if len(creates) != 1 || len(creates[0].GetTools()) != 1 || creates[0].GetTools()[0].GetName() != "read_file" ||
		len(s.Tools) != 1 {
		t.Fatalf("create = %v, session tools = %v", creates, s.Tools)
	}
	run, err := s.Send(ctx, "hi")
	if err != nil {
		t.Fatal(err)
	}
	if len(sends[0].GetTools()) != 0 || sends[0].GetInterrupt() || len(run.tools) != 1 || run.tools[0].Name() != "read_file" {
		t.Fatalf("send = %v, run tools = %v", sends[0], run.tools)
	}
	other := NewTool("other", "", nil, func(context.Context, map[string]any) (string, error) { return "", nil })
	run, err = s.Send(ctx, "again", WithTools(other), WithInterrupt())
	if err != nil {
		t.Fatal(err)
	}
	if len(sends[1].GetTools()) != 1 || sends[1].GetTools()[0].GetName() != "other" || !sends[1].GetInterrupt() ||
		len(run.tools) != 1 || run.tools[0].Name() != "other" {
		t.Fatalf("send = %v, run tools = %v", sends[1], run.tools)
	}
	if _, err := s.Send(ctx, "bad", WithTools(other, other)); err == nil || len(sends) != 2 {
		t.Fatalf("duplicate tools: err=%v sends=%d", err, len(sends))
	}
}

func TestSessionInterrupt(t *testing.T) {
	var reqs []*maatv1.InterruptSessionRequest
	f := &fakeBackend{interrupt: func(req *maatv1.InterruptSessionRequest) (*maatv1.InterruptSessionResponse, error) {
		reqs = append(reqs, req)
		if len(reqs) == 2 {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("try later"))
		}
		return &maatv1.InterruptSessionResponse{ThreadId: "thr_2", Generation: 1}, nil
	}}
	c := newTestClient(t, f)
	s := &Session{ID: "ses_1", c: c}
	if err := s.Interrupt(context.Background(), InterruptThread("thr_2")); err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 1 || reqs[0].GetSessionId() != "ses_1" || reqs[0].GetThreadId() != "thr_2" {
		t.Fatalf("requests = %v", reqs)
	}
	// 中断不是幂等的：失败时不重试。
	if err := s.Interrupt(context.Background()); err == nil || len(reqs) != 2 {
		t.Fatalf("err=%v requests=%d", err, len(reqs))
	}
}

// platformErrorMeta 与 platformError 相同，但 ErrorInfo 带指定的 metadata。
func platformErrorMeta(code connect.Code, reason, msg string, meta map[string]string) *connect.Error {
	err := connect.NewError(code, errors.New(msg))
	if d, derr := connect.NewErrorDetail(&maatv1.ErrorInfo{Reason: reason, Metadata: meta}); derr == nil {
		err.AddDetail(d)
	}
	return err
}
