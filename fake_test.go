package maat

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
	"github.com/bootun/maat-go/gen/maat/v1/maatv1connect"
)

// fakeBackend 是单元测试用的假平台：每个 RPC 的行为由测试通过函数字段指定。
type fakeBackend struct {
	maatv1connect.UnimplementedMetaServiceHandler
	maatv1connect.UnimplementedSessionServiceHandler
	maatv1connect.UnimplementedEventServiceHandler
	maatv1connect.UnimplementedToolServiceHandler
	maatv1connect.UnimplementedBlobServiceHandler

	whoAmI     func(ctx context.Context, req *connect.Request[maatv1.WhoAmIRequest]) (*connect.Response[maatv1.WhoAmIResponse], error)
	create     func(req *maatv1.CreateSessionRequest) (*maatv1.CreateSessionResponse, error)
	get        func(req *maatv1.GetSessionRequest) (*maatv1.GetSessionResponse, error)
	list       func(req *maatv1.ListSessionsRequest) (*maatv1.ListSessionsResponse, error)
	send       func(req *maatv1.SendMessageRequest) (*maatv1.SendMessageResponse, error)
	listEvents func(req *maatv1.ListSessionEventsRequest) (*maatv1.ListSessionEventsResponse, error)
	interrupt  func(req *maatv1.InterruptSessionRequest) (*maatv1.InterruptSessionResponse, error)
	claim      func(req *maatv1.ClaimToolCallRequest) (*maatv1.ClaimToolCallResponse, error)
	renew      func(req *maatv1.RenewToolCallLeaseRequest) (*maatv1.RenewToolCallLeaseResponse, error)
	submit     func(req *maatv1.SubmitToolResultRequest) (*maatv1.SubmitToolResultResponse, error)
	pending    func(req *maatv1.ListPendingToolCallsRequest) (*maatv1.ListPendingToolCallsResponse, error)
	fork       func(req *maatv1.ForkSessionRequest) (*maatv1.ForkSessionResponse, error)
	ckpts      func(req *maatv1.ListCheckpointsRequest) (*maatv1.ListCheckpointsResponse, error)
	annotate   func(req *maatv1.AnnotateCheckpointRequest) (*maatv1.AnnotateCheckpointResponse, error)
	getBlob    func(req *maatv1.GetBlobRequest) (*maatv1.GetBlobResponse, error)
	// conns[i] 是第 i 个事件流连接的行为；超出时连接一直保持到客户端断开。
	conns []func(ctx context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error

	mu         sync.Mutex
	streamReqs []*maatv1.StreamSessionEventsRequest
	streamHdrs []http.Header
}

func (f *fakeBackend) WhoAmI(ctx context.Context, req *connect.Request[maatv1.WhoAmIRequest]) (*connect.Response[maatv1.WhoAmIResponse], error) {
	return f.whoAmI(ctx, req)
}

func (f *fakeBackend) CreateSession(_ context.Context, req *connect.Request[maatv1.CreateSessionRequest]) (*connect.Response[maatv1.CreateSessionResponse], error) {
	return respond(f.create(req.Msg))
}

func (f *fakeBackend) GetSession(_ context.Context, req *connect.Request[maatv1.GetSessionRequest]) (*connect.Response[maatv1.GetSessionResponse], error) {
	return respond(f.get(req.Msg))
}

func (f *fakeBackend) ListSessions(_ context.Context, req *connect.Request[maatv1.ListSessionsRequest]) (*connect.Response[maatv1.ListSessionsResponse], error) {
	return respond(f.list(req.Msg))
}

func (f *fakeBackend) SendMessage(_ context.Context, req *connect.Request[maatv1.SendMessageRequest]) (*connect.Response[maatv1.SendMessageResponse], error) {
	return respond(f.send(req.Msg))
}

func (f *fakeBackend) ListSessionEvents(_ context.Context, req *connect.Request[maatv1.ListSessionEventsRequest]) (*connect.Response[maatv1.ListSessionEventsResponse], error) {
	return respond(f.listEvents(req.Msg))
}

func (f *fakeBackend) InterruptSession(_ context.Context, req *connect.Request[maatv1.InterruptSessionRequest]) (*connect.Response[maatv1.InterruptSessionResponse], error) {
	return respond(f.interrupt(req.Msg))
}

func (f *fakeBackend) ClaimToolCall(_ context.Context, req *connect.Request[maatv1.ClaimToolCallRequest]) (*connect.Response[maatv1.ClaimToolCallResponse], error) {
	return respond(f.claim(req.Msg))
}

func (f *fakeBackend) RenewToolCallLease(_ context.Context, req *connect.Request[maatv1.RenewToolCallLeaseRequest]) (*connect.Response[maatv1.RenewToolCallLeaseResponse], error) {
	return respond(f.renew(req.Msg))
}

func (f *fakeBackend) SubmitToolResult(_ context.Context, req *connect.Request[maatv1.SubmitToolResultRequest]) (*connect.Response[maatv1.SubmitToolResultResponse], error) {
	return respond(f.submit(req.Msg))
}

func (f *fakeBackend) ListPendingToolCalls(_ context.Context, req *connect.Request[maatv1.ListPendingToolCallsRequest]) (*connect.Response[maatv1.ListPendingToolCallsResponse], error) {
	return respond(f.pending(req.Msg))
}

func (f *fakeBackend) ForkSession(_ context.Context, req *connect.Request[maatv1.ForkSessionRequest]) (*connect.Response[maatv1.ForkSessionResponse], error) {
	return respond(f.fork(req.Msg))
}

func (f *fakeBackend) ListCheckpoints(_ context.Context, req *connect.Request[maatv1.ListCheckpointsRequest]) (*connect.Response[maatv1.ListCheckpointsResponse], error) {
	return respond(f.ckpts(req.Msg))
}

func (f *fakeBackend) AnnotateCheckpoint(_ context.Context, req *connect.Request[maatv1.AnnotateCheckpointRequest]) (*connect.Response[maatv1.AnnotateCheckpointResponse], error) {
	return respond(f.annotate(req.Msg))
}

func (f *fakeBackend) GetBlob(_ context.Context, req *connect.Request[maatv1.GetBlobRequest]) (*connect.Response[maatv1.GetBlobResponse], error) {
	return respond(f.getBlob(req.Msg))
}

func (f *fakeBackend) StreamSessionEvents(ctx context.Context, req *connect.Request[maatv1.StreamSessionEventsRequest],
	st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
	f.mu.Lock()
	i := len(f.streamReqs)
	f.streamReqs = append(f.streamReqs, req.Msg)
	f.streamHdrs = append(f.streamHdrs, req.Header().Clone())
	f.mu.Unlock()
	if i >= len(f.conns) {
		<-ctx.Done()
		return nil
	}
	return f.conns[i](ctx, st)
}

func (f *fakeBackend) streams() []*maatv1.StreamSessionEventsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*maatv1.StreamSessionEventsRequest(nil), f.streamReqs...)
}

func (f *fakeBackend) header(i int) http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.streamHdrs[i]
}

func respond[T any](msg *T, err error) (*connect.Response[T], error) {
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(msg), nil
}

// newTestClient 启动假平台并返回指向它的 Client（重试与重连的退避缩短到毫秒级）。
func newTestClient(t *testing.T, f *fakeBackend) *Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(maatv1connect.NewMetaServiceHandler(f))
	mux.Handle(maatv1connect.NewSessionServiceHandler(f))
	mux.Handle(maatv1connect.NewEventServiceHandler(f))
	mux.Handle(maatv1connect.NewToolServiceHandler(f))
	mux.Handle(maatv1connect.NewBlobServiceHandler(f))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := NewClient(WithBaseURL(srv.URL), WithAPIKey("test-key"), WithHTTPClient(srv.Client()),
		WithLogger(slog.New(slog.DiscardHandler)))
	c.cfg.retry = backoff{min: time.Millisecond, max: time.Millisecond}
	c.cfg.reconnect = backoff{min: time.Millisecond, max: 5 * time.Millisecond}
	return c
}

// send 在事件流上发送一条事件。
func send(st *connect.ServerStream[maatv1.StreamSessionEventsResponse], token string, ev *maatv1.Event) error {
	return st.Send(&maatv1.StreamSessionEventsResponse{Event: ev, ResumeToken: token})
}

// 以下是构造事件的辅助函数。

func marker(run, step string, attempt uint32) *maatv1.Event {
	return &maatv1.Event{Type: "stream.marker", RunId: run, StepId: step, Attempt: attempt,
		Payload: &maatv1.Event_StreamMarker{StreamMarker: &maatv1.StreamMarker{}}}
}

func delta(run, step string, attempt uint32, text string) *maatv1.Event {
	return &maatv1.Event{Type: "agent.message.delta", RunId: run, StepId: step, Attempt: attempt,
		Payload: &maatv1.Event_AgentMessageDelta{AgentMessageDelta: &maatv1.AgentMessageDelta{Text: text}}}
}

func message(seq uint64, run, step, text string) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "agent.message", RunId: run, StepId: step,
		Payload: &maatv1.Event_AgentMessage{AgentMessage: &maatv1.AgentMessage{Text: text}}}
}

func reset() *maatv1.Event {
	return &maatv1.Event{Type: "stream.reset", Payload: &maatv1.Event_StreamReset{StreamReset: &maatv1.StreamReset{}}}
}

func heartbeat(token string, closing bool) *maatv1.Event {
	return &maatv1.Event{Type: "stream.heartbeat",
		Payload: &maatv1.Event_StreamHeartbeat{StreamHeartbeat: &maatv1.StreamHeartbeat{ResumeToken: token, Closing: closing}}}
}

func threadStatus(seq uint64, run string, status maatv1.ThreadStatus, reason maatv1.StopReason) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "thread.status_changed", ThreadId: "thr_1", RunId: run,
		Payload: &maatv1.Event_ThreadStatusChanged{ThreadStatusChanged: &maatv1.ThreadStatusChanged{Status: status, StopReason: reason}}}
}

func runCompleted(seq uint64, run string) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "run.completed", ThreadId: "thr_1", RunId: run,
		Payload: &maatv1.Event_RunCompleted{RunCompleted: &maatv1.RunCompleted{
			StopReason: maatv1.StopReason_STOP_REASON_END_TURN, Usage: &maatv1.Usage{InputTokens: 10, OutputTokens: 5},
			FinalTextPreview: "preview",
		}}}
}

func runFailed(seq uint64, run, code, msg string) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "run.failed", ThreadId: "thr_1", RunId: run,
		Payload: &maatv1.Event_RunFailed{RunFailed: &maatv1.RunFailed{Code: code, Message: msg}}}
}

// 以下是工具调用相关的事件。

func toolCall(seq uint64, run, id, name string, args map[string]any, attempt uint32) *maatv1.Event {
	st, err := structpb.NewStruct(args)
	if err != nil {
		panic(err)
	}
	return &maatv1.Event{Seq: seq, Type: "agent.tool_call", ThreadId: "thr_1", RunId: run, StepId: "stp_1",
		Payload: &maatv1.Event_AgentToolCall{AgentToolCall: &maatv1.AgentToolCall{ToolCall: &maatv1.ToolCall{
			Id: id, RunId: run, ThreadId: "thr_1", StepId: "stp_1", Name: name, Kind: maatv1.ToolCallKind_TOOL_CALL_KIND_CLIENT,
			Args: st, Status: maatv1.ToolCallStatus_TOOL_CALL_STATUS_PENDING, DispatchAttempt: attempt,
		}}}}
}

func toolCompleted(seq uint64, run, id string) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "tool_call.completed", ThreadId: "thr_1", RunId: run, StepId: "stp_1",
		Payload: &maatv1.Event_ToolCallCompleted{ToolCallCompleted: &maatv1.ToolCallCompleted{ToolCallId: id}}}
}

func toolCancelled(seq uint64, run, id string) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "tool_call.cancelled", ThreadId: "thr_1", RunId: run, StepId: "stp_1",
		Payload: &maatv1.Event_ToolCallCancelled{ToolCallCancelled: &maatv1.ToolCallCancelled{ToolCallId: id, Reason: "interrupted"}}}
}

func toolReopened(seq uint64, run, id string, attempt uint32) *maatv1.Event {
	return &maatv1.Event{Seq: seq, Type: "tool_call.reopened", ThreadId: "thr_1", RunId: run, StepId: "stp_1",
		Payload: &maatv1.Event_ToolCallReopened{ToolCallReopened: &maatv1.ToolCallReopened{ToolCallId: id, DispatchAttempt: attempt}}}
}
