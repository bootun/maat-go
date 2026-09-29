package maat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// 执行器取消工具函数的 ctx 时，context.Cause(ctx) 为以下错误之一，此时结果不再回传。
// （传给 Run.Stream、Attach 的 ctx 结束时，Cause 为该 ctx 的原因；Timeout 到期时为 context.DeadlineExceeded，
// 工具函数返回的结果照常回传。）
var (
	// ErrToolCallCancelled：调用已被平台取消（中断、会话归档等）。
	ErrToolCallCancelled = errors.New("maat: tool call cancelled")
	// ErrLeaseLost：租约已失效（续约失败，或调用已被平台以其他方式解决，例如判定执行器掉线）。
	ErrLeaseLost = errors.New("maat: tool call lease lost")
	// ErrRunEnded：调用所属的 Run 已结束。
	ErrRunEnded = errors.New("maat: run ended")
	// ErrExecutorStopped：执行器停止（Run.Stream 结束、Attach 返回）。
	ErrExecutorStopped = errors.New("maat: executor stopped")
)

// Executor 只执行工具调用、不发送消息（spec §14.4 的 Executor 模式），适合把工具执行部署在
// 发送消息的进程之外。用 Client.Executor 构造。
type Executor struct {
	c      *Client
	tools  []Tool
	onFork func(ctx context.Context, f ForkInfo) error
}

// OnFork 设置接入 fork 出的会话时的回调（spec §8.4）：Attach 在处理任何工具调用之前调用 fn，
// 传入 fork 点的 executor_state_ref，由调用方恢复执行环境（例如 git checkout <sha>）。
// fn 返回错误时 Attach 返回该错误，不执行任何调用。每次 Attach 都会调用（包括重新接入进行中的 fork 会话，
// 此时可以根据 ForkInfo.Session 判断是否需要恢复）。返回 x 本身，便于链式调用。
func (x *Executor) OnFork(fn func(ctx context.Context, f ForkInfo) error) *Executor {
	x.onFork = fn
	return x
}

// Executor 返回执行 tools 的 Executor。同一 Client 的所有执行器使用同一个 executor_id。
func (c *Client) Executor(tools ...Tool) *Executor {
	return &Executor{c: c, tools: append([]Tool(nil), tools...)}
}

// Attach 为会话执行工具调用，阻塞直到 ctx 结束（返回 ctx 的错误）或出现不可重试的错误（例如会话不存在）。
// 它先用 ListPendingToolCalls 补拉接入前已经发起的调用，再订阅之后的已提交事件（不含 delta）；
// 只认领注册了实现的工具，其余调用留给其他执行器。多个执行器同时接入同一会话时，每个调用只有一个
// 执行器认领成功。
func (x *Executor) Attach(ctx context.Context, sessionID string) error {
	if x.c.err != nil {
		return x.c.err
	}
	ex, err := newExecutor(ctx, x.c, x.tools)
	if err != nil {
		return err
	}
	defer ex.close()
	// 订阅的起点：补拉之前的会话位置。之后发起的调用一定出现在事件流中，之前发起且仍未解决的
	// 一定出现在补拉结果中；两者重叠的部分按派发次数去重。
	s, err := x.c.Sessions.Get(ctx, sessionID)
	if err != nil {
		return err
	}
	if s.ForkedFrom != nil && x.onFork != nil {
		info, err := forkInfoOf(ctx, x.c, s)
		if err != nil {
			return err
		}
		if err := x.onFork(ctx, info); err != nil {
			return fmt.Errorf("maat: on fork: %w", err)
		}
	}
	if err := ex.backfill(ctx, sessionID); err != nil {
		return err
	}
	for raw, err := range x.c.subscribe(ctx, subscription{sessionID: sessionID, afterSeq: s.LastSeq}) {
		if err != nil {
			return err
		}
		ex.handle(raw.Event)
	}
	return ctx.Err()
}

// forkInfoOf 读取 fork 出的会话的第一条事件 session.forked，取出 executor_state_ref。
func forkInfoOf(ctx context.Context, x *Client, s *Session) (ForkInfo, error) {
	info := ForkInfo{Session: s, From: *s.ForkedFrom}
	err := x.call(ctx, true, func(ctx context.Context) error {
		r, err := x.events.ListSessionEvents(ctx, connect.NewRequest(&maatv1.ListSessionEventsRequest{
			SessionId: s.ID, Types: []string{"session.forked"}, Page: &maatv1.PageRequest{PageSize: 1},
		}))
		if err != nil {
			return err
		}
		if evts := r.Msg.GetEvents(); len(evts) > 0 {
			info.ExecutorStateRef = evts[0].GetSessionForked().GetExecutorStateRef()
		}
		return nil
	})
	if err != nil {
		return ForkInfo{}, err
	}
	return info, nil
}

// executor 执行一次订阅中收到的工具调用：认领 → 执行（并发受限）→ 按 lease/3 续约 → 回传。
type executor struct {
	c     *Client
	tools map[string]Tool
	sem   chan struct{}
	ctx   context.Context
	stop  context.CancelCauseFunc
	wg    sync.WaitGroup

	mu sync.Mutex
	// calls 是每个调用最近一次派发；调用解决（或所属 Run 结束）时删除。
	calls map[string]*dispatch
	// known 记录调用的工具名与所属 Run：tool_call.reopened 事件不带这些信息。
	known map[string]callInfo
}

type callInfo struct {
	name  string
	runID string
}

type dispatch struct {
	attempt uint32
	runID   string
	cancel  context.CancelCauseFunc
}

func newExecutor(ctx context.Context, c *Client, tools []Tool) (*executor, error) {
	if _, err := toolDefinitions(tools); err != nil {
		return nil, err
	}
	m := make(map[string]Tool, len(tools))
	for _, t := range tools {
		m[t.Name()] = t
	}
	ctx, stop := context.WithCancelCause(ctx)
	return &executor{
		c: c, tools: m, sem: make(chan struct{}, c.cfg.toolConcurrency), ctx: ctx, stop: stop,
		calls: map[string]*dispatch{}, known: map[string]callInfo{},
	}, nil
}

// close 取消所有执行中的调用并等待它们结束。
func (x *executor) close() {
	x.stop(ErrExecutorStopped)
	x.wg.Wait()
}

// backfill 派发会话中已经发起、尚未被认领的调用（只列出注册了实现的工具）。
func (x *executor) backfill(ctx context.Context, sessionID string) error {
	names := make([]string, 0, len(x.tools))
	for n := range x.tools {
		names = append(names, n)
	}
	token := ""
	for {
		var res *maatv1.ListPendingToolCallsResponse
		err := x.c.call(ctx, true, func(ctx context.Context) error {
			r, err := x.c.tools.ListPendingToolCalls(ctx, connect.NewRequest(&maatv1.ListPendingToolCallsRequest{
				SessionId: sessionID, Names: names, Page: &maatv1.PageRequest{PageToken: token},
			}))
			if err != nil {
				return err
			}
			res = r.Msg
			return nil
		})
		if err != nil {
			return err
		}
		for _, tc := range res.GetToolCalls() {
			x.remember(tc)
			if tc.GetStatus() == maatv1.ToolCallStatus_TOOL_CALL_STATUS_PENDING {
				x.start(tc.GetId(), tc.GetDispatchAttempt())
			}
		}
		if token = res.GetPage().GetNextPageToken(); token == "" {
			return nil
		}
	}
}

// handle 处理一条事件：派发新发起或重新开放的调用，取消已被取消、已解决或所属 Run 已结束的调用。
func (x *executor) handle(e *maatv1.Event) {
	switch p := e.GetPayload().(type) {
	case *maatv1.Event_AgentToolCall:
		tc := p.AgentToolCall.GetToolCall()
		x.remember(tc)
		x.start(tc.GetId(), tc.GetDispatchAttempt())
	case *maatv1.Event_ToolCallReopened:
		x.start(p.ToolCallReopened.GetToolCallId(), p.ToolCallReopened.GetDispatchAttempt())
	case *maatv1.Event_ToolCallCancelled:
		x.finish(p.ToolCallCancelled.GetToolCallId(), ErrToolCallCancelled)
	case *maatv1.Event_ToolCallCompleted:
		x.finish(p.ToolCallCompleted.GetToolCallId(), ErrLeaseLost)
	case *maatv1.Event_ToolCallFailed:
		x.finish(p.ToolCallFailed.GetToolCallId(), ErrLeaseLost)
	case *maatv1.Event_RunCompleted, *maatv1.Event_RunFailed:
		x.endRun(e.GetRunId())
	}
}

// remember 记录由调用方执行、且注册了实现的调用。
func (x *executor) remember(tc *maatv1.ToolCall) {
	if tc.GetKind() == maatv1.ToolCallKind_TOOL_CALL_KIND_PLATFORM {
		return
	}
	if _, ok := x.tools[tc.GetName()]; !ok {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.known[tc.GetId()] = callInfo{name: tc.GetName(), runID: tc.GetRunId()}
}

// start 以第 attempt 次派发执行调用。同一次派发只执行一次；更新的派发（重新开放）取代旧的。
func (x *executor) start(id string, attempt uint32) {
	x.mu.Lock()
	defer x.mu.Unlock()
	info, ok := x.known[id]
	if !ok || x.ctx.Err() != nil {
		return
	}
	if d, ok := x.calls[id]; ok {
		if d.attempt >= attempt {
			return
		}
		d.cancel(ErrLeaseLost)
	}
	ctx, cancel := context.WithCancelCause(x.ctx)
	x.calls[id] = &dispatch{attempt: attempt, runID: info.runID, cancel: cancel}
	t := x.tools[info.name]
	x.wg.Add(1)
	go func() {
		defer x.wg.Done()
		defer cancel(nil)
		x.execute(ctx, cancel, t, id)
	}()
}

// finish 在调用被解决或取消时取消仍在执行的派发。
func (x *executor) finish(id string, cause error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if d, ok := x.calls[id]; ok {
		d.cancel(cause)
		delete(x.calls, id)
	}
	delete(x.known, id)
}

// endRun 在 Run 结束时取消它仍在执行的调用。
func (x *executor) endRun(runID string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for id, d := range x.calls {
		if d.runID == runID {
			d.cancel(ErrRunEnded)
			delete(x.calls, id)
		}
	}
	for id, info := range x.known {
		if info.runID == runID {
			delete(x.known, id)
		}
	}
}

// execute 执行一次派发。ctx 被取消（调用取消、租约失效、Run 结束、执行器停止）时不回传结果。
func (x *executor) execute(ctx context.Context, cancel context.CancelCauseFunc, t Tool, id string) {
	select {
	case x.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-x.sem }()
	log := x.c.cfg.logger.With(slog.String("tool_call_id", id), slog.String("tool", t.Name()))

	claim, err := x.claim(ctx, id)
	if err != nil {
		x.logClaimError(ctx, log, err)
		return
	}
	res, ref := x.run(ctx, cancel, t, id, claim)
	if cause := context.Cause(ctx); cause != nil {
		log.DebugContext(ctx, "maat: tool result dropped", slog.String("cause", cause.Error()))
		return
	}
	x.submit(log, id, claim.GetLeaseToken(), res, ref)
}

// claim 认领调用。
func (x *executor) claim(ctx context.Context, id string) (*maatv1.ClaimToolCallResponse, error) {
	var res *maatv1.ClaimToolCallResponse
	err := x.c.call(ctx, true, func(ctx context.Context) error {
		r, err := x.c.tools.ClaimToolCall(ctx, connect.NewRequest(&maatv1.ClaimToolCallRequest{
			ToolCallId: id, ExecutorId: x.c.executorID, LeaseSeconds: x.c.cfg.leaseSeconds(),
		}))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	return res, err
}

// logClaimError 记录认领失败。被其他执行器抢先、调用已不是 pending 属于正常情况。
func (x *executor) logClaimError(ctx context.Context, log *slog.Logger, err error) {
	var me *MaatError
	switch {
	case ctx.Err() != nil:
	case errors.As(err, &me) && me.Code == connect.CodeAlreadyExists:
		if me.Metadata["claimed_by"] == x.c.executorID {
			// 之前的认领已成功但响应丢失：拿不到租约 token，只能等租约过期后由平台处理。
			log.WarnContext(ctx, "maat: claim response lost; the lease will expire", slog.String("error", err.Error()))
			return
		}
		log.DebugContext(ctx, "maat: tool call claimed by another executor", slog.String("claimed_by", me.Metadata["claimed_by"]))
	case errors.As(err, &me) && me.Code == connect.CodeFailedPrecondition:
		log.DebugContext(ctx, "maat: tool call is no longer pending", slog.String("reason", me.Reason))
	default:
		log.WarnContext(ctx, "maat: claim tool call", slog.String("error", err.Error()))
	}
}

// run 执行工具函数，期间按 lease/3 续约；返回结果与 executor_state_ref。
func (x *executor) run(ctx context.Context, cancel context.CancelCauseFunc, t Tool, id string,
	claim *maatv1.ClaimToolCallResponse) (ToolResult, string) {
	var raw json.RawMessage
	if args := claim.GetArgs(); args != nil {
		b, err := protojson.Marshal(args)
		if err != nil {
			return ErrorResult("maat: decode tool arguments: " + err.Error()), ""
		}
		raw = b
	}
	inv := &invocation{}
	tctx := context.WithValue(ctx, invocationKey{}, inv)
	if t.timeout > 0 {
		var stop context.CancelFunc
		tctx, stop = context.WithTimeout(tctx, t.timeout)
		defer stop()
	}
	rctx, stopRenew := context.WithCancel(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		x.renew(rctx, cancel, id, claim.GetLeaseToken())
	}()
	res := t.call(tctx, raw)
	stopRenew()
	<-renewed
	return res, inv.ref()
}

// renew 每 lease/3 续约一次，直到 ctx 结束。续约被拒绝时以 ErrToolCallCancelled 或 ErrLeaseLost 取消调用；
// 暂时性错误留到下一次再试（租约还剩 2/3）。
func (x *executor) renew(ctx context.Context, cancel context.CancelCauseFunc, id, token string) {
	tick := time.NewTicker(x.c.cfg.toolLease / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		err := x.c.call(ctx, true, func(ctx context.Context) error {
			_, err := x.c.tools.RenewToolCallLease(ctx, connect.NewRequest(&maatv1.RenewToolCallLeaseRequest{
				ToolCallId: id, LeaseToken: token, LeaseSeconds: x.c.cfg.leaseSeconds(),
			}))
			return err
		})
		var me *MaatError
		switch {
		case err == nil, ctx.Err() != nil:
			continue
		case errors.As(err, &me) && me.Retryable:
			x.c.cfg.logger.DebugContext(ctx, "maat: renew tool call lease", slog.String("tool_call_id", id), slog.String("error", err.Error()))
			continue
		case me != nil && me.Reason == "tool_call_cancelled":
			cancel(ErrToolCallCancelled)
		default:
			x.c.cfg.logger.WarnContext(ctx, "maat: tool call lease lost", slog.String("tool_call_id", id), slog.String("error", err.Error()))
			cancel(ErrLeaseLost)
		}
		return
	}
}

// submit 回传结果。平台拒绝结果本身（例如超过 4MB）时改为回传说明原因的错误结果，
// 保证调用得到解决；调用已被取消或已解决时忽略。
func (x *executor) submit(log *slog.Logger, id, token string, res ToolResult, ref string) {
	ctx := x.ctx
	pr, err := res.toProto()
	if err != nil {
		pr = &maatv1.ToolResult{
			Content: []*maatv1.ContentPart{{Part: &maatv1.ContentPart_Text{Text: "maat: encode tool result: " + err.Error()}}},
			IsError: true,
		}
	}
	err = x.submitOnce(ctx, id, token, pr, ref)
	var me *MaatError
	switch {
	case err == nil, ctx.Err() != nil:
		return
	case errors.As(err, &me) && (me.Reason == "tool_call_cancelled" || me.Reason == "tool_call_not_pending"):
		log.DebugContext(ctx, "maat: tool call already resolved", slog.String("reason", me.Reason))
		return
	case me != nil && me.Code == connect.CodeInvalidArgument:
		log.WarnContext(ctx, "maat: tool result rejected; submitting an error result instead", slog.String("error", err.Error()))
		fallback := &maatv1.ToolResult{
			Content: []*maatv1.ContentPart{{Part: &maatv1.ContentPart_Text{Text: "maat: the platform rejected the tool result: " + me.Message}}},
			IsError: true,
		}
		if err := x.submitOnce(ctx, id, token, fallback, ""); err != nil && ctx.Err() == nil {
			log.WarnContext(ctx, "maat: submit tool result", slog.String("error", err.Error()))
		}
	default:
		log.WarnContext(ctx, "maat: submit tool result", slog.String("error", err.Error()))
	}
}

// submitOnce 调用 SubmitToolResult。同一执行器以相同内容重复提交是幂等的，因此可以重试。
func (x *executor) submitOnce(ctx context.Context, id, token string, res *maatv1.ToolResult, ref string) error {
	return x.c.call(ctx, true, func(ctx context.Context) error {
		_, err := x.c.tools.SubmitToolResult(ctx, connect.NewRequest(&maatv1.SubmitToolResultRequest{
			ToolCallId: id, LeaseToken: token, Result: res, ExecutorStateRef: ref, ExecutorId: x.c.executorID,
		}))
		return err
	})
}
