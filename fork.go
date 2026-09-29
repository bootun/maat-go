package maat

import (
	"context"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// ForkSessionParams 是 fork 会话的参数（spec §8.3）。
type ForkSessionParams struct {
	// CheckpointID 是 fork 点，必须是稳定的 checkpoint（见 Checkpoints.List 的 StableOnly），必填。
	CheckpointID string
	// Message 是新会话的第一条消息；为空（没有 Parts）时新会话保持空闲。
	// 带消息需要 sessions.write 权限，不带消息需要 sessions.manage。
	Message MessageInput
	// ClientMessageID 是 Message 的客户端 ID；为空时 SDK 生成一个。
	ClientMessageID string
	// Model 覆盖模型别名；为空时继承源会话。
	Model string
	// Tools 非空时覆盖新会话的工具集，并由本进程执行（与 CreateSessionParams.Tools 相同；传入与源会话相同的
	// 工具即可在保持声明不变的同时由本进程执行）。为空时继承源会话的工具声明，返回的 Run 不自动执行工具，
	// 需要用 Client.Executor 接入。
	Tools []Tool
	// Title 与 Metadata 不从源会话继承。
	Title    string
	Metadata map[string]string
	// IdempotencyKey 是幂等键；为空时 SDK 生成一个，保证内部重试不会重复创建。
	IdempotencyKey string
}

// ForkResult 是 Fork 的结果。
type ForkResult struct {
	Session *Session
	// Run 是处理 Message 的 Run；不带消息时为 nil。
	Run *Run
	// ExecutorStateRef 是 fork 点（或它之前最近一个非空）的 executor_state_ref（spec §8.3、§8.4）。
	// 本进程执行工具时，应当在 Run.Stream / Run.Wait 之前据此恢复执行环境（例如 git checkout）；
	// 在其他进程执行工具时使用 Executor.OnFork。
	ExecutorStateRef string
}

// Fork 从稳定的 checkpoint 创建新会话：新会话的上下文就是 checkpoint 的上下文，Agent 版本与源会话相同。
func (s *Sessions) Fork(ctx context.Context, p ForkSessionParams) (*ForkResult, error) {
	defs, err := toolDefinitions(p.Tools)
	if err != nil {
		return nil, err
	}
	req := &maatv1.ForkSessionRequest{
		CheckpointId: p.CheckpointID, Model: p.Model, Tools: defs, Title: p.Title, Metadata: p.Metadata,
		IdempotencyKey: p.IdempotencyKey,
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = newIdempotencyKey()
	}
	if len(p.Message.Parts) > 0 {
		if req.Message, err = p.Message.toProto(); err != nil {
			return nil, err
		}
		req.ClientMessageId = p.ClientMessageID
		if req.ClientMessageId == "" {
			req.ClientMessageId = newIdempotencyKey()
		}
	}
	var res *maatv1.ForkSessionResponse
	err = s.c.call(ctx, true, func(ctx context.Context) error {
		r, err := s.c.sessions.ForkSession(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	sess := s.c.sessionOf(res.GetSession())
	sess.Tools = append([]Tool(nil), p.Tools...)
	out := &ForkResult{Session: sess, ExecutorStateRef: res.GetExecutorStateRef()}
	if res.GetRunId() != "" {
		// 新会话的全部事件都属于这次 fork，Run 从头订阅。
		out.Run = &Run{
			ID: res.GetRunId(), SessionID: sess.ID, ThreadID: res.GetThreadId(), MessageID: res.GetMessageId(),
			Delivery: DeliveryNewRun, c: s.c, tools: append([]Tool(nil), p.Tools...),
		}
	}
	return out, nil
}

// ForkInfo 描述一个 fork 出的会话从哪里来，以及恢复执行环境所需的 executor_state_ref。
type ForkInfo struct {
	// Session 是 fork 出的会话（接入时的快照）。
	Session *Session
	From    ForkOrigin
	// ExecutorStateRef 是 fork 点（或它之前最近一个非空）的 executor_state_ref；可能为空。
	ExecutorStateRef string
}
