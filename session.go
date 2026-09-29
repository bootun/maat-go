package maat

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// Sessions 管理会话。
type Sessions struct{ c *Client }

// CreateSessionParams 是创建会话的参数。
type CreateSessionParams struct {
	// Agent 是 Agent ID（agt_…），必填。
	Agent string
	// AgentVersion 是 Agent 的版本；0 表示当前版本。
	AgentVersion uint32
	// Model 是模型别名；为空时使用 Agent 的默认模型。SDK 中只出现别名，不出现 LLM 的地址与密钥。
	Model string
	Title string
	// Metadata 最多 32 个键；键匹配 ^[a-zA-Z0-9_.-]{1,64}$，值不超过 512 字节。
	Metadata map[string]string
	// IdempotencyKey 是幂等键：同一项目内重复使用时返回同一会话。为空时 SDK 生成一个，
	// 保证内部重试不会重复创建。
	IdempotencyKey string
	// Tools 是会话可用的调用方工具：声明随会话创建提交给平台，实现由本进程执行
	// （Run.Stream 默认自动执行，见 Session.Tools）。
	Tools []Tool
}

// Create 创建会话（不带首条消息，会话处于空闲状态），之后用 Session.Send 发送消息。
func (s *Sessions) Create(ctx context.Context, p CreateSessionParams) (*Session, error) {
	defs, err := toolDefinitions(p.Tools)
	if err != nil {
		return nil, err
	}
	key := p.IdempotencyKey
	if key == "" {
		key = newIdempotencyKey()
	}
	req := &maatv1.CreateSessionRequest{
		AgentId: p.Agent, AgentVersion: p.AgentVersion, Model: p.Model, Title: p.Title, Metadata: p.Metadata,
		IdempotencyKey: key, Tools: defs,
	}
	var res *maatv1.CreateSessionResponse
	err = s.c.call(ctx, true, func(ctx context.Context) error {
		r, err := s.c.sessions.CreateSession(ctx, connect.NewRequest(req))
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
	return sess, nil
}

// Get 读取会话的最新状态。
func (s *Sessions) Get(ctx context.Context, id string) (*Session, error) {
	var res *maatv1.GetSessionResponse
	err := s.c.call(ctx, true, func(ctx context.Context) error {
		r, err := s.c.sessions.GetSession(ctx, connect.NewRequest(&maatv1.GetSessionRequest{SessionId: id}))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.c.sessionOf(res.GetSession()), nil
}

// ListSessionsParams 是列出会话的过滤条件。
type ListSessionsParams struct {
	Statuses []SessionStatus
	AgentID  string
	// Metadata 过滤 metadata 中包含这些键值对的会话。
	Metadata map[string]string
	// IncludeArchived 为真时也列出已归档的会话。
	IncludeArchived bool
	// PageSize 是每次请求的条数；0 表示使用平台默认值。
	PageSize uint32
}

// List 按创建时间倒序列出会话，自动翻页。
func (s *Sessions) List(ctx context.Context, p ListSessionsParams) iter.Seq2[*Session, error] {
	filter := &maatv1.SessionFilter{AgentId: p.AgentID, Metadata: p.Metadata, IncludeArchived: p.IncludeArchived}
	for _, st := range p.Statuses {
		filter.Statuses = append(filter.Statuses, sessionStatusToProto[st])
	}
	return func(yield func(*Session, error) bool) {
		token := ""
		for {
			var res *maatv1.ListSessionsResponse
			err := s.c.call(ctx, true, func(ctx context.Context) error {
				r, err := s.c.sessions.ListSessions(ctx, connect.NewRequest(&maatv1.ListSessionsRequest{
					Filter: filter, Page: &maatv1.PageRequest{PageSize: p.PageSize, PageToken: token},
				}))
				if err != nil {
					return err
				}
				res = r.Msg
				return nil
			})
			if err != nil {
				yield(nil, err)
				return
			}
			for _, ps := range res.GetSessions() {
				if !yield(s.c.sessionOf(ps), nil) {
					return
				}
			}
			if token = res.GetPage().GetNextPageToken(); token == "" {
				return
			}
		}
	}
}

var sessionStatusToProto = map[SessionStatus]maatv1.SessionStatus{
	SessionIdle:           maatv1.SessionStatus_SESSION_STATUS_IDLE,
	SessionRunning:        maatv1.SessionStatus_SESSION_STATUS_RUNNING,
	SessionRequiresAction: maatv1.SessionStatus_SESSION_STATUS_REQUIRES_ACTION,
}

// Session 是一个会话。字段是获取时的快照，最新状态用 Sessions.Get 重新读取。
type Session struct {
	ID           string
	AgentID      string
	AgentVersion uint32
	// Model 是模型别名。
	Model    string
	Status   SessionStatus
	Title    string
	Metadata map[string]string
	// PrimaryThreadID 是主线程；Send 不指定线程时发给它。
	PrimaryThreadID string
	// LastSeq 是获取快照时最新已提交事件的 seq。
	LastSeq          uint64
	PendingToolCalls uint32
	Usage            Usage
	Archived         bool
	// ForkedFrom 是 fork 的来源；不是 fork 出的会话时为 nil（spec §8.3）。
	ForkedFrom *ForkOrigin
	CreatedAt  time.Time
	UpdatedAt  time.Time
	// Tools 是本进程为该会话执行的工具实现：Send 返回的 Run 在 Stream / Wait 时自动执行其中的工具调用。
	// Sessions.Create 时设为 CreateSessionParams.Tools；通过 Sessions.Get、List 取得的会话需要自行设置
	// （声明已在创建会话时提交给平台）。
	Tools []Tool

	c *Client
}

// ForkOrigin 是 fork 出的会话的来源：源会话、checkpoint 与 fork 点的 seq。
type ForkOrigin struct {
	SessionID    string
	CheckpointID string
	Seq          uint64
}

func (c *Client) sessionOf(p *maatv1.Session) *Session {
	s := &Session{
		ID: p.GetId(), AgentID: p.GetAgentId(), AgentVersion: p.GetAgentVersion(), Model: p.GetModel(),
		Status: enumOf[SessionStatus](p.GetStatus(), "SESSION_STATUS_"), Title: p.GetTitle(), Metadata: p.GetMetadata(),
		PrimaryThreadID: p.GetPrimaryThreadId(), LastSeq: p.GetLastSeq(), PendingToolCalls: p.GetPendingToolCalls(),
		Usage: usageOf(p.GetUsage()), Archived: p.GetLifecycle() == maatv1.Lifecycle_LIFECYCLE_ARCHIVED,
		CreatedAt: p.GetCreatedAt().AsTime(), UpdatedAt: p.GetUpdatedAt().AsTime(), c: c,
	}
	if f := p.GetForkedFrom(); f != nil {
		s.ForkedFrom = &ForkOrigin{SessionID: f.GetSessionId(), CheckpointID: f.GetCheckpointId(), Seq: f.GetSeq()}
	}
	return s
}

// MessageInput 是发送给 Agent 的消息。Phase 1 只支持文本与 JSON。
type MessageInput struct {
	Parts []ContentPart
}

// ContentPart 是消息中的一段内容：Text 与 JSON 二选一（JSON 非空时使用 JSON）。
type ContentPart struct {
	Text string
	// JSON 是结构化内容，值必须能表示为 JSON（string、float64、bool、nil、[]any、map[string]any 等）。
	JSON map[string]any
}

// TextMessage 构造只有一段文本的消息。
func TextMessage(text string) MessageInput { return MessageInput{Parts: []ContentPart{{Text: text}}} }

func (m MessageInput) toProto() (*maatv1.MessageInput, error) {
	out := &maatv1.MessageInput{Parts: make([]*maatv1.ContentPart, 0, len(m.Parts))}
	for i, p := range m.Parts {
		if p.JSON == nil {
			out.Parts = append(out.Parts, &maatv1.ContentPart{Part: &maatv1.ContentPart_Text{Text: p.Text}})
			continue
		}
		v, err := structpb.NewStruct(p.JSON)
		if err != nil {
			return nil, fmt.Errorf("maat: message part %d is not valid JSON: %w", i, err)
		}
		out.Parts = append(out.Parts, &maatv1.ContentPart{Part: &maatv1.ContentPart_Json{Json: v}})
	}
	return out, nil
}

// SendOption 配置 Send。
type SendOption func(*sendOptions)

type sendOptions struct {
	clientMessageID string
	threadID        string
	model           string
	tools           []Tool
	toolsSet        bool
	interrupt       bool
}

// WithClientMessageID 设置消息的幂等键：重复发送同一 ID 只会投递一次。为空时 SDK 生成一个，
// 保证内部重试不会重复投递。
func WithClientMessageID(id string) SendOption {
	return func(o *sendOptions) { o.clientMessageID = id }
}

// WithThread 把消息发给指定线程（默认发给主线程）。
func WithThread(threadID string) SendOption { return func(o *sendOptions) { o.threadID = threadID } }

// WithModel 从这条消息起把会话的模型别名切换为 alias（之后的消息沿用）。
func WithModel(alias string) SendOption { return func(o *sendOptions) { o.model = alias } }

// WithTools 把本条消息开启的 Run 的工具集替换为 tools（只作用于这个 Run）：声明随消息提交给平台，
// 返回的 Run 自动执行的也是这些工具。消息插入到运行中的 Run 时，平台沿用该 Run 原有的工具集。
func WithTools(tools ...Tool) SendOption {
	return func(o *sendOptions) { o.tools, o.toolsSet = tools, true }
}

// WithInterrupt 先中断线程当前的 Run（结束原因为 interrupted），再以新 Run 处理这条消息。
func WithInterrupt() SendOption { return func(o *sendOptions) { o.interrupt = true } }

// Send 发送一条文本消息，返回处理它的 Run。线程空闲时开启新 Run；线程运行中调用即为插入消息，
// 返回的是正在运行的 Run（Run.Delivery 为 DeliveryInserted）。
func (s *Session) Send(ctx context.Context, text string, opts ...SendOption) (*Run, error) {
	return s.SendInput(ctx, TextMessage(text), opts...)
}

// SendInput 与 Send 相同，但消息可以包含多段文本与 JSON。
func (s *Session) SendInput(ctx context.Context, in MessageInput, opts ...SendOption) (*Run, error) {
	var o sendOptions
	for _, f := range opts {
		f(&o)
	}
	if o.clientMessageID == "" {
		o.clientMessageID = newIdempotencyKey()
	}
	msg, err := in.toProto()
	if err != nil {
		return nil, err
	}
	tools := s.Tools
	var defs []*maatv1.ToolDefinition
	if o.toolsSet {
		tools = o.tools
		if defs, err = toolDefinitions(o.tools); err != nil {
			return nil, err
		}
	}
	req := &maatv1.SendMessageRequest{
		SessionId: s.ID, ThreadId: o.threadID, Message: msg, ClientMessageId: o.clientMessageID, Model: o.model,
		Tools: defs, Interrupt: o.interrupt,
	}
	var res *maatv1.SendMessageResponse
	err = s.c.call(ctx, true, func(ctx context.Context) error {
		r, err := s.c.sessions.SendMessage(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	thread := o.threadID
	if thread == "" {
		thread = s.PrimaryThreadID
	}
	return &Run{
		ID: res.GetRunId(), SessionID: s.ID, ThreadID: thread, MessageID: res.GetMessageId(),
		Delivery: enumOf[Delivery](res.GetDelivery(), "DELIVERY_"), c: s.c, afterSeq: s.LastSeq,
		tools: append([]Tool(nil), tools...),
	}, nil
}

// InterruptOption 配置 Interrupt。
type InterruptOption func(*maatv1.InterruptSessionRequest)

// InterruptThread 中断指定线程（默认中断主线程）。
func InterruptThread(threadID string) InterruptOption {
	return func(r *maatv1.InterruptSessionRequest) { r.ThreadId = threadID }
}

// Interrupt 中断线程当前的 Run：正在输出的文本作为部分消息保留，等待中的工具调用被取消
// （执行中的工具函数的 ctx 随之取消），Run 以 StopInterrupted 结束。线程空闲时什么也不做。
// 中断后立即发送新消息请使用 Send(..., WithInterrupt())。
//
// 中断不是幂等的，出错时不会自动重试。
func (s *Session) Interrupt(ctx context.Context, opts ...InterruptOption) error {
	req := &maatv1.InterruptSessionRequest{SessionId: s.ID}
	for _, f := range opts {
		f(req)
	}
	return s.c.call(ctx, false, func(ctx context.Context) error {
		_, err := s.c.sessions.InterruptSession(ctx, connect.NewRequest(req))
		return err
	})
}

// historyPageSize 是 History 每次请求的条数（平台上限 500）。
const historyPageSize = 200

// HistoryOption 配置 History。
type HistoryOption func(*historyOptions)

type historyOptions struct {
	expandRefs, ancestors bool
	types, threadIDs      []string
}

// ExpandRefs 让平台把只给了预览（或只给了 ref）的内容换成完整内容：user.message 与 agent.message 的正文、
// agent.tool_call 的参数、tool_call.completed 的结果。每条上限 256KB，超出时保持原样（可用 Session.GetBlob 读取）。
func ExpandRefs() HistoryOption { return func(o *historyOptions) { o.expandRefs = true } }

// IncludeAncestors 对 fork 出的会话先返回祖先会话在 fork 点之前的事件（最多 16 层，按"最远的祖先 → 当前会话"
// 的顺序），每条事件的 SessionId 标明来源（spec §8.3）。不能与 afterSeq > 0 同时使用。
func IncludeAncestors() HistoryOption { return func(o *historyOptions) { o.ancestors = true } }

// HistoryTypes 只返回这些类型的事件，例如 "agent.message"。
func HistoryTypes(types ...string) HistoryOption {
	return func(o *historyOptions) { o.types = append(o.types, types...) }
}

// HistoryThreads 只返回这些线程的事件。
func HistoryThreads(threadIDs ...string) HistoryOption {
	return func(o *historyOptions) { o.threadIDs = append(o.threadIDs, threadIDs...) }
}

// ErrAncestorsWithAfterSeq 表示 IncludeAncestors 与 afterSeq > 0 同时使用：谱系链中的位置只能由分页表示。
var ErrAncestorsWithAfterSeq = errors.New("maat: IncludeAncestors cannot be combined with afterSeq > 0")

// History 按 seq 顺序读取 seq > afterSeq 的已提交事件，自动翻页（spec §11.6）。
// 历史中每个 step 只有成功那次 attempt 的内容，不需要对账。已归档的历史由平台透明地从对象存储读取。
func (s *Session) History(ctx context.Context, afterSeq uint64, opts ...HistoryOption) iter.Seq2[RawEvent, error] {
	var o historyOptions
	for _, f := range opts {
		f(&o)
	}
	return func(yield func(RawEvent, error) bool) {
		if o.ancestors && afterSeq > 0 {
			yield(RawEvent{}, ErrAncestorsWithAfterSeq)
			return
		}
		token := ""
		for {
			var res *maatv1.ListSessionEventsResponse
			err := s.c.call(ctx, true, func(ctx context.Context) error {
				r, err := s.c.events.ListSessionEvents(ctx, connect.NewRequest(&maatv1.ListSessionEventsRequest{
					SessionId: s.ID, AfterSeq: afterSeq, Page: &maatv1.PageRequest{PageSize: historyPageSize, PageToken: token},
					ThreadIds: o.threadIDs, Types: o.types, IncludeAncestors: o.ancestors, ExpandRefs: o.expandRefs,
				}))
				if err != nil {
					return err
				}
				res = r.Msg
				return nil
			})
			if err != nil {
				yield(RawEvent{}, err)
				return
			}
			for _, e := range res.GetEvents() {
				if !yield(RawEvent{Event: e}, nil) {
					return
				}
			}
			if token = res.GetPage().GetNextPageToken(); token == "" {
				return
			}
		}
	}
}

// Stream 订阅会话的全部事件：先补齐 AfterSeq（默认 0）之后的历史，再接实时流，断线后自动续传。
// 它产出对账后的高层事件（见 Event），直到 ctx 结束（产出 ctx 的错误）或出现不可重试的错误。
// Session.Stream 不执行工具调用：请使用 Run.Stream，或用 Client.Executor 接入会话。
func (s *Session) Stream(ctx context.Context, opts ...StreamOption) iter.Seq2[Event, error] {
	o := newStreamOptions(opts)
	return func(yield func(Event, error) bool) {
		rec := NewReconciler()
		for raw, err := range s.c.subscribe(ctx, subscription{
			sessionID: s.ID, afterSeq: o.afterSeq, token: o.resumeToken, includeDeltas: !o.noDeltas,
		}) {
			if err != nil {
				yield(nil, err)
				return
			}
			if o.raw && !yield(raw, nil) {
				return
			}
			for _, ev := range rec.Apply(raw.Event) {
				if !yield(ev, nil) {
					return
				}
			}
		}
	}
}
