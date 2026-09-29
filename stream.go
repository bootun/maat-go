package maat

import (
	"context"
	"errors"
	"iter"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// resumeTokenTrailer 是事件流因错误结束时携带最后续传位置的 trailer。
const resumeTokenTrailer = "Maat-Resume-Token"

// StreamOption 配置 Run.Stream 与 Session.Stream。
type StreamOption func(*streamOptions)

type streamOptions struct {
	raw           bool
	noDeltas      bool
	noAutoExecute bool
	afterSeq      uint64
	resumeToken   string
	// executor 是 Run.Stream 内部创建的执行器。
	executor *executor
}

func newStreamOptions(opts []StreamOption) streamOptions {
	var o streamOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}

// WithRawEvents 让 Stream 在高层事件之外，对每条原始事件先产出一个 RawEvent（包括心跳等流控制事件）。
func WithRawEvents() StreamOption { return func(o *streamOptions) { o.raw = true } }

// WithoutDeltas 不接收瞬时事件（delta、marker），只处理已提交事件：TextEvent 只在 step 提交时产出一次。
func WithoutDeltas() StreamOption { return func(o *streamOptions) { o.noDeltas = true } }

// WithAutoExecute 设置 Run.Stream（以及 Run.Wait）是否自动执行工具调用，默认开启。
// 关闭后工具调用需要由其他执行器（例如 Client.Executor）处理。Session.Stream 从不执行工具调用。
func WithAutoExecute(on bool) StreamOption { return func(o *streamOptions) { o.noAutoExecute = !on } }

// AfterSeq 让 Session.Stream 从 seq > n 的已提交事件开始（默认 0，即先补齐全部历史）。
// 通常与 Session.History 配合：先读历史，再用最后一条的 seq 订阅实时流。Run.Stream 忽略该选项。
func AfterSeq(n uint64) StreamOption { return func(o *streamOptions) { o.afterSeq = n } }

// WithResumeToken 从某个续传位置（RawEvent.ResumeToken）继续订阅，例如进程重启后接着渲染。
// 同时设置 AfterSeq 时以续传位置为准。
func WithResumeToken(token string) StreamOption {
	return func(o *streamOptions) { o.resumeToken = token }
}

// subscription 是一次会话事件订阅的参数。
type subscription struct {
	sessionID     string
	afterSeq      uint64
	token         string
	includeDeltas bool
}

// subscribe 订阅会话事件流，断线后自动续传（spec §11.4、§14.4）：
//   - 携带最新的 resume token 重连，指数退避 0.5s → 10s，收到事件后退避重置；
//   - 收到 closing 心跳（服务端即将关闭）时立即重连；如果它是新连接上的第一条事件（服务端仍在关闭中），
//     按退避处理，避免空转；
//   - 已提交事件按 seq 去重，保证每条只产出一次；
//   - 不可重试的错误（认证失败、无权限、会话不存在等）产出后结束。
//
// 它只在 ctx 结束或出现不可重试的错误时结束，结束前产出该错误。
func (c *Client) subscribe(ctx context.Context, sub subscription) iter.Seq2[RawEvent, error] {
	return func(yield func(RawEvent, error) bool) {
		if c.err != nil {
			yield(RawEvent{}, c.err)
			return
		}
		token, lastSeq, failures := sub.token, sub.afterSeq, 0
		for {
			id := newRequestID()
			sctx, cancel := context.WithCancel(withRequestID(ctx, id))
			stream, err := c.events.StreamSessionEvents(sctx, connect.NewRequest(&maatv1.StreamSessionEventsRequest{
				SessionId: sub.sessionID, ResumeToken: token, AfterSeq: lastSeq, IncludeDeltas: &sub.includeDeltas,
			}))
			closing, stopped, received := false, false, 0
			if err == nil {
				for stream.Receive() {
					msg := stream.Msg()
					received++
					if t := msg.GetResumeToken(); t != "" {
						token = t
					}
					ev := msg.GetEvent()
					if seq := ev.GetSeq(); seq > 0 {
						if seq <= lastSeq {
							continue
						}
						lastSeq = seq
					}
					if !yield(RawEvent{Event: ev, ResumeToken: msg.GetResumeToken()}, nil) {
						stopped = true
						break
					}
					if ev.GetStreamHeartbeat().GetClosing() {
						closing = true
						break
					}
					failures = 0
				}
				err = stream.Err()
				if t := stream.ResponseTrailer().Get(resumeTokenTrailer); t != "" {
					token = t
				}
				cancel()       // 先取消，Close 不再等待服务端结束
				stream.Close() // 主动关闭或已经出错的流，关闭错误没有意义
			}
			cancel()
			switch {
			case stopped:
				return
			case ctx.Err() != nil:
				yield(RawEvent{}, ctx.Err())
				return
			case closing && received > 1:
				continue // 服务端即将关闭，立即换一个连接
			}
			if err != nil {
				err = toError(err, id)
				if me := (*MaatError)(nil); !errors.As(err, &me) || !me.Retryable {
					yield(RawEvent{}, err)
					return
				}
			}
			// 可重试的错误，或服务端意外地正常结束了流：退避后重连。
			if sleep(ctx, c.cfg.reconnect.delay(failures)) != nil {
				yield(RawEvent{}, ctx.Err())
				return
			}
			failures++
		}
	}
}
