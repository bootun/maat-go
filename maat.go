// Package maat 是 maat 平台的 Go SDK，只面向服务端使用（平台 API Key 不得下发到浏览器）。
//
//	c := maat.NewClient() // 读取 MAAT_BASE_URL、MAAT_API_KEY
//	s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: "agt_..."})
//	if err != nil { ... }
//	run, err := s.Send(ctx, "总结一下 README")
//	if err != nil { ... }
//	for ev, err := range run.Stream(ctx) {
//		if err != nil { ... }
//		switch e := ev.(type) {
//		case maat.TextEvent:        // e.Text 是该 step 已对账的完整文本
//		case maat.StepRewoundEvent: // 该 step 发生了重试，清空已渲染的内容
//		}
//	}
//	res, err := run.Wait(ctx)
//
// 事件流在断线后自动续传；Stream 产出的是按 spec §11.5 对账后的高层事件，
// 原始事件可以通过 WithRawEvents 一并取得。
//
// 调用方工具用 NewTool 声明（CreateSessionParams.Tools、WithTools），Run.Stream 与 Run.Wait 自动认领、执行、
// 续约并回传；只执行工具、不发送消息的进程使用 Client.Executor（Executor 模式）。
package maat

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
	"github.com/bootun/maat-go/gen/maat/v1/maatv1connect"
)

// Version 是 SDK 的版本，写在 User-Agent 中。
const Version = "0.3.0"

// Client 是 maat 平台的客户端，可以并发使用。
type Client struct {
	// Sessions 管理会话。
	Sessions *Sessions
	// Checkpoints 列出与标注 checkpoint（spec §8）。
	Checkpoints *Checkpoints

	cfg config
	// err 是配置错误；非空时每次调用都返回它。
	err      error
	meta     maatv1connect.MetaServiceClient
	sessions maatv1connect.SessionServiceClient
	events   maatv1connect.EventServiceClient
	tools    maatv1connect.ToolServiceClient
	blobs    maatv1connect.BlobServiceClient
	// executorID 标识本 Client 的工具执行器（exe_ 前缀），用于认领与回传。
	executorID string
}

// NewClient 构造 Client。未通过 Option 设置的地址与 API Key 从环境变量 MAAT_BASE_URL、MAAT_API_KEY 读取；
// 缺少或无效时不会立即报错，而是在每次调用时返回 ErrNoBaseURL / ErrNoAPIKey 等错误。
func NewClient(opts ...Option) *Client {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.baseURL == "" {
		cfg.baseURL = os.Getenv(EnvBaseURL)
	}
	if cfg.apiKey == "" {
		cfg.apiKey = os.Getenv(EnvAPIKey)
	}
	c := &Client{cfg: cfg, executorID: "exe_" + rand.Text()}
	c.Sessions = &Sessions{c: c}
	c.Checkpoints = &Checkpoints{c: c}
	base, err := normalizeBaseURL(cfg.baseURL)
	if err == nil && cfg.apiKey == "" {
		err = ErrNoAPIKey
	}
	c.err = err
	ic := connect.WithInterceptors(headers{apiKey: cfg.apiKey, userAgent: "maat-go/" + Version})
	c.meta = maatv1connect.NewMetaServiceClient(cfg.httpClient, base, ic)
	c.sessions = maatv1connect.NewSessionServiceClient(cfg.httpClient, base, ic)
	c.events = maatv1connect.NewEventServiceClient(cfg.httpClient, base, ic)
	c.tools = maatv1connect.NewToolServiceClient(cfg.httpClient, base, ic)
	c.blobs = maatv1connect.NewBlobServiceClient(cfg.httpClient, base, ic)
	return c
}

// normalizeBaseURL 校验平台地址并去掉末尾的 "/"。
func normalizeBaseURL(raw string) (string, error) {
	if raw == "" {
		return "", ErrNoBaseURL
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("maat: invalid base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errors.New("maat: invalid base URL: must be an absolute http(s) URL")
	}
	return strings.TrimRight(raw, "/"), nil
}

// PrincipalKind 是调用者的类型。
type PrincipalKind string

// 调用者类型。
const (
	PrincipalAPIKey      PrincipalKind = "api_key"
	PrincipalConsoleUser PrincipalKind = "console_user"
)

// Identity 是当前调用者的身份。
type Identity struct {
	Kind PrincipalKind
	// ID 是 API Key ID（key_…）或用户 ID（usr_…）。
	ID   string
	Name string
	// OrgID 是所属组织。
	OrgID string
	// ProjectID 是 API Key 绑定的项目。
	ProjectID string
	// Scopes 是 API Key 的权限，例如 "sessions:write"。
	Scopes []string
}

// WhoAmI 返回当前 API Key 的身份，可用作连通性检查。
func (c *Client) WhoAmI(ctx context.Context) (*Identity, error) {
	var res *maatv1.WhoAmIResponse
	err := c.call(ctx, true, func(ctx context.Context) error {
		r, err := c.meta.WhoAmI(ctx, connect.NewRequest(&maatv1.WhoAmIRequest{}))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Identity{
		Kind: enumOf[PrincipalKind](res.GetKind(), "PRINCIPAL_KIND_"), ID: res.GetId(), Name: res.GetName(),
		OrgID: res.GetOrgId(), ProjectID: res.GetProjectId(), Scopes: res.GetScopes(),
	}, nil
}

// call 执行一次一元调用：为每次尝试生成请求 ID，把错误转换为 *MaatError；
// idempotent 为真时对 Retryable 的错误按退避重试，最多 maxAttempts 次。
// 有副作用的调用只有带幂等键（client_message_id、idempotency_key）时才能标记为 idempotent。
func (c *Client) call(ctx context.Context, idempotent bool, fn func(context.Context) error) error {
	if c.err != nil {
		return c.err
	}
	for attempt := 0; ; attempt++ {
		id := newRequestID()
		err := toError(fn(withRequestID(ctx, id)), id)
		if err == nil {
			return nil
		}
		var me *MaatError
		if !idempotent || attempt+1 >= c.cfg.maxAttempts || !errors.As(err, &me) || !me.Retryable || ctx.Err() != nil {
			return err
		}
		if serr := sleep(ctx, c.cfg.retry.delay(attempt)); serr != nil {
			return err
		}
	}
}

// sleep 等待 d，ctx 结束时提前返回 ctx 的错误。
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type requestIDKey struct{}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// newRequestID 生成请求 ID（可打印 ASCII，服务端在响应头与日志中原样使用）。
func newRequestID() string { return "req_" + rand.Text() }

// newIdempotencyKey 生成幂等键，保证内部重试不会重复创建会话或发送消息。
func newIdempotencyKey() string { return "sdk_" + rand.Text() }

// headers 给一元与流式请求加上认证、User-Agent 与请求 ID。
// （connect.UnaryInterceptorFunc 不作用于流式调用，所以实现完整的 Interceptor。）
type headers struct {
	apiKey    string
	userAgent string
}

func (h headers) set(ctx context.Context, hdr http.Header) {
	hdr.Set("Authorization", "Bearer "+h.apiKey)
	hdr.Set("User-Agent", h.userAgent)
	id, _ := ctx.Value(requestIDKey{}).(string)
	if id == "" {
		id = newRequestID()
	}
	hdr.Set(RequestIDHeader, id)
}

func (h headers) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		h.set(ctx, req.Header())
		return next(ctx, req)
	}
}

func (h headers) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		h.set(ctx, conn.RequestHeader())
		return conn
	}
}

func (h headers) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// enumOf 把 proto 枚举转换为 SDK 的字符串常量：去掉前缀并转为小写（"SESSION_STATUS_IDLE" → "idle"），
// UNSPECIFIED 转为空串。平台新增的取值同样按此规则转换，保持向前兼容。
func enumOf[T ~string](e fmt.Stringer, prefix string) T {
	s := strings.TrimPrefix(e.String(), prefix)
	if s == "UNSPECIFIED" {
		return ""
	}
	return T(strings.ToLower(s))
}
