package maat

import (
	"errors"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// RequestIDHeader 是请求 ID 的 HTTP 头。SDK 为每个请求生成一个，平台在响应中原样返回。
const RequestIDHeader = "X-Request-Id"

// 配置错误：NewClient 不返回错误，缺少配置时每次调用都返回它们。
var (
	ErrNoBaseURL = errors.New("maat: base URL is not set (use WithBaseURL or " + EnvBaseURL + ")")
	ErrNoAPIKey  = errors.New("maat: API key is not set (use WithAPIKey or " + EnvAPIKey + ")")
)

// MaatError 是平台返回的错误（spec §14.4）。用 errors.As 取出：
//
//	var me *maat.MaatError
//	if errors.As(err, &me) && me.Reason == "session_archived" { ... }
type MaatError struct {
	// Code 是 Connect 错误码。
	Code connect.Code
	// Reason 是稳定的机器可读原因（ErrorInfo.reason），例如 "session_archived"。
	// 请求没有到达平台（连接失败、代理错误）时为空。
	Reason string
	// Metadata 是与 Reason 相关的附加信息（ErrorInfo.metadata）。
	Metadata map[string]string
	// Message 是给人看的描述。
	Message string
	// Retryable 表示同一请求稍后重试可能成功。SDK 内部只重试 Retryable 的错误。
	Retryable bool
	// RequestID 是该请求的 X-Request-Id，向平台方反馈问题时请附上。
	RequestID string

	err error
}

// Error 实现 error 接口。
func (e *MaatError) Error() string {
	s := "maat: " + e.Code.String()
	if e.Reason != "" {
		s += " (" + e.Reason + ")"
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.RequestID != "" {
		s += " [request_id=" + e.RequestID + "]"
	}
	return s
}

// Unwrap 返回底层的 *connect.Error。
func (e *MaatError) Unwrap() error { return e.err }

// toError 把 Connect 错误转换为 *MaatError；其他错误（如 context 取消前的本地错误）原样返回。
// requestID 是 SDK 为该请求生成的 ID，响应中带有 X-Request-Id 时以响应为准。
func toError(err error, requestID string) error {
	if err == nil {
		return nil
	}
	var me *MaatError
	if errors.As(err, &me) {
		return err
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	e := &MaatError{Code: ce.Code(), Message: ce.Message(), RequestID: requestID, err: err}
	if id := ce.Meta().Get(RequestIDHeader); id != "" {
		e.RequestID = id
	}
	hasInfo := false
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			continue
		}
		if info, ok := v.(*maatv1.ErrorInfo); ok {
			e.Reason, e.Metadata, hasInfo = info.GetReason(), info.GetMetadata(), true
			break
		}
	}
	e.Retryable = retryable(ce, hasInfo)
	return e
}

// retryable 判断错误是否值得重试：
//   - 客户端合成的错误（不是平台返回的）来自传输层：连接失败、连接中途断开（connect 报告为
//     "incomplete envelope" 等），都是暂时性的；只有取消不重试；
//   - 平台返回的 Unavailable、ResourceExhausted、Aborted、DeadlineExceeded 是暂时性的；
//   - 平台自身的错误总带有 ErrorInfo，没有 ErrorInfo 的 Unknown / Internal 来自中间的代理，也视为暂时性的。
func retryable(ce *connect.Error, hasInfo bool) bool {
	code := ce.Code()
	switch {
	case code == connect.CodeCanceled:
		return false
	case !connect.IsWireError(ce):
		return true
	}
	switch code {
	case connect.CodeUnavailable, connect.CodeResourceExhausted, connect.CodeAborted, connect.CodeDeadlineExceeded:
		return true
	case connect.CodeUnknown, connect.CodeInternal:
		return !hasInfo
	default:
		return false
	}
}
