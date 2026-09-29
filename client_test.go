package maat

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
	"github.com/bootun/maat-go/gen/maat/v1/maatv1connect"
)

func TestNewClientConfig(t *testing.T) {
	t.Setenv(EnvBaseURL, "")
	t.Setenv(EnvAPIKey, "")
	ctx := context.Background()
	tests := []struct {
		name string
		opts []Option
		want string
	}{
		{"缺少地址", []Option{WithAPIKey("k")}, ErrNoBaseURL.Error()},
		{"缺少 Key", []Option{WithBaseURL("http://localhost:8080")}, ErrNoAPIKey.Error()},
		{"不是 http(s)", []Option{WithBaseURL("ftp://example.com"), WithAPIKey("k")}, "invalid base URL"},
		{"相对地址", []Option{WithBaseURL("localhost:8080"), WithAPIKey("k")}, "invalid base URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.opts...)
			if _, err := c.WhoAmI(ctx); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			// 事件流同样返回配置错误。
			for _, err := range c.subscribe(ctx, subscription{sessionID: "ses_1"}) {
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("subscribe err = %v", err)
				}
			}
		})
	}
}

func TestNewClientReadsEnvironment(t *testing.T) {
	auth := make(chan string, 1)
	f := &fakeBackend{whoAmI: func(_ context.Context, req *connect.Request[maatv1.WhoAmIRequest]) (*connect.Response[maatv1.WhoAmIResponse], error) {
		auth <- req.Header().Get("Authorization")
		return connect.NewResponse(&maatv1.WhoAmIResponse{
			Kind: maatv1.PrincipalKind_PRINCIPAL_KIND_API_KEY, Id: "key_1", Name: "sdk", OrgId: "org_1", ProjectId: "prj_1",
			Scopes: []string{"sessions:read"},
		}), nil
	}}
	mux := http.NewServeMux()
	mux.Handle(maatv1connect.NewMetaServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv(EnvBaseURL, srv.URL+"/")
	t.Setenv(EnvAPIKey, "env-key")

	me, err := NewClient().WhoAmI(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := Identity{Kind: PrincipalAPIKey, ID: "key_1", Name: "sdk", OrgID: "org_1", ProjectID: "prj_1", Scopes: []string{"sessions:read"}}
	if me.Kind != want.Kind || me.ID != want.ID || me.ProjectID != want.ProjectID || len(me.Scopes) != 1 {
		t.Fatalf("identity = %+v", me)
	}
	if a := <-auth; a != "Bearer env-key" {
		t.Fatalf("authorization = %q", a)
	}
}

func TestCallRetriesRetryableErrors(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	attempts := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), ids...)
	}
	f := &fakeBackend{whoAmI: func(_ context.Context, req *connect.Request[maatv1.WhoAmIRequest]) (*connect.Response[maatv1.WhoAmIResponse], error) {
		mu.Lock()
		defer mu.Unlock()
		ids = append(ids, req.Header().Get(RequestIDHeader))
		if len(ids) < 3 {
			return nil, platformError(connect.CodeUnavailable, "unavailable", "try again")
		}
		return connect.NewResponse(&maatv1.WhoAmIResponse{Id: "key_1"}), nil
	}}
	c := newTestClient(t, f)
	if _, err := c.WhoAmI(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := attempts(); len(got) != 3 || got[0] == got[1] || got[1] == got[2] {
		t.Fatalf("request ids = %v", got)
	}

	// 超过最大尝试次数后返回最后一次的错误。
	mu.Lock()
	ids = nil
	mu.Unlock()
	c.cfg.maxAttempts = 2
	_, err := c.WhoAmI(context.Background())
	var me *MaatError
	if !errors.As(err, &me) || me.Code != connect.CodeUnavailable || !me.Retryable || len(attempts()) != 2 {
		t.Fatalf("err = %v, attempts = %d", err, len(attempts()))
	}
}

func TestCallDoesNotRetryPermanentErrors(t *testing.T) {
	var n atomic.Int32
	f := &fakeBackend{whoAmI: func(context.Context, *connect.Request[maatv1.WhoAmIRequest]) (*connect.Response[maatv1.WhoAmIResponse], error) {
		n.Add(1)
		err := platformError(connect.CodeUnauthenticated, "invalid_api_key", "invalid API key")
		err.Meta().Set(RequestIDHeader, "req-from-server")
		return nil, err
	}}
	c := newTestClient(t, f)
	_, err := c.WhoAmI(context.Background())
	var me *MaatError
	if !errors.As(err, &me) {
		t.Fatalf("err = %v", err)
	}
	want := MaatError{Code: connect.CodeUnauthenticated, Reason: "invalid_api_key", Message: "invalid API key", RequestID: "req-from-server"}
	if me.Code != want.Code || me.Reason != want.Reason || me.Message != want.Message || me.RequestID != want.RequestID ||
		me.Retryable || me.Metadata["k"] != "v" || n.Load() != 1 {
		t.Fatalf("err = %#v, attempts = %d", me, n.Load())
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatal("MaatError must unwrap to *connect.Error")
	}
	if s := err.Error(); s != "maat: unauthenticated (invalid_api_key): invalid API key [request_id=req-from-server]" {
		t.Fatalf("Error() = %q", s)
	}
}

func TestToError(t *testing.T) {
	plain := errors.New("local")
	if !errors.Is(toError(plain, "req_1"), plain) {
		t.Fatal("non-Connect errors must be returned unchanged")
	}
	// 客户端合成的错误（连接中途断开等）来自传输层，可以重试；取消不重试。
	var me *MaatError
	if !errors.As(toError(connect.NewError(connect.CodeInvalidArgument, errors.New("incomplete envelope")), "req_1"), &me) ||
		!me.Retryable || me.RequestID != "req_1" {
		t.Fatalf("transport error = %#v", me)
	}
	if !errors.As(toError(connect.NewError(connect.CodeCanceled, context.Canceled), "req_1"), &me) || me.Retryable {
		t.Fatalf("canceled = %#v", me)
	}

	// 平台返回的错误按错误码与 ErrorInfo 判断。
	tests := []struct {
		name      string
		err       *connect.Error
		retryable bool
	}{
		{"代理返回的 Unknown（没有 ErrorInfo）", connect.NewError(connect.CodeUnknown, errors.New("bad gateway")), true},
		{"平台的 Internal", platformError(connect.CodeInternal, "internal", "internal error"), false},
		{"Unavailable", platformError(connect.CodeUnavailable, "unavailable", "try later"), true},
		{"ResourceExhausted", platformError(connect.CodeResourceExhausted, "rate_limited", "slow down"), true},
		{"FailedPrecondition", platformError(connect.CodeFailedPrecondition, "session_archived", "archived"), false},
		{"InvalidArgument", platformError(connect.CodeInvalidArgument, "invalid_metadata", "bad key"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeBackend{whoAmI: func(context.Context, *connect.Request[maatv1.WhoAmIRequest]) (*connect.Response[maatv1.WhoAmIResponse], error) {
				return nil, tt.err
			}}
			c := newTestClient(t, f)
			c.cfg.maxAttempts = 1
			_, err := c.WhoAmI(context.Background())
			var me *MaatError
			if !errors.As(err, &me) || me.Code != tt.err.Code() || me.Retryable != tt.retryable || !strings.HasPrefix(me.RequestID, "req_") {
				t.Fatalf("err = %#v", me)
			}
		})
	}
}
