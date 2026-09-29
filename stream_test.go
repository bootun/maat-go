package maat

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// collect 读取订阅，直到 stop 返回真、出错或 ctx 结束。
func collect(t *testing.T, it func(func(RawEvent, error) bool), stop func(RawEvent) bool) ([]RawEvent, error) {
	t.Helper()
	var got []RawEvent
	var err error
	it(func(e RawEvent, e2 error) bool {
		if e2 != nil {
			err = e2
			return false
		}
		got = append(got, e)
		return !stop(e)
	})
	return got, err
}

func types(evts []RawEvent) string {
	out := make([]string, 0, len(evts))
	for _, e := range evts {
		out = append(out, e.GetType())
	}
	return strings.Join(out, ",")
}

func TestSubscribeResumesWithTokenAfterError(t *testing.T) {
	f := &fakeBackend{}
	f.conns = append(f.conns,
		func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			if err := send(st, "t1", message(1, "run_1", "s1", "a")); err != nil {
				return err
			}
			if err := send(st, "t2", delta("run_1", "s2", 1, "b")); err != nil {
				return err
			}
			st.ResponseTrailer().Set(resumeTokenTrailer, "t2")
			return connect.NewError(connect.CodeUnavailable, errors.New("subscriber lagged"))
		},
		func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			// 服务端重放了 seq 1：客户端按 seq 去重。
			for _, e := range []*maatv1.Event{message(1, "run_1", "s1", "a"), message(2, "run_1", "s2", "bc"), runCompleted(3, "run_1")} {
				if err := send(st, "t3", e); err != nil {
					return err
				}
			}
			return nil
		},
	)
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := collect(t, c.subscribe(ctx, subscription{sessionID: "ses_1", includeDeltas: true}),
		func(e RawEvent) bool { return e.GetType() == "run.completed" })
	if err != nil {
		t.Fatal(err)
	}
	if want := "agent.message,agent.message.delta,agent.message,run.completed"; types(got) != want {
		t.Fatalf("events = %s, want %s", types(got), want)
	}
	reqs := f.streams()
	if len(reqs) != 2 {
		t.Fatalf("connections = %d", len(reqs))
	}
	if reqs[1].GetResumeToken() != "t2" || reqs[1].GetAfterSeq() != 1 || !reqs[1].GetIncludeDeltas() {
		t.Fatalf("resume request = %v", reqs[1])
	}
	if got[0].ResumeToken != "t1" || got[1].ResumeToken != "t2" {
		t.Fatalf("tokens = %q, %q", got[0].ResumeToken, got[1].ResumeToken)
	}
}

func TestSubscribeReconnectsImmediatelyOnClosingHeartbeat(t *testing.T) {
	f := &fakeBackend{}
	f.conns = append(f.conns,
		func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			if err := send(st, "t1", message(1, "run_1", "s1", "x")); err != nil {
				return err
			}
			return send(st, "t2", heartbeat("t2", true))
		},
		func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			return send(st, "t3", message(2, "run_1", "s2", "y"))
		},
	)
	c := newTestClient(t, f)
	// 如果 closing 之后还要退避，这里会等上一小时，测试超时失败。
	c.cfg.reconnect = backoff{min: time.Hour, max: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := collect(t, c.subscribe(ctx, subscription{sessionID: "ses_1"}),
		func(e RawEvent) bool { return e.GetSeq() == 2 })
	if err != nil {
		t.Fatal(err)
	}
	if types(got) != "agent.message,stream.heartbeat,agent.message" {
		t.Fatalf("events = %s", types(got))
	}
	if reqs := f.streams(); len(reqs) != 2 || reqs[1].GetResumeToken() != "t2" {
		t.Fatalf("requests = %v", reqs)
	}
}

func TestSubscribeBacksOffWhenNewConnectionIsClosing(t *testing.T) {
	f := &fakeBackend{}
	for range 3 {
		f.conns = append(f.conns, func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			return send(st, "t1", heartbeat("t1", true))
		})
	}
	c := newTestClient(t, f)
	c.cfg.reconnect = backoff{min: 300 * time.Millisecond, max: 300 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := collect(t, c.subscribe(ctx, subscription{sessionID: "ses_1"}), func(RawEvent) bool { return false })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	// 500ms 内最多两个连接：第二个连接之前有 300ms 的退避。
	if n := len(f.streams()); n != 2 {
		t.Fatalf("connections = %d", n)
	}
}

func TestSubscribeRetriesTransportFailure(t *testing.T) {
	f := &fakeBackend{}
	f.conns = append(f.conns,
		func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			if err := send(st, "t1", message(1, "run_1", "s1", "x")); err != nil {
				return err
			}
			panic(http.ErrAbortHandler) // 连接在流中途断开
		},
		func(_ context.Context, st *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
			return send(st, "t2", message(2, "run_1", "s2", "y"))
		},
	)
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := collect(t, c.subscribe(ctx, subscription{sessionID: "ses_1"}),
		func(e RawEvent) bool { return e.GetSeq() == 2 })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("events = %s", types(got))
	}
	if reqs := f.streams(); len(reqs) != 2 || reqs[1].GetResumeToken() != "t1" {
		t.Fatalf("requests = %v", reqs)
	}
}

func TestSubscribeStopsOnPermanentError(t *testing.T) {
	f := &fakeBackend{}
	f.conns = append(f.conns, func(context.Context, *connect.ServerStream[maatv1.StreamSessionEventsResponse]) error {
		return platformError(connect.CodeNotFound, "session_not_found", "session ses_1 not found")
	})
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := collect(t, c.subscribe(ctx, subscription{sessionID: "ses_1"}), func(RawEvent) bool { return false })
	var me *MaatError
	if !errors.As(err, &me) || me.Code != connect.CodeNotFound || me.Reason != "session_not_found" || me.Retryable {
		t.Fatalf("err = %#v", err)
	}
	if me.RequestID == "" || !strings.HasPrefix(me.RequestID, "req_") {
		t.Fatalf("request id = %q", me.RequestID)
	}
	if n := len(f.streams()); n != 1 {
		t.Fatalf("connections = %d", n)
	}
}

func TestSubscribeEndsWithContextError(t *testing.T) {
	f := &fakeBackend{} // 连接一直保持
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := collect(t, c.subscribe(ctx, subscription{sessionID: "ses_1"}), func(RawEvent) bool { return false })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	hdr := f.header(0)
	if hdr.Get("Authorization") != "Bearer test-key" || !strings.HasPrefix(hdr.Get("User-Agent"), "maat-go/") ||
		!strings.HasPrefix(hdr.Get(RequestIDHeader), "req_") {
		t.Fatalf("headers = %v", hdr)
	}
}

func TestBackoff(t *testing.T) {
	b := backoff{min: 500 * time.Millisecond, max: 10 * time.Second}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for n, w := range want {
		if got := b.delay(n); got != w {
			t.Fatalf("delay(%d) = %v, want %v", n, got, w)
		}
	}
	b.jitter = true
	for n := range 20 {
		if got, base := b.delay(n), (backoff{min: b.min, max: b.max}).delay(n); got < base*8/10 || got >= base*12/10 {
			t.Fatalf("jittered delay(%d) = %v, base %v", n, got, base)
		}
	}
}

// platformError 构造平台风格的错误（带 ErrorInfo detail）。
func platformError(code connect.Code, reason, msg string) *connect.Error {
	err := connect.NewError(code, errors.New(msg))
	if d, derr := connect.NewErrorDetail(&maatv1.ErrorInfo{Reason: reason, Metadata: map[string]string{"k": "v"}}); derr == nil {
		err.AddDetail(d)
	}
	return err
}
