//go:build integration

package maat_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/bootun/maat-go"
	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
	"github.com/bootun/maat-go/internal/testenv"
)

// 集成测试针对后端的 compose 环境运行（见 internal/testenv）：
//
//	(cd ../maat && make up)
//	MAAT_E2E_ENV_FILE=../maat/deploy/.env.e2e go test -tags=integration ./...

func newClient(e testenv.Env, opts ...maat.Option) *maat.Client {
	return maat.NewClient(append([]maat.Option{maat.WithBaseURL(e.BaseURL), maat.WithAPIKey(e.SDKKey)}, opts...)...)
}

func TestIntegration_WhoAmI(t *testing.T) {
	e := testenv.Load(t)
	me, err := newClient(e).WhoAmI(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if me.Kind != maat.PrincipalAPIKey || me.ProjectID == "" || !slices.Contains(me.Scopes, "sessions:write") {
		t.Fatalf("identity = %+v", me)
	}
}

func TestIntegration_SendStreamWait(t *testing.T) {
	e := testenv.Load(t)
	const want = "你好，世界。这是 SDK 集成测试。"
	agent := e.NewAgent(t, "script", testenv.Script(`{"text":"`+want+`","chunk_size":4,"delay_ms":20}`))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := newClient(e)
	tag := time.Now().Format(time.RFC3339Nano)
	s, err := c.Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent, Title: "sdk it", Metadata: map[string]string{"sdk_it": tag}})
	if err != nil {
		t.Fatal(err)
	}
	if s.Status != maat.SessionIdle || s.PrimaryThreadID == "" {
		t.Fatalf("session = %+v", s)
	}
	run, err := s.Send(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if run.Delivery != maat.DeliveryNewRun || run.ID == "" {
		t.Fatalf("run = %+v", run)
	}
	var final string
	completed := false
	for ev, err := range run.Stream(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		switch ev := ev.(type) {
		case maat.TextEvent:
			if ev.RunID != run.ID {
				t.Fatalf("event of another run: %+v", ev)
			}
			if ev.Final {
				final = ev.Text
			}
		case maat.RunCompletedEvent:
			completed = ev.StopReason == maat.StopEndTurn
		}
	}
	if final != want || !completed {
		t.Fatalf("final = %q, completed = %v", final, completed)
	}
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != want || res.StopReason != maat.StopEndTurn || res.Error != nil {
		t.Fatalf("result = %+v", res)
	}

	var types []string
	for ev, err := range s.History(ctx, 0) {
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, ev.GetType())
	}
	for _, typ := range []string{"session.created", "user.message", "agent.message", "run.completed"} {
		if !slices.Contains(types, typ) {
			t.Fatalf("history %v lacks %s", types, typ)
		}
	}
	got, err := c.Sessions.Get(ctx, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSeq <= s.LastSeq {
		t.Fatalf("last seq %d -> %d", s.LastSeq, got.LastSeq)
	}
	var ids []string
	for ls, err := range c.Sessions.List(ctx, maat.ListSessionsParams{Metadata: map[string]string{"sdk_it": tag}}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ls.ID)
	}
	if !slices.Equal(ids, []string{s.ID}) {
		t.Fatalf("listed = %v", ids)
	}
}

func TestIntegration_InsertMessageDuringRun(t *testing.T) {
	e := testenv.Load(t)
	agent := e.NewAgent(t, "script", testenv.Script(
		`{"text":"`+strings.Repeat("这是一段较慢的输出。", 6)+`","chunk_size":2,"delay_ms":60}`,
		`{"text":"echo: {{last_user}}"}`,
	))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := newClient(e).Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "开始")
	if err != nil {
		t.Fatal(err)
	}
	var inserted *maat.Run
	finals := 0
	for ev, err := range run.Stream(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		te, ok := ev.(maat.TextEvent)
		if !ok {
			continue
		}
		if te.Final {
			finals++
			continue
		}
		if inserted == nil {
			if inserted, err = s.Send(ctx, "插入的消息"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if inserted == nil || inserted.Delivery != maat.DeliveryInserted || inserted.ID != run.ID {
		t.Fatalf("inserted = %+v, run %s", inserted, run.ID)
	}
	res, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if finals != 2 || res.Text != "echo: 插入的消息" || res.StopReason != maat.StopEndTurn {
		t.Fatalf("finals = %d, result = %+v", finals, res)
	}
}

func TestIntegration_ResumeAfterDisconnect(t *testing.T) {
	e := testenv.Load(t)
	want := strings.Repeat("续传测试。", 10)
	agent := e.NewAgent(t, "script", testenv.Script(`{"text":"`+want+`","chunk_size":3,"delay_ms":40}`))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cut := &cutTransport{base: http.DefaultTransport}
	s, err := newClient(e, maat.WithHTTPClient(&http.Client{Transport: cut})).Sessions.Create(ctx, maat.CreateSessionParams{Agent: agent})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Send(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	var final string
	for ev, err := range run.Stream(ctx, maat.WithRawEvents()) {
		if err != nil {
			t.Fatal(err)
		}
		switch ev := ev.(type) {
		case maat.RawEvent:
			if seq := ev.GetSeq(); seq > 0 {
				seqs = append(seqs, seq)
			}
		case maat.TextEvent:
			if ev.Final {
				final = ev.Text
			}
		}
	}
	if n := cut.streams(); n < 2 {
		t.Fatalf("stream connections = %d, want a reconnect", n)
	}
	if final != want {
		t.Fatalf("final = %q", final)
	}
	// 断线前后收到的已提交事件与历史一致：不重复、不遗漏。
	var history []uint64
	for ev, err := range s.History(ctx, s.LastSeq) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.GetRunId() == run.ID {
			history = append(history, ev.GetSeq())
		}
		if ev.GetType() == "run.completed" && ev.GetRunId() == run.ID {
			break
		}
	}
	if !slices.Equal(seqs, history) {
		t.Fatalf("streamed seqs %v, history %v", seqs, history)
	}
}

// cutTransport 让第一个事件流响应在放行第一条 agent.message.delta 之后中断，模拟网络断开。
type cutTransport struct {
	base http.RoundTripper

	mu sync.Mutex
	n  int
}

func (c *cutTransport) streams() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *cutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := c.base.RoundTrip(req)
	if err != nil || !strings.HasSuffix(req.URL.Path, "/StreamSessionEvents") {
		return res, err
	}
	c.mu.Lock()
	c.n++
	first := c.n == 1
	c.mu.Unlock()
	if first {
		res.Body = &cutBody{rc: res.Body}
	}
	return res, nil
}

// cutBody 按 Connect 的流式帧（1 字节标志 + 4 字节长度 + 消息）转发响应体。
type cutBody struct {
	rc   io.ReadCloser
	buf  bytes.Buffer
	done bool
}

func (b *cutBody) Read(p []byte) (int, error) {
	for b.buf.Len() == 0 {
		if b.done {
			return 0, io.ErrUnexpectedEOF
		}
		var hdr [5]byte
		if _, err := io.ReadFull(b.rc, hdr[:]); err != nil {
			return 0, err
		}
		msg := make([]byte, binary.BigEndian.Uint32(hdr[1:]))
		if _, err := io.ReadFull(b.rc, msg); err != nil {
			return 0, err
		}
		b.buf.Write(hdr[:])
		b.buf.Write(msg)
		if hdr[0]&0x02 == 0 && isDelta(hdr[0]&0x01 != 0, msg) {
			b.done = true
		}
	}
	return b.buf.Read(p)
}

func (b *cutBody) Close() error { return b.rc.Close() }

func isDelta(compressed bool, msg []byte) bool {
	if compressed {
		zr, err := gzip.NewReader(bytes.NewReader(msg))
		if err != nil {
			return false
		}
		if msg, err = io.ReadAll(zr); err != nil {
			return false
		}
	}
	var res maatv1.StreamSessionEventsResponse
	return proto.Unmarshal(msg, &res) == nil && res.GetEvent().GetAgentMessageDelta() != nil
}
