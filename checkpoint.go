package maat

import (
	"context"
	"iter"
	"time"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// CheckpointKind 是 checkpoint 的粒度（spec §8.1）。
type CheckpointKind string

// checkpoint 的粒度：每个稳定点都有 step 级 checkpoint，Run 结束时最后一个稳定的升级为 turn。
const (
	CheckpointStep CheckpointKind = "step"
	CheckpointTurn CheckpointKind = "turn"
)

var checkpointKindToProto = map[CheckpointKind]maatv1.CheckpointKind{
	CheckpointStep: maatv1.CheckpointKind_CHECKPOINT_KIND_STEP,
	CheckpointTurn: maatv1.CheckpointKind_CHECKPOINT_KIND_TURN,
}

// Checkpoint 是指向某一时刻线程上下文的指针（spec §8）。只有 Stable 的 checkpoint 可以 fork。
type Checkpoint struct {
	ID        string
	SessionID string
	ThreadID  string
	RunID     string
	// Seq 是 checkpoint 对应的会话事件位置。
	Seq    uint64
	Kind   CheckpointKind
	Stable bool
	// ContextRoot 是上下文快照的 ref。
	ContextRoot string
	// ExecutorStateRef 是执行器挂在这个 checkpoint 上的状态引用（spec §8.4），平台不解释。
	ExecutorStateRef string
	Label            string
	CreatedAt        time.Time
}

func checkpointOf(p *maatv1.Checkpoint) Checkpoint {
	return Checkpoint{
		ID: p.GetId(), SessionID: p.GetSessionId(), ThreadID: p.GetThreadId(), RunID: p.GetRunId(), Seq: p.GetSeq(),
		Kind: enumOf[CheckpointKind](p.GetKind(), "CHECKPOINT_KIND_"), Stable: p.GetStable(), ContextRoot: p.GetContextRoot(),
		ExecutorStateRef: p.GetExecutorStateRef(), Label: p.GetLabel(), CreatedAt: p.GetCreatedAt().AsTime(),
	}
}

// Checkpoints 列出与标注 checkpoint。
type Checkpoints struct{ c *Client }

// ListCheckpointsParams 是列出 checkpoint 的过滤条件。
type ListCheckpointsParams struct {
	// ThreadID 只列出该线程的 checkpoint；为空表示全部线程。
	ThreadID string
	// Kind 只列出该粒度；为空表示全部。
	Kind CheckpointKind
	// StableOnly 只列出可以 fork 的 checkpoint。
	StableOnly bool
	// PageSize 是每次请求的条数；0 表示使用平台默认值。
	PageSize uint32
}

// List 按 seq 升序列出会话的 checkpoint，自动翻页。
func (cs *Checkpoints) List(ctx context.Context, sessionID string, p ListCheckpointsParams) iter.Seq2[Checkpoint, error] {
	return func(yield func(Checkpoint, error) bool) {
		token := ""
		for {
			var res *maatv1.ListCheckpointsResponse
			err := cs.c.call(ctx, true, func(ctx context.Context) error {
				r, err := cs.c.sessions.ListCheckpoints(ctx, connect.NewRequest(&maatv1.ListCheckpointsRequest{
					SessionId: sessionID, ThreadId: p.ThreadID, Kind: checkpointKindToProto[p.Kind], StableOnly: p.StableOnly,
					Page: &maatv1.PageRequest{PageSize: p.PageSize, PageToken: token},
				}))
				if err != nil {
					return err
				}
				res = r.Msg
				return nil
			})
			if err != nil {
				yield(Checkpoint{}, err)
				return
			}
			for _, pc := range res.GetCheckpoints() {
				if !yield(checkpointOf(pc), nil) {
					return
				}
			}
			if token = res.GetPage().GetNextPageToken(); token == "" {
				return
			}
		}
	}
}

// Annotate 事后设置 checkpoint 的 executor_state_ref（≤ 1KB；空串表示清除），返回更新后的 checkpoint
// （spec §8.4）。工具执行时设置请使用 SetExecutorStateRef。
func (cs *Checkpoints) Annotate(ctx context.Context, checkpointID, executorStateRef string) (Checkpoint, error) {
	if err := checkExecutorStateRef(executorStateRef); err != nil {
		return Checkpoint{}, err
	}
	var res *maatv1.AnnotateCheckpointResponse
	err := cs.c.call(ctx, true, func(ctx context.Context) error {
		r, err := cs.c.sessions.AnnotateCheckpoint(ctx, connect.NewRequest(&maatv1.AnnotateCheckpointRequest{
			CheckpointId: checkpointID, ExecutorStateRef: executorStateRef,
		}))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	if err != nil {
		return Checkpoint{}, err
	}
	return checkpointOf(res.GetCheckpoint()), nil
}
