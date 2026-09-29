package maat

import (
	"context"
	"time"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// Thread 是会话中的一个线程：主线程，或 SubAgent 经 spawn_agent 创建的子线程（spec §7）。
// 子线程结束 Run 后变为空闲，可以用 Session.Send(..., WithThread(id)) 直接追问。
type Thread struct {
	ID string
	// ParentThreadID 与 ParentToolCallID 是创建它的线程与 spawn_agent 调用；主线程为空。
	ParentThreadID   string
	ParentToolCallID string
	// Path 是从主线程到本线程的线程 ID 列表。
	Path []string
	// Depth 是线程深度：主线程为 0。
	Depth uint32
	// AgentName 是 SubAgent 的名字；主线程为 "main"。
	AgentName string
	// Mode 是子线程的运行方式；主线程为空。
	Mode   ThreadMode
	Status ThreadStatus
	// StopReason 是最近一个 Run 的结束原因。
	StopReason         StopReason
	CurrentRunID       string
	LatestCheckpointID string
	StepCount          uint32
	// Usage 是该线程全部 Run 的用量之和。
	Usage     Usage
	CreatedAt time.Time
	UpdatedAt time.Time
}

func threadOf(p *maatv1.Thread) Thread {
	return Thread{
		ID: p.GetId(), ParentThreadID: p.GetParentThreadId(), ParentToolCallID: p.GetParentToolCallId(), Path: p.GetPath(),
		Depth: p.GetDepth(), AgentName: p.GetAgentName(), Mode: enumOf[ThreadMode](p.GetMode(), "THREAD_MODE_"),
		Status: enumOf[ThreadStatus](p.GetStatus(), "THREAD_STATUS_"), StopReason: enumOf[StopReason](p.GetStopReason(), "STOP_REASON_"),
		CurrentRunID: p.GetCurrentRunId(), LatestCheckpointID: p.GetLatestCheckpointId(), StepCount: p.GetStepCount(),
		Usage: usageOf(p.GetUsage()), CreatedAt: p.GetCreatedAt().AsTime(), UpdatedAt: p.GetUpdatedAt().AsTime(),
	}
}

// Threads 列出会话的全部线程（按创建顺序，主线程在前），自动翻页。
func (s *Session) Threads(ctx context.Context) ([]Thread, error) {
	var (
		out   []Thread
		token string
	)
	for {
		var res *maatv1.ListThreadsResponse
		err := s.c.call(ctx, true, func(ctx context.Context) error {
			r, err := s.c.sessions.ListThreads(ctx, connect.NewRequest(&maatv1.ListThreadsRequest{
				SessionId: s.ID, Page: &maatv1.PageRequest{PageToken: token},
			}))
			if err != nil {
				return err
			}
			res = r.Msg
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, t := range res.GetThreads() {
			out = append(out, threadOf(t))
		}
		if token = res.GetPage().GetNextPageToken(); token == "" {
			return out, nil
		}
	}
}
