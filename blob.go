package maat

import (
	"context"
	"time"

	"connectrpc.com/connect"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// Blob 是会话引用的大内容（spec §13.2）：Content 与 DownloadURL 二选一。
type Blob struct {
	Ref string
	// Kind 是 "message"、"tool_args"、"tool_result" 或 "summary"。
	Kind string
	// ContentType 是公开格式：message 与 tool_result 为 application/json，tool_args 为模型给出的参数原文
	// （application/json），summary 为 text/markdown。
	ContentType string
	// Size 是公开格式内容的字节数。
	Size uint64
	// Content 在内容不超过 1MB 时有值。
	Content []byte
	// DownloadURL 在内容超过 1MB 时有值，ExpiresAt 之前有效；响应可能带 Content-Encoding: zstd。
	DownloadURL string
	ExpiresAt   time.Time
}

// GetBlob 读取会话（或它的 fork 祖先）引用的 Blob，例如事件中的 content_ref、args_ref、result_ref。
// 没有被该会话引用的 ref 返回 NotFound。
func (s *Session) GetBlob(ctx context.Context, ref string) (*Blob, error) {
	var res *maatv1.GetBlobResponse
	err := s.c.call(ctx, true, func(ctx context.Context) error {
		r, err := s.c.blobs.GetBlob(ctx, connect.NewRequest(&maatv1.GetBlobRequest{SessionId: s.ID, Ref: ref}))
		if err != nil {
			return err
		}
		res = r.Msg
		return nil
	})
	if err != nil {
		return nil, err
	}
	b := &Blob{
		Ref: res.GetRef(), Kind: res.GetKind(), ContentType: res.GetContentType(), Size: res.GetSize(),
		Content: res.GetContent(), DownloadURL: res.GetDownloadUrl(),
	}
	if res.GetDownloadUrlExpiresAt() != nil {
		b.ExpiresAt = res.GetDownloadUrlExpiresAt().AsTime()
	}
	return b, nil
}
