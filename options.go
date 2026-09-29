package maat

import (
	"math/rand/v2"
	"net/http"
	"time"
)

// 未通过 Option 设置时读取的环境变量（spec §14.1）。
const (
	EnvBaseURL = "MAAT_BASE_URL"
	EnvAPIKey  = "MAAT_API_KEY"
)

// Option 配置 Client。
type Option func(*config)

type config struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	// maxAttempts 是一元调用的最大尝试次数（含第一次）；只有 Retryable 的错误才会重试。
	maxAttempts int
	retry       backoff
	// reconnect 是事件流断线后的重连退避（spec §14.4：0.5s → 10s）。
	reconnect backoff
}

func defaultConfig() config {
	return config{
		// 不设 Timeout：它会限制整个流式响应的时长。一元调用的时限由调用方的 ctx 控制。
		httpClient:  &http.Client{},
		maxAttempts: 3,
		retry:       backoff{min: 200 * time.Millisecond, max: 2 * time.Second, jitter: true},
		reconnect:   backoff{min: 500 * time.Millisecond, max: 10 * time.Second, jitter: true},
	}
}

// WithBaseURL 设置平台地址，例如 "https://maat.example.com"。未设置时读取 MAAT_BASE_URL。
func WithBaseURL(u string) Option { return func(c *config) { c.baseURL = u } }

// WithAPIKey 设置平台 API Key。未设置时读取 MAAT_API_KEY。
// API Key 只能在服务端使用，不得下发到浏览器（spec §14.1）。
func WithAPIKey(key string) Option { return func(c *config) { c.apiKey = key } }

// WithHTTPClient 设置底层 HTTP 客户端。事件流是长连接，请不要给它设置 Timeout。
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// backoff 是指数退避。
type backoff struct {
	min, max time.Duration
	// jitter 为真时把等待时间乘以 [0.8, 1.2) 的随机因子，避免大量客户端同时重连。
	jitter bool
}

// delay 返回第 n 次（从 0 开始）失败后的等待时间：min·2ⁿ，不超过 max。
func (b backoff) delay(n int) time.Duration {
	d := b.min
	for i := 0; i < n && d < b.max; i++ {
		d *= 2
	}
	d = min(d, b.max)
	if b.jitter {
		d = time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
	}
	return d
}
