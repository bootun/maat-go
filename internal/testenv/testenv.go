// Package testenv 为集成测试准备后端环境：读取连接信息，并用项目管理员 Key 创建指向 fake LLM 的
// 凭据、模型别名与 Agent。
//
// 连接信息按以下顺序读取（后者覆盖前者）：
//  1. MAAT_E2E_ENV_FILE 指向的文件（后端仓库 `make up` 生成的 deploy/.env.e2e，
//     使用其中的 MAAT_E2E_BASE_URL、MAAT_API_KEY、MAAT_SDK_API_KEY）；
//  2. 环境变量 MAAT_E2E_BASE_URL、MAAT_E2E_ADMIN_API_KEY、MAAT_E2E_SDK_API_KEY。
//
// 没有配置 MAAT_E2E_BASE_URL 时跳过测试。
//
// 管理类 API（凭据、模型别名、Agent）不在 SDK 的范围内，这里直接以 Connect 的 JSON 协议调用，
// 不需要对应的 proto。
package testenv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Env 是集成测试的后端环境。
type Env struct {
	BaseURL string
	// AdminKey 是 project:admin 权限的 API Key，用于创建凭据、模型别名与 Agent。
	AdminKey string
	// SDKKey 是 SDK 使用的 API Key（sessions:read、sessions:write）。
	SDKKey string
	// FakeLLMURL 是后端访问 fake LLM 的地址（写入凭据的 base URL），默认为 compose 网络内的地址。
	FakeLLMURL string
}

// Load 读取环境；没有配置时跳过测试。
func Load(t testing.TB) Env {
	t.Helper()
	vars := map[string]string{}
	if path := os.Getenv("MAAT_E2E_ENV_FILE"); path != "" {
		if err := readEnvFile(path, vars); err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
	}
	e := Env{
		BaseURL:    first(os.Getenv("MAAT_E2E_BASE_URL"), vars["MAAT_E2E_BASE_URL"]),
		AdminKey:   first(os.Getenv("MAAT_E2E_ADMIN_API_KEY"), vars["MAAT_API_KEY"]),
		SDKKey:     first(os.Getenv("MAAT_E2E_SDK_API_KEY"), vars["MAAT_SDK_API_KEY"]),
		FakeLLMURL: first(os.Getenv("MAAT_E2E_FAKELLM_INTERNAL_URL"), "http://fakellm:8080/v1"),
	}
	if e.BaseURL == "" {
		t.Skip("MAAT_E2E_BASE_URL is not set; start the backend with `make up` and set MAAT_E2E_ENV_FILE")
	}
	if e.AdminKey == "" || e.SDKKey == "" {
		t.Fatal("admin and SDK API keys are required (MAAT_E2E_ADMIN_API_KEY / MAAT_E2E_SDK_API_KEY or MAAT_E2E_ENV_FILE)")
	}
	return e
}

func first(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func readEnvFile(path string, vars map[string]string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), "="); ok && !strings.HasPrefix(k, "#") {
			vars[k] = v
		}
	}
	return sc.Err()
}

// Script 构造 fake LLM 的脚本（每个参数是一个 step 的 JSON，格式见后端 plan 附录 C）。
func Script(steps ...string) string { return `{"steps":[` + strings.Join(steps, ",") + `]}` }

// NewAgent 创建一个测试独占的 Agent：凭据指向 fake LLM，模型别名的 upstream_model 为场景名；
// script 非空时写在 system prompt 的 FAKE_SCRIPT: 之后。测试结束时归档 Agent。返回 Agent ID。
func (e Env) NewAgent(t testing.TB, scenario, script string) string {
	t.Helper()
	ctx := context.Background()
	nonce := fmt.Sprintf("sdk-%s-%d", scenario, time.Now().UnixNano())

	var cred struct {
		Credential struct {
			ID string `json:"id"`
		} `json:"credential"`
	}
	e.admin(ctx, t, "maat.v1.CredentialService/CreateCredential", map[string]any{
		"name": nonce, "baseUrl": e.FakeLLMURL, "apiKey": "sk-" + nonce,
	}, &cred)
	e.admin(ctx, t, "maat.v1.ModelAliasService/CreateModelAlias", map[string]any{
		"modelAlias": map[string]any{
			"alias": nonce, "credentialId": cred.Credential.ID, "upstreamModel": scenario,
			"contextWindow": 32000, "maxOutputTokens": 4096, "capabilities": map[string]any{"streamUsage": true},
		},
	}, nil)
	// nonce 写在 system prompt 中，使每个测试的请求体不同（fake LLM 按请求体计数 attempt）。
	prompt := "You are an SDK integration test agent (" + nonce + ")."
	if script != "" {
		prompt += "\nFAKE_SCRIPT:" + script
	}
	var agent struct {
		Agent struct {
			ID string `json:"id"`
		} `json:"agent"`
	}
	e.admin(ctx, t, "maat.v1.AgentService/CreateAgent", map[string]any{
		"name": nonce, "initialVersion": map[string]any{"systemPrompt": prompt, "defaultModelAlias": nonce},
	}, &agent)
	t.Cleanup(func() {
		e.admin(context.Background(), t, "maat.v1.AgentService/ArchiveAgent", map[string]any{"agentId": agent.Agent.ID}, nil)
	})
	return agent.Agent.ID
}

// admin 以 Connect 的 JSON 协议调用一个一元 RPC。
func (e Env) admin(ctx context.Context, t testing.TB, procedure string, req, res any) {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("%s: marshal request: %v", procedure, err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.BaseURL, "/")+"/"+procedure, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("%s: %v", procedure, err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Connect-Protocol-Version", "1")
	hreq.Header.Set("Authorization", "Bearer "+e.AdminKey)
	hres, err := (&http.Client{}).Do(hreq)
	if err != nil {
		t.Fatalf("%s: %v", procedure, err)
	}
	defer hres.Body.Close()
	out, err := io.ReadAll(hres.Body)
	if err != nil {
		t.Fatalf("%s: read response: %v", procedure, err)
	}
	if hres.StatusCode != http.StatusOK {
		t.Fatalf("%s: HTTP %d: %s", procedure, hres.StatusCode, out)
	}
	if res != nil {
		if err := json.Unmarshal(out, res); err != nil {
			t.Fatalf("%s: decode response: %v", procedure, err)
		}
	}
}
