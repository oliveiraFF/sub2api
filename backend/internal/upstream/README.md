# package upstream — 上游适配层（PoC）

把「每个上游一个 god file」收敛成一个接口。当前状态：

| 文件 | 行数 | 落在四个关注点上的行数 | 占比 |
|---|---:|---:|---:|
| `service/gateway_service.go`（Claude） | 10403 | 392 | 3.8% |
| `service/openai_gateway_service.go`（OpenAI/Codex） | 7616 | 175 | 2.3% |
| `service/antigravity_gateway_service.go`（Antigravity/Gemini） | 4444 | 501 | 11.3% |
| **合计** | **22463** | **1068** | — |

四个关注点：鉴权头拼装、base URL 解析、上游错误分类、会话粘性键。

## 账号状态机 + 请求留痕（`pool.go`）

按设计稿实现了 reference scheduler，并修正了其中三处生产隐患：

| 设计稿写法 | 问题 | 本实现 |
|---|---|---|
| 所有 429 一律冷却 | OpenAI 的「额度耗尽」也是 429（`insufficient_quota`），冷却会让池子反复选中死号 | 交给适配器 `Classify`，判为 `invalid` 并摘号 |
| `sticky map[session]=account` 不设上限 | 长期运行内存只增不减 | TTL（默认 30min）+ 容量上限（默认 1 万）+ LRU 式淘汰 |
| `Report` 里 `InFlight--` | defer + 显式调用会双重释放，账号静默失容 | `Lease` 幂等释放，且与状态转移同锁完成 |
| 「invalid 需人工恢复」 | 多数 401 是 access token 过期，刷新任务可自愈 | `Reinstate(id)` 供刷新任务调用 |
| 只有 `Status` 一个字段 | 会把「管理员停用」和「临时冷却」混为一谈 | 状态机与持久化状态分离：池内 `active/cooling/degraded/invalid`，库里仍是 `active/disabled/error` + `RateLimitedAt/TempUnschedulableUntil` |

`degraded` 的语义：连续 5xx/超时达到阈值后降级，只有在没有可用 `active` 账号时才被选中，且同一时刻只放一个探针，探针成功即回 `active`，连续失败到阈值转 `invalid`。

时间可注入（`WithClock`），`Classify` 也显式接收 `now` —— 否则冷却窗口既不可测也不可重放。

## 为什么值得改

不是行数问题，是**变更成本**问题：接第 4 个上游 = 再写 4000–9000 行，且四个关注点各复制一份。
改成适配器后，新上游 ≈ 200 行（本包三个适配器分别是 122/129/110 行）。

按保守 50% 可下沉估算，约 500 行可从 god file 移出，更重要的是**改一处等于改三处的地方只剩一处**。

## 接口

```go
type Adapter interface {
    Platform() Platform
    DefaultBaseURL() string
    AuthHeaders(Credentials) (http.Header, error)
    BaseURL(Credentials) string
    Classify(status int, header http.Header, body []byte) Failure
    StickyKey(RequestMeta) string
}

// 平台特有的端点列表（Antigravity 的 daily/prod 双端点）
type BaseURLFallbacks interface { BaseURLFallbacks(Credentials) []string }
```

`Failure` 是调度器唯一需要认识的东西：`auth`（摘号）/ `rate_limit`（冷却到 RetryAt）/
`overload`（换号重试）/ `transient`（重试）/ `client`（不重试，原样返回）。

## 已经吸收掉的真实差异

这些都不是我编的，是从现有代码里读出来的：

- Claude：`x-api-key` vs `authorization: Bearer`；OAuth 必带 `anthropic-beta: oauth-2025-04-20`；`anthropic-version: 2023-06-01`；
  `service.Account.GetBaseURL()` 对非 API-key 账号返回 `""`（`account.go:863-866`）这个「空串即默认」的哨兵语义；
  余额不足以 **400** 返回（不是 402/429）；529 与 `overloaded_error` 都属于过载。
- OpenAI：`insufficient_quota` 挂在 **429** 上但语义是账号级（必须摘号，不能当成限流冷却）；
  Codex CLI 指纹 = `version: 0.125.0` + `originator: codex_cli_rs`（`openai_gateway_service.go:65,3736`）；
  `chatgpt-account-id` 只在 OAuth 账号上出现。
- Antigravity：双端点（daily sandbox 优先、prod 兜底）；自定义 `base_url` 在 API-key 账号上要拼 `/antigravity` 后缀；
  429 里的 `MODEL_CAPACITY_EXHAUSTED` 是「当前模型没容量」（换号重试），不是「账号没额度」。

## 迁移路径（不一次性重写）

1. **只读接线**：~~在 `GatewayService`/`OpenAIGatewayService`/`AntigravityGatewayService` 里注入 `*upstream.Registry`，
   把「决定发哪些鉴权头」的空函数换成 `adapter.AuthHeaders(...)`（行为不变，纯机械替换）。~~
   **✅ 已完成（Claude API-key 路径）**：`service/anthropic_apikey_auth.go:setAnthropicAPIKeyAuthHeader`
   改为委托 `ClaudeAdapter.AuthHeaders`，5 个调用点（`gateway_service.go` ×4、`upstream_models.go`、
   `account_test_service.go`）签名不变。为承载「同一个 key 既要能走 x-api-key 又要能走
   Authorization: Bearer」的中转站差异，适配器新增 `Credentials.APIKeyScheme` 维度。
   桥接层：`service/upstream_auth_bridge.go`（只搬运鉴权头，不搬运 anthropic-version/accept，
   以免覆盖客户端自带的版本号）。一致性由 `service/upstream_auth_bridge_test.go` 逐字段对比旧实现保证。
2. **错误分类接线**：把散落的 `if status == 429 {...} else if status == 401 {...}` 换成一次 `adapter.Classify(...)`，
   再按 `Failure.Kind` 分支。这一步能直接删掉 ~90 处字面量比较。
3. **base URL 接线**：`GetBaseURL()` / `GetOpenAIBaseURL()` / `resolveAntigravityForwardBaseURL()` 改为委托适配器。
4. **粘性键接线**：三处 `sessionHash` 计算统一走 `adapter.StickyKey(...)`。
5. 每步独立可回滚、独立可测。

> 注：第 1 步属于「立接缝」，不做减法——`anthropic_apikey_auth.go` 从 37 行变 53 行。
> 行数收益在第 2、3 步（错误分类与 base URL 是重复密度最高的两处）。

## 验证

```bash
cd backend
go test ./internal/upstream/ -count=1   # 19 个分类用例 + 鉴权/baseURL/粘性键
go vet  ./internal/upstream/
```

本包只依赖标准库，所以接入前可以先独立跑测试，不会把 ent/redis/aws 的依赖树拖进测试二进制。
