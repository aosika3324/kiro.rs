# Design: Port Kiro-Go endpoint fallback vào kiro.rs

**Date:** 2026-07-01
**Branch:** `feat/switch-codewhisperer-endpoint`
**Status:** Approved design; implementation not started

## Context

kiro.rs đã có endpoint mới `codewhisperer` ở commit `f12476d`. Endpoint đó port đúng route CodeWhisperer của Kiro-Go:

- `codewhisperer.us-east-1.amazonaws.com` ở `us-east-1`
- `q.{region}.amazonaws.com` ở region khác
- `x-amz-target: AmazonCodeWhispererStreamingService.GenerateAssistantResponse`
- path/origin/content-type/profileArn/User-Agent giống `ide`

Load test thực tế cho thấy endpoint mới chạy được nhưng không giải quyết rate-limit:

| Test | Endpoint | Pattern | 200 | 502 chứa upstream 429 |
|---|---|---:|---:|---:|
| Burst | `ide` | 400 concurrent / 30 accounts | 26 | 374 |
| Burst | `codewhisperer` | 400 concurrent / 30 accounts | 22 | 378 |
| Paced | `codewhisperer` | 400 requests / ~60s / 30 accounts | 38 | 362 |

Body lỗi là upstream rate-limit theo user/account:

```text
429 Too Many Requests
USER_REQUEST_RATE_EXCEEDED
```

Kết luận: lỗi không phải do chỉ riêng `ide`; khác biệt lớn còn lại với Kiro-Go là **endpoint fallback trong cùng account**.

## Kiro-Go behavior cần port

Kiro-Go có ba route trong `C:\Users\Admin\Kiro-Go\proxy\kiro.go`:

| Kiro-Go name | kiro.rs target endpoint | URL base | Origin | `x-amz-target` |
|---|---|---|---|---|
| Kiro IDE | `ide` | `q.us-east-1.amazonaws.com/generateAssistantResponse` | `AI_EDITOR` | none |
| CodeWhisperer | `codewhisperer` | `codewhisperer.us-east-1.amazonaws.com/generateAssistantResponse` | `AI_EDITOR` | `AmazonCodeWhispererStreamingService.GenerateAssistantResponse` |
| AmazonQ | `amazonq` (new) | `q.us-east-1.amazonaws.com/generateAssistantResponse` | `AI_EDITOR` | `AmazonQDeveloperStreamingService.SendMessage` |

Kiro-Go sorts endpoints by config:

```text
preferred endpoint first
then other endpoints if endpointFallback=true
```

When one endpoint returns `429`, Kiro-Go tries the next endpoint on the **same account** before giving up and moving through account-level failover.

## Goals

1. Add `amazonq` endpoint to kiro.rs.
2. Add route-level fallback matching Kiro-Go semantics.
3. Try fallback endpoints on the same credential before marking the credential rate-limited or moving to another credential.
4. Preserve existing behavior unless fallback is explicitly enabled.
5. Keep account-level scheduler/rate-limit changes out of this patch.

## Non-goals

- Do not redesign account scheduling.
- Do not add token bucket / queue in this patch.
- Do not change streaming parser or Anthropic conversion.
- Do not change existing `ide`, `cli`, or `codewhisperer` request format except where needed for shared helpers.
- Do not make `cli` part of Kiro-Go fallback; `cli` uses different path/protocol/origin and is not equivalent to Kiro-Go AmazonQ route.

## Config design

Add fields to `Config`:

```json
{
  "preferredEndpoint": "codewhisperer",
  "endpointFallback": true
}
```

Semantics:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `preferredEndpoint` | string/null | absent = use `defaultEndpoint` | Endpoint attempted first when fallback order is built. |
| `endpointFallback` | bool | `false` | If false, keep current single-endpoint behavior. If true, try remaining Kiro-Go-compatible endpoints after preferred. |

Backward compatibility:

- Existing `defaultEndpoint` remains supported.
- Existing per-credential `endpoint` remains supported.
- If `endpointFallback=false`, endpoint selection remains exactly current: `credential.endpoint` or `config.defaultEndpoint`.
- If `endpointFallback=true`, the primary endpoint is:
  1. `credential.endpoint`, if present
  2. else `config.preferredEndpoint`, if present
  3. else `config.defaultEndpoint`

Recommended runtime config for parity testing:

```json
{
  "defaultEndpoint": "codewhisperer",
  "preferredEndpoint": "codewhisperer",
  "endpointFallback": true
}
```

## Endpoint order

Fallback set includes only Kiro-Go-compatible streaming routes:

```text
ide
codewhisperer
amazonq
```

`cli` is excluded.

Order rules:

- Primary first.
- Then remaining endpoints in Kiro-Go declaration order: `ide`, `codewhisperer`, `amazonq`.
- Deduplicate primary.

Examples:

| Primary | Fallback enabled order |
|---|---|
| `codewhisperer` | `codewhisperer -> ide -> amazonq` |
| `ide` | `ide -> codewhisperer -> amazonq` |
| `amazonq` | `amazonq -> ide -> codewhisperer` |

If primary is `cli`, fallback remains single endpoint `cli`. This avoids mixing incompatible request formats.

## New `amazonq` endpoint

Add `src/kiro/endpoint/amazonq.rs`.

Behavior:

- name: `amazonq`
- host: `q.{api_region}.amazonaws.com`
- API URL: `https://q.{api_region}.amazonaws.com/generateAssistantResponse`
- MCP URL: `https://q.{api_region}.amazonaws.com/mcp`
- content-type: `application/json`
- origin: leave as `AI_EDITOR`
- profileArn injection: same as `ide`/`codewhisperer`
- User-Agent: same KiroIDE-style streaming UA as `ide`/`codewhisperer`
- `x-amz-target: AmazonQDeveloperStreamingService.SendMessage`
- headers: same base headers as `ide`/`codewhisperer`, including token type handling

Region handling:

- Use the same `api_region` behavior as `ide`:
  - external_idp uses `credentials.data_plane_region()` from profileArn
  - other credentials use `credentials.effective_api_region(config)`
- Host is always `q.{region}.amazonaws.com`.

## Provider flow

Current flow:

```text
acquire credential
resolve profileArn
choose one endpoint
send request
handle status
```

New fallback-enabled flow:

```text
acquire credential
resolve profileArn
build endpoint order for this credential/config
for endpoint in order:
  send request using same credential/token/profileArn
  if success: return
  if endpoint-fallbackable error: try next endpoint on same credential
  if non-fallbackable error: handle with existing account logic
if all endpoints exhausted with fallbackable errors:
  apply existing account-level handling based on last error
```

Implementation detail: isolate one upstream attempt into a helper so `call_api_with_retry` is readable:

```rust
async fn send_api_attempt(
    &self,
    ctx: &CallContext,
    endpoint: Arc<dyn KiroEndpoint>,
    request_body: &str,
    is_stream: bool,
    machine_id: &str,
) -> ApiAttemptResult
```

The helper handles request construction, body transform, client selection, send, status/body extraction, and trace metadata for one endpoint attempt.

## Fallback classification

Fallback endpoint on the same credential only for endpoint-specific or transient cases:

| Status/error | Fallback endpoint? | Existing account handling? | Reason |
|---|---:|---:|---|
| HTTP 429 | yes | after all endpoints fail | Matches Kiro-Go; may be route quota-specific. |
| HTTP 408 | yes | after all endpoints fail | Transient timeout. |
| HTTP 5xx | yes | after all endpoints fail | Transient upstream/server issue. |
| Network error | yes | after all endpoints fail | Could be route/connection-specific. |
| HTTP 400 | no | immediate bad request | Request/body problem; retrying endpoints is noise. |
| HTTP 401/403 | no | existing token refresh / account failover logic | Auth/token/account issue. |
| HTTP 402 | no | existing quota exhausted logic | Account quota/overage issue. |
| HTTP 524 | no | existing fast fail | Current code intentionally does not retry gateway timeout. |
| client validation marker | no | immediate bad request | Caller request invalid. |

Special case for `429 USER_REQUEST_RATE_EXCEEDED`:

- With fallback enabled: try remaining endpoints on same account first.
- If all fallback endpoints return rate-limit, then call existing `report_rate_limited(ctx.id, cooldown)` and continue to next credential.
- This preserves current cooldown behavior but delays it until route fallback is exhausted.

## Trace and logging

Every endpoint attempt must emit its own trace attempt with endpoint name.

Required debug/warn messages:

```text
使用端点 [codewhisperer] POST ...
Endpoint codewhisperer returned 429; trying fallback endpoint ide on credential #N
Endpoint ide returned 429; trying fallback endpoint amazonq on credential #N
All fallback endpoints exhausted for credential #N; applying account rate-limit cooldown
```

Trace fields already support endpoint name:

```rust
TraceAttempt { credential_id, endpoint, http_status, outcome, error_snippet, duration_ms }
```

No schema change is required.

## Tests

Unit tests:

1. Endpoint registry includes `amazonq`.
2. `amazonq` URL/header/body behavior:
   - API URL uses `/generateAssistantResponse`
   - host is `q.us-east-1.amazonaws.com` for us-east-1
   - host is `q.eu-central-1.amazonaws.com` for eu-central-1 external_idp profileArn
   - `x-amz-target` equals `AmazonQDeveloperStreamingService.SendMessage`
   - profileArn injection matches `ide`
3. Endpoint order helper:
   - fallback false -> one endpoint only
   - primary `codewhisperer`, fallback true -> `codewhisperer, ide, amazonq`
   - primary `ide`, fallback true -> `ide, codewhisperer, amazonq`
   - primary `amazonq`, fallback true -> `amazonq, ide, codewhisperer`
   - primary `cli`, fallback true -> `cli` only
4. Classification helper:
   - fallbackable: 429, 408, 5xx, network error
   - not fallbackable: 400, 401, 402, 403, 524, client validation
5. Provider behavior with mock endpoint/client boundary if feasible:
   - 429 on primary then 200 on fallback returns success without cooling account
   - 429 on all fallback endpoints calls rate-limit cooldown once
   - 401 on primary does not try fallback

Manual verification:

1. `cargo build`
2. `cargo test endpoint`
3. `cargo test amazonq`
4. Start server with:
   ```json
   "preferredEndpoint": "codewhisperer",
   "endpointFallback": true
   ```
5. Run:
   - 100 concurrent / 1 account
   - 400 concurrent / 30 accounts
   - 400 requests / 60s / 30 accounts
6. Compare against baseline:
   - success count
   - upstream `USER_REQUEST_RATE_EXCEEDED` count
   - trace endpoint sequence

## Rollout

Default rollout is safe:

```json
"endpointFallback": false
```

Users opt in explicitly. If fallback behaves poorly, set:

```json
"endpointFallback": false
```

or choose a single endpoint via existing:

```json
"defaultEndpoint": "ide"
```

## Acceptance criteria

- `amazonq` endpoint is selectable by name.
- `endpointFallback=false` preserves current behavior.
- `endpointFallback=true` tries Kiro-Go-compatible endpoints on the same credential before account failover.
- 429 on primary no longer immediately cools the account if another fallback endpoint succeeds.
- Traces show all attempted endpoint names in order.
- Build and tests pass.
- Manual load test produces a clear before/after comparison.
