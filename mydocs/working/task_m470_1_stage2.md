# Task M470-1 Stage 2 완료 보고서

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

구현계획서: [task_m470_1_impl.md](../plans/task_m470_1_impl.md)

Stage: 2 — Provider-neutral bootstrap과 Worker bridge contract

## 단계 목적

기존 MariaDB + ChromaDB local runtime은 그대로 완전한 기본 runtime으로 유지하면서, Container → Worker → D1/Vectorize Cloudflare 배포 경로의 runtime 선택·bridge 계약·readiness 표현만 provider-neutral하게 만든다. 이 단계는 authorization context, versioned request/response envelope, status-to-error mapping, account-neutral deploy template, 정적 type check를 확립하고, 실제 D1 canonical·Vectorize·operator data 기능은 구현하지 않는다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `go-service/internal/config/config.go` | `RuntimeProfileCloudflare`, `StoreModeCloudflareAuthority`, `VectorModeCloudflare` opt-in 선택자, `CloudflareBridgeURL`/`CloudflareBridgeToken` 환경 설정, allowlist/검증, `IsCloudflareProfile()`/`CloudflareProfileReady()` parity gate 추가. 기존 default와 validation은 변경하지 않았다. |
| `go-service/internal/config/config_test.go` | Cloudflare 프로필 로드·검증·기본 vector mode·token redaction 테스트 추가. |
| `go-service/internal/cloudflarebridge/client.go` | virtual-host 전용 Worker bridge client. versioned envelope, timeout, status-to-error sentinel/BridgeError mapping, bounded retryable 판정, `Ping`/`Health` 진단 제공. Cloudflare API token·SDK·account/resource ID를 포함하지 않는다. |
| `go-service/internal/cloudflarebridge/client_test.go` | round trip, status mapping, operation error, version/id correlation, timeout, empty operation 계약 테스트. |
| `go-service/internal/httpapi/server.go` | `cloudflare_authority` store mode에 Stage 2 bootstrap noop placeholder case 추가. write guard(`usesShadowWriteStore`)는 Stage 2 동안 MariaDB 전용으로 유지한다. |
| `go-service/internal/httpapi/group_health.go` | `/ready`에 Cloudflare parity gate 추가. parity 미완료 시 `503` + `ready_blocker=cloudflare_parity_incomplete`. local profile의 기존 readiness check·label·동작은 그대로 유지. |
| `go-service/internal/httpapi/group_health_cloudflare_test.go` | Cloudflare readiness bootstrap 차단, bridge config 표기, local readiness 무변경 테스트. |
| `deploy/cloudflare/worker/src/index.ts` | versioned envelope을 받아 `bridge.ping`/`bridge.health`를 구현하고, canonical D1·vector·admin data operation은 typed `not_implemented`로 응답하는 Worker. timing-safe bearer 인증, 경로/method/version 검증 포함. |
| `deploy/cloudflare/worker/package.json`, `package-lock.json`, `tsconfig.json` | `npm run check`(`tsc --noEmit`) 기반 정적 검사 구성. |
| `deploy/cloudflare/wrangler.template.toml` | stable binding 이름(`DB`, `VECTORIZE`)과 non-account-specific 설정만 둔 template. account ID·database ID·index ID·route·domain·secret 없음. |
| `deploy/cloudflare/.gitignore` | rendered `wrangler.toml`, secret 파일, `node_modules` 등 로컬/보호 산출물 제외. |
| `.gitignore` | `deploy/` 전체 무시 규칙을 `deploy/*` + `!deploy/cloudflare/`로 좁혀 Cloudflare 배포 source는 tracked, 나머지 deploy build output은 계속 무시되도록 조정. |

## 본문 변경 정도 / 본문 무손실 여부

코드 작업이며, 사용자·문서 원문은 변경하지 않았다. 기존 local MariaDB/ChromaDB 기본값, 공개 API/config 동작, HTTP route, reset/turn/memory 동작은 변경하지 않았다. 새 Cloudflare 선택자는 명시적 opt-in이며 기본값에는 영향을 주지 않는다. `/ready`의 기존 local check key와 값은 그대로이며 Cloudflare profile에서만 새 `cloudflare_*` key가 추가된다.

## 검증 결과

실행 명령:

```bash
cd go-service
go test ./internal/config ./internal/httpapi ./internal/cloudflarebridge -count=1
cd ..
cd deploy/cloudflare/worker
npm ci
npm run check
cd ../../..
git diff --check
```

결과:

- `ok github.com/risulongmemory/archive-center-go/internal/config 0.304s`
- `ok github.com/risulongmemory/archive-center-go/internal/httpapi 55.091s`
- `ok github.com/risulongmemory/archive-center-go/internal/cloudflarebridge 0.848s`
- `npm ci`가 lockfile로부터 설치 재현 성공, `npm run check`(`tsc --noEmit`) 오류 없음.
- `go vet ./internal/config ./internal/cloudflarebridge ./internal/httpapi` 무출력(경고 없음).
- `git diff --check` 통과(공백 오류 없음).
- tracked artifact account-neutral 확인: `deploy/` stage 대상이 `.gitignore`, `package-lock.json`, `package.json`, `src/index.ts`, `tsconfig.json`, `wrangler.template.toml` 6개이며 `node_modules`는 제외된다. tracked 산출물에 account/resource ID, secret, token이 없다.
- Cloudflare 자격 증명이 없는 환경이므로 authenticated remote integration은 실행하지 않았다. 성공으로 주장하지 않는다.

## 잔여 위험

- D1 canonical store provider가 없어 Cloudflare profile의 canonical write는 Stage 3 전까지 의도적으로 차단된다(`usesShadowWriteStore` false, noop placeholder). Stage 2에서 readiness는 `bootstrap_only`로 정직하게 보고한다.
- `cloudflareParityComplete` 상수는 Stage 2에서 `false`이며, Stage 3·4·5 gate 구현이 완료될 때까지 `/ready`가 functional deployment ready를 보고하지 않는다.
- Operator parity의 durable reset/job 상태는 아직 없다. 기존 `adminJobManager`는 process-memory 전용이므로 Cloudflare operator 단계에서 D1 영속 control plane으로 대체해야 한다.
- Worker `bridge.health`는 D1/Vectorize binding 존재 여부만 보고하며 데이터 기능 readiness 증명이 아니다.
- Cloudflare Container artifact(render script, Dockerfile, deploy 문서)는 Stage 5 산출물로 남아 있다.

## 다음 단계 영향

- Stage 3은 D1 canonical provider와 `deploy/cloudflare/migrations/*.sql`을 추가하고, 이 단계의 bridge `d1.query`/`d1.batch` operation을 실제 D1 handler로 구현한다. 그 시점에 write guard를 provider-neutral capability로 교체하고 `cloudflareParityComplete`의 `C` gate를 충족시킨다.
- Stage 4는 `vector.*` operation을 Vectorize handler로 구현하고 outbox/fallback/dedup/rebuild를 붙인다.
- Stage 5는 `admin.*` operation, Container artifact, account-neutral render script와 운영 문서를 추가하고 마지막으로 `cloudflareParityComplete`를 켠다.
- Stage 3 이전까지 Cloudflare profile은 bootstrap으로만 존재하며, local MariaDB + ChromaDB runtime이 계속 유일한 완전한 product runtime이다.

## 승인 요청

- Stage 2 산출물과 검증 결과를 검토·승인해 주세요.
- Stage 3을 시작하려면 **`Stage 2 승인, Stage 3 시작`**이라고 명시해 주세요.
