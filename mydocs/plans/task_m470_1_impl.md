# 구현계획서 — Cloudflare Containers 기반 D1·Vectorize 배포 옵션 추가

수행계획서: [task_m470_1.md](task_m470_1.md)

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

마일스톤: M470

## 단계 개요

| Stage | 제목 | 주요 산출 | 검증 |
|---|---|---|---|
| 1 | Provider-neutral bootstrap과 Worker bridge contract | Cloudflare runtime config, Go bridge client, Worker RPC surface | Go bridge/config contract tests 및 Worker static type/config check |
| 2 | D1 canonical 최소 수직 경로 | D1 migration, D1 Store, normal turn canonical persistence/read | Store/httpapi contract, idempotency, session isolation tests |
| 3 | Vectorize recall consistency | Vectorize provider, outbox lifecycle, fallback/dedup/rebuild | Vector/httpapi eventual-consistency and recovery tests |
| 4 | Container artifact·호환성·운영 문서 | Container build, readiness, deployment docs, full regression evidence | JS/Go full suite, artifact review, optional remote boundary |

## 문서 위치 확인

| 파일 | 수행계획서상 선택 위치 | Stage 산출물 경로 | 일치 여부 | 비고 |
|---|---|---|---|---|
| 사용자 진입 문서 | `README.md` | `README.md` | OK | Stage 4에서 Cloudflare option discovery 및 compatibility boundary를 추가한다. |
| Cloudflare 운영 문서 | `deploy/cloudflare/README.md` | `deploy/cloudflare/README.md` | OK | Worker/Container artifact와 binding·migration 절차를 함께 둔다. |
| 내부 계획·보고 | `mydocs/` | `mydocs/plans/`, `mydocs/working/`, `mydocs/report/` | OK | task artifacts only; 제품 문서로 취급하지 않는다. |

## Stage 1 — Provider-neutral bootstrap과 Worker bridge contract

### 산출물

신규:

- `go-service/internal/cloudflarebridge/client.go`
- `go-service/internal/cloudflarebridge/client_test.go`
- `deploy/cloudflare/worker/src/index.ts`
- `deploy/cloudflare/worker/package.json`
- `deploy/cloudflare/worker/package-lock.json`
- `deploy/cloudflare/worker/tsconfig.json`
- `deploy/cloudflare/wrangler.template.toml`
- `deploy/cloudflare/.gitignore`

수정:

- `go-service/internal/config/config.go`
- `go-service/internal/config/config_test.go`
- `go-service/internal/httpapi/server.go`
- `go-service/internal/httpapi/group_health.go`
- 필요한 `go-service/internal/httpapi/*_test.go`

### 변경 내용

- 기존 local runtime profile/store/vector mode의 default와 validation을 유지하면서, 명시 opt-in Cloudflare runtime profile과 provider-neutral dependency/health representation을 추가한다.
- Go bridge client는 virtual host outbound binding만 사용한다. bridge URL, request timeout, request/response envelope, status-to-error mapping은 환경변수/HTTP contract로 고정하고 Cloudflare API token·D1/Vectorize SDK를 Go에 넣지 않는다.
- Worker는 Container request를 virtual hostname으로 받으며 D1/Vectorize binding handler를 내부 route로 분기한다. Stage 1에서는 request validation, versioning, error mapping, health/ping surface와 mockable D1 bridge operation만 확정한다.
- tracked Wrangler template에는 stable binding names와 non-account-specific settings만 둔다. `account_id`, D1 resource ID, route/domain, token/secret은 넣지 않으며, `.gitignore`로 local/generated deploy config를 제외한다.
- health/readiness 응답은 local MariaDB/Chroma labels와 behavior를 보존한 채 cloud provider status를 별도 표현한다. 아직 D1/Vectorize가 준비되지 않은 Cloudflare profile은 ready가 되지 않아야 한다.

### 검증

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

### 커밋

```text
Task #1 Stage 1: Cloudflare bridge와 runtime bootstrap 추가
```

## Stage 2 — D1 canonical 최소 수직 경로

### 산출물

신규:

- `deploy/cloudflare/migrations/0001_canonical_vertical_slice.sql`
- `go-service/internal/store/d1.go`
- `go-service/internal/store/d1_chat_memory.go`
- `go-service/internal/store/d1_memory_derivation.go`
- `go-service/internal/store/d1_test.go`
- `go-service/internal/store/d1_chat_memory_test.go`
- `go-service/internal/store/d1_memory_derivation_test.go`

수정:

- `deploy/cloudflare/worker/src/index.ts`
- `go-service/internal/store/store.go`
- `go-service/internal/httpapi/server.go`
- `go-service/internal/httpapi/group_turn_complete.go`
- `go-service/internal/httpapi/group_turn_prepare.go`
- `go-service/internal/httpapi/*_test.go` 중 D1 runtime contract가 필요한 파일

### 변경 내용

- D1 migration은 최초 vertical slice에 필요한 chat log, effective input, memory, evidence, source revision, memory-vector outbox와 최소 session-scoped indexes만 정의한다. MariaDB migration inventory는 변경하지 않는다.
- D1 Store는 existing `store.Store` contract를 충족하되, vertical-slice 밖 optional capability는 명시 capability check/`ErrNotEnabled`로 노출한다. unsupported path를 silent no-op으로 만들지 않는다.
- canonical write는 source revision/idempotency key 및 outbox enqueue를 D1 transaction contract로 묶는다. MariaDB lock/upsert/last-insert-id SQL을 재사용하지 않으며, D1-compatible statement와 deterministic IDs를 사용한다.
- complete-turn 및 prepare-turn에서 D1 profile이 normal chat persistence와 canonical recall materialization을 수행하도록 연결한다. MariaDB/fixture/noop routes와 policy는 변경하지 않는다.

### 검증

```bash
cd go-service
go test ./internal/store ./internal/httpapi ./internal/config -count=1
go test ./... -run 'D1|Cloudflare|PrepareTurn|CompleteTurn|Memory' -count=1
cd ..
git diff --check
```

- bridge mock으로 D1 batch/transaction failure, idempotent replay, source revision conflict, rollback, session isolation을 검증한다.
- MariaDB store unit tests와 local runtime config tests가 그대로 통과함을 확인한다.

### 커밋

```text
Task #1 Stage 2: D1 canonical vertical slice 추가
```

## Stage 3 — Vectorize recall consistency

### 산출물

신규:

- `go-service/internal/vector/vectorize.go`
- `go-service/internal/vector/vectorize_test.go`
- `go-service/internal/vector/vectorize_recovery_test.go`

수정:

- `deploy/cloudflare/worker/src/index.ts`
- `go-service/internal/vector/vector.go`
- `go-service/internal/httpapi/prepare_turn_recall.go`
- `go-service/internal/httpapi/memory_vector_outbox_processor.go`
- `go-service/internal/httpapi/group_turn_prepare.go`
- `go-service/internal/httpapi/*_test.go` 중 vector/outbox recovery contract가 필요한 파일

### 변경 내용

- Vectorize provider는 Go bridge를 통해 search/upsert/delete/query health를 호출하며, only vector IDs, embeddings, and minimal filterable metadata를 송신한다. canonical text/payload는 D1 read로 hydrate한다.
- Chroma expression filter에 직접 의존하지 않는 provider-neutral filter representation 또는 provider-side translation boundary를 만든다. existing Chroma behavior/test fixtures를 보존한다.
- outbox completion은 Vectorize mutation acknowledgement와 immediate query readback을 분리한다. D1 outbox retry/lease states, D1 recent-memory fallback, source revision filtering, stable vector-document ID deduplication을 적용한다.
- delete session과 rebuild는 D1 canonical rows를 source of truth로 삼으며, Vectorize query visibility delay가 user recall correctness를 손상시키지 않도록 한다.

### 검증

```bash
cd go-service
go test ./internal/vector ./internal/httpapi ./internal/store -count=1
go test ./... -run 'Vectorize|Outbox|Recall|Rebuild|Recovery|Dedup' -count=1
cd ..
git diff --check
```

- delayed Vectorize visibility, mutation failure/retry, duplicate ID, D1 fallback, session isolation, delete/rebuild and Chroma regression을 mock contract로 검증한다.

### 커밋

```text
Task #1 Stage 3: Vectorize recall consistency 추가
```

## Stage 4 — Container artifact·호환성·운영 문서

### 산출물

신규:

- `deploy/cloudflare/container/Dockerfile`
- `deploy/cloudflare/container/.dockerignore`
- `deploy/cloudflare/README.md`
- `deploy/cloudflare/scripts/render-wrangler-config.mjs`
- 필요한 Cloudflare artifact static validation test 또는 script

수정:

- `README.md`
- `deploy/cloudflare/wrangler.template.toml`
- `deploy/cloudflare/.gitignore`
- `go-service/internal/httpapi/group_health.go` 및 관련 tests
- 필요한 CI/build configuration

### 변경 내용

- Container artifact는 Go backend를 stateless로 실행하고, product data·migration state·queue/session state를 local filesystem에 쓰지 않도록 명시한다.
- Worker configuration template은 D1, Vectorize, Container binding 이름과 outbound virtual host mapping만 선언하며 account ID, resource ID, route/domain, secret/API token은 sample·source·image에 넣지 않는다.
- predeploy renderer는 CI protected variables 또는 gitignore된 local values만 읽어 tracked tree 밖 temporary Wrangler config를 만들고, deploy command는 그 temporary config만 사용한다.
- README는 Cloudflare option이 explicit opt-in이고 existing local MariaDB/Chroma mode와 공존함을 설명한다. Cloudflare guide는 account-neutral template, required deploy-time variable names, binding names, D1 migration, Worker/Container build/deploy, health verification, Vectorize rebuild, local/remote test boundary를 설명한다.
- provider-neutral readiness/health surface, artifact configuration and test results are aligned with the documented procedure.

### 검증

```bash
node --check "Archive Center.js"
cd go-service
go test ./... -count=1
cd ..
git diff --check
git status --short
```

- Worker/container configuration은 binding names, outbound host routes, no direct Go Cloudflare credential, no persistent container filesystem use를 review한다. tracked template/source/image에 account ID, resource ID, route/domain, token/secret이 없는지도 static check한다.
- Cloudflare account credentials와 resource mapping이 CI protected variables 또는 ignored local values로 제공된 경우에만 remote D1/Vectorize/Container integration smoke를 실행하고, 제공되지 않으면 local/mocked boundary와 미실행 이유를 final report에 기록한다.

### 커밋

```text
Task #1 Stage 4: Cloudflare deployment compatibility 문서화
```

## 검증

- 각 Stage 검증 명령은 해당 Stage 완료보고서 작성 전에 실행한다.
- 실패한 검증은 Stage 완료로 처리하지 않는다. 계획된 capability 범위나 artifact layout을 바꿔야 하면 구현계획서를 먼저 갱신하고 승인을 다시 받는다.
- all-stage integration evidence는 MariaDB/Chroma local regression, D1 canonical persistence, Vectorize fallback/dedup/rebuild, Container restart persistence boundary, provider-neutral readiness/health를 포함한다.
- remote tests are optional and must never be represented as executed without supplied Cloudflare credentials and observed results.

## 커밋

- 각 Stage는 source/artifact changes와 `mydocs/working/task_m470_1_stage{N}.md` 완료보고서를 함께 커밋한다.
- Stage commit은 순서대로 `Task #1 Stage {N}: ...` 형식을 사용한다.
- 구현계획서는 본 승인 요청을 위해 별도 커밋으로 보존한다.

## 단계 의존성

- Stage 2는 Stage 1의 runtime profile, bridge envelope, error mapping이 확정된 뒤에만 시작한다.
- Stage 3은 Stage 2의 D1 canonical rows/outbox contract가 검증된 뒤에만 시작한다.
- Stage 4는 Stage 3의 provider/readiness semantics가 검증되고 documentation procedure가 그 실제 artifact와 일치할 때만 시작한다.

## 위험과 대응

- **D1 Store contract broadness**: base Store and optional interfaces have wide surface area. Stage 2 uses explicit capability classification and tests every live vertical-slice call path; full parity is not silently claimed.
- **Worker bridge semantics**: D1 transaction/batch and Vectorize API response/error shapes may differ from assumptions. Keep a versioned envelope with contract tests before provider wiring.
- **Eventual Vectorize visibility**: do not gate canonical turn success on immediate vector query. Use D1 outbox state plus recall fallback/dedup and recovery tests.
- **Artifact drift**: Worker configuration, Container image, and documentation can diverge. Stage 4 static review must test declared host/binding names, account-neutral templates, and the absence of credential/resource embedding.
- **Account-specific configuration leakage**: `account_id`, D1 resource IDs, domains/routes, or secrets in committed files would bind the artifact to one account and leak deployment details. Use stable binding names, ignored/local or protected deployment values, and a temporary rendered config only.
- **Local compatibility regression**: run targeted and full Go tests plus JavaScript syntax validation; do not alter MariaDB/Chroma default selection.

## 승인 요청 사항

- 위 4개 Stage의 순서, 파일 범위, verification gates, and commit boundaries를 승인해 주세요.
- 계정 중립 `wrangler.template.toml`과 temporary deploy config renderer를 사용하고, account/resource IDs·routes·tokens·secrets를 source control에서 제외하는 deployment configuration boundary를 승인해 주세요.
- 특히 Stage 2의 D1 vertical slice 밖 Store capability는 explicit unsupported/provider guard로 남기고, full capability parity를 후속 task로 분리하는 것을 승인해 주세요.
- Stage 1부터 코드/artifact 변경을 시작해도 되는지 승인해 주세요.
