# 구현계획서 — Cloudflare Containers 기반 D1·Vectorize 배포 옵션 추가

수행계획서: [task_m470_1.md](task_m470_1.md)

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

마일스톤: M470

## parity 원칙

Cloudflare profile은 MariaDB + ChromaDB local runtime의 축소판이 아니다. 현재 노출된 사용자 기능과 운영 기능은 모두 functional-parity target이며, `C`(D1 canonical), `V`(Vectorize semantic consistency), `O`(operator/admin) 표기는 구현·검증 gate일 뿐 지원 제외 disposition이 아니다. gate가 미완료인 동안에는 Cloudflare option을 ready deployment로 제공하지 않는다. 기존 local default·public API·behavior는 전 단계에서 보존한다.

## 단계 개요

| Stage | 제목 | 주요 산출 | 검증 |
|---|---|---|---|
| 1 | MariaDB capability parity inventory와 gate map | Store/consumer/table/SQL-semantic matrix, P/C/V/O gate | static inventory cross-check 및 local baseline tests |
| 2 | Provider-neutral bootstrap과 Worker bridge contract | Cloudflare runtime config, generic Go bridge client, versioned Worker RPC | Go bridge/config contract tests 및 Worker static type/config check |
| 3 | D1 canonical parity | full D1 schema/provider, canonical persistence/read/update/rollback/reference/session contract | Store/httpapi parity, transaction/idempotency/session isolation tests |
| 4 | Vectorize semantic parity | Vectorize provider, outbox, fallback/dedup/rebuild/reprocessing/recovery | eventual-consistency, recovery and Chroma compatibility tests |
| 5 | Operator parity·Container artifact·운영 문서 | admin/migration/recovery/source/canon procedures, Container build, deployment docs | operator contract, full regression, artifact/account-neutral review |

## 문서 위치 확인

| 파일 | 수행계획서상 선택 위치 | Stage 산출물 경로 | 일치 여부 | 비고 |
|---|---|---|---|---|
| 사용자 진입 문서 | `README.md` | `README.md` | OK | Stage 5에서 Cloudflare option과 parity/readiness boundary를 추가한다. |
| Cloudflare 운영 문서 | `deploy/cloudflare/README.md` | `deploy/cloudflare/README.md` | OK | Worker/Container artifact, binding, parity migration/recovery 절차를 함께 둔다. |
| 내부 계획·보고 | `mydocs/` | `mydocs/plans/`, `mydocs/tech/`, `mydocs/working/`, `mydocs/report/` | OK | task artifacts only; 제품 문서로 취급하지 않는다. |

## Stage 1 — MariaDB capability parity inventory와 gate map

### 산출물

수정:

- `mydocs/tech/task_m470_1_mariadb_capability_inventory.md`
- `mydocs/working/task_m470_1_stage1.md`
- `mydocs/plans/task_m470_1.md`
- `mydocs/plans/task_m470_1_impl.md`

### 변경 내용

- `store.Store` base method와 public optional interface 84개, MariaDB implementation 26개, migration/schema, runtime consumer를 계속 전수 기록한다.
- `R/D/U` first-release disposition을 제거하고 `P`(functional parity target)와 `C`/`V`/`O` implementation gate를 분리한다. 일반 사용자 capability와 local runtime이 제공하는 operator capability 모두 P다.
- lorebook, rollback/reroll, memory admission/outbox/recovery, status/world projection, persona/entity memory, explorer mutation, reference/canon/source discovery, migration/worldline을 난도만으로 unsupported/profile guard 처리하지 않는다.
- `ShadowStatusReporter` 같은 local dual-write 내부 진단만 N/A로 기록한다. Cloudflare product route의 capability absence, silent no-op, partial write는 허용하지 않는다.
- Stage 1은 D1 schema, Go provider, Worker, Container source/artifact를 만들지 않는다. Stage 2 이후 구현 순서는 gate map이 정하되 어떤 P row도 permanent unsupported로 축소하지 않는다.

### 검증

```bash
cd go-service
go test ./internal/store ./internal/httpapi -count=1
cd ..
git diff --check
```

- interface 84개와 production MariaDB implementation 26개가 matrix에 모두 있는지 다시 cross-check한다.
- 모든 row에 implementation source, consumer, table/transaction dependency, P와 C/V/O gate가 기록됐는지 review한다.

### 커밋

```text
Task #1 Stage 1: Cloudflare parity gate map 정정
```

## Stage 2 — Provider-neutral bootstrap과 Worker bridge contract

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

- 기존 local runtime profile/store/vector mode의 default와 validation을 유지하면서 명시 opt-in Cloudflare profile과 provider-neutral dependency/health representation을 추가한다.
- Go bridge client는 virtual-host outbound binding만 사용한다. timeout, versioned request/response envelope, status-to-error mapping은 generic operation contract로 고정하고 Cloudflare API token·D1/Vectorize SDK를 Go에 넣지 않는다.
- Worker는 Container request를 virtual hostname으로 받아 D1/Vectorize binding handler로 분기한다. Stage 2에서 canonical query/transaction, vector mutation/query, operator job/administration을 전달할 수 있는 authorization context와 error contract를 정하되 data feature를 구현하지 않는다.
- tracked Wrangler template에는 stable binding names와 non-account-specific settings만 둔다. account ID, D1 resource ID, route/domain, token/secret은 넣지 않으며 ignored/local or protected values로 temporary deploy config를 생성한다.
- health/readiness는 local MariaDB/Chroma labels와 behavior를 보존한다. parity gates가 미완료인 Cloudflare profile은 functional deployment ready를 보고하지 않는다.

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
Task #1 Stage 2: Cloudflare bridge와 runtime bootstrap 추가
```

## Stage 3 — D1 canonical parity

### 산출물

신규 또는 확장:

- `deploy/cloudflare/migrations/*.sql`
- `go-service/internal/store/d1.go`, `d1_*.go` 및 동등한 D1 Store contract tests

수정:

- `deploy/cloudflare/worker/src/index.ts`
- `go-service/internal/store/store.go`
- `go-service/internal/httpapi/server.go`
- complete-turn, prepare-turn, rollback, state/world, persona, lorebook/reference, explorer, session route 및 관련 `*_test.go`

### 변경 내용

- D1 migrations와 Store provider는 모든 `C` gate canonical rows를 지원한다. immutable ledger만 구현하는 minimal slice는 Cloudflare parity release가 아니다.
- normal turn, prepare-turn lorebook/persona/state/world, entity identity, rollback/reroll, explorer mutation, reference/canon data, session/worldline data와 operator-readable canonical records가 local과 같은 observable result를 낸다.
- canonical write는 source revision/idempotency key와 outbox enqueue를 D1 transaction contract로 묶는다. MariaDB lock/upsert/last-insert-id SQL을 재사용하지 않으며 D1-compatible statement, deterministic IDs, retry policy를 사용한다.
- `O` capability의 authorization/audit procedure는 Stage 5에서 완료하되, 그 data model과 atomic persistence contract는 이 단계에서 제공한다.

### 검증

```bash
cd go-service
go test ./internal/store ./internal/httpapi ./internal/config -count=1
go test ./... -run 'D1|Cloudflare|PrepareTurn|CompleteTurn|Rollback|Lorebook|Persona|State|Session' -count=1
cd ..
git diff --check
```

- bridge mock으로 D1 batch/transaction failure, idempotent replay, source revision conflict, rollback, JSON/current projection, session isolation을 검증한다.
- MariaDB Store unit tests와 local runtime config tests가 그대로 통과함을 확인한다.

### 커밋

```text
Task #1 Stage 3: D1 canonical parity 추가
```

## Stage 4 — Vectorize semantic parity

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
- memory admission, precise memory, explorer mutation, session migration/recovery 및 관련 tests

### 변경 내용

- Vectorize provider는 Go bridge를 통해 search/upsert/delete/query health를 호출하고 vector ID, embedding, minimal filter metadata만 송신한다. canonical text/payload는 D1에서 hydrate한다.
- outbox completion은 Vectorize mutation acknowledgement와 query visibility를 분리한다. D1 outbox retry/lease states, D1 recent-memory fallback, source revision filtering, stable vector-document ID deduplication을 적용한다.
- precise/admission memory, reprocessing, recovery, explorer supersession, session migration vector parity, delete/rebuild가 local Chroma 경험과 같은 결과를 내도록 한다.

### 검증

```bash
cd go-service
go test ./internal/vector ./internal/httpapi ./internal/store -count=1
go test ./... -run 'Vectorize|Outbox|Recall|Rebuild|Recovery|Dedup|Reprocess|Migration' -count=1
cd ..
git diff --check
```

- delayed Vectorize visibility, mutation failure/retry, duplicate ID, D1 fallback, session isolation, delete/rebuild, reprocessing/recovery와 Chroma regression을 mock contract로 검증한다.

### 커밋

```text
Task #1 Stage 4: Vectorize semantic parity 추가
```

## Stage 5 — Operator parity·Container artifact·운영 문서

### 산출물

신규:

- `deploy/cloudflare/container/Dockerfile`
- `deploy/cloudflare/container/.dockerignore`
- `deploy/cloudflare/README.md`
- `deploy/cloudflare/scripts/render-wrangler-config.mjs`
- 필요한 operator/admin artifact validation test 또는 script

수정:

- `README.md`
- `deploy/cloudflare/wrangler.template.toml`
- `deploy/cloudflare/.gitignore`
- admin reset/export, session migration/stitch/worldline, vector recovery/reindex, source discovery, canon administration, health/readiness route와 관련 tests
- 필요한 CI/build configuration

### 변경 내용

- reset, snapshot/export, migration/stitch/worldline, source discovery, canon registry/pack, repair/recovery와 maintenance job을 Cloudflare에서 동등하게 수행한다. Worker-admin authorization, audit, confirmation, idempotent job state, D1 recovery and Vectorize reconciliation을 구현한다.
- Container artifact는 Go backend를 stateless로 실행한다. product data, migration state, queue/job state, user setting은 image/layer/local filesystem에 두지 않는다.
- deploy renderer는 account/resource IDs, routes, secrets를 ignored/protected input에서 temporary config로만 materialize한다. README/runbook은 normal deployment와 admin recovery/migration procedures, local compatibility와 remote-test boundary를 구분해 문서화한다.

### 검증

```bash
node --check "Archive Center.js"
cd go-service
go test ./... -count=1
cd ..
# project-standard container build command and Worker static check
git diff --check
```

- operator authorization/confirmation/audit, reset/export, migration/recovery/source/canon job contracts, Container restart persistence boundary, tracked artifact account-neutral scan, full local regression을 검증한다.
- Cloudflare credentials가 있으면 authenticated remote integration을 추가로 실행한다. 없으면 remote execution은 문서화된 미실행 boundary로 남기며 성공을 주장하지 않는다.

### 커밋

```text
Task #1 Stage 5: Cloudflare operator parity와 배포 문서 추가
```

## 공통 리스크와 금지 사항

- D1/SQLite concurrency는 MariaDB의 `FOR UPDATE`, MySQL upsert, JSON predicate와 다르다. 각 `C` row마다 transaction/retry/idempotency test 없이는 parity 완료로 선언하지 않는다.
- Vectorize eventual consistency는 D1 fallback/outbox/dedup/rebuild로 다룬다. Vectorize를 canonical truth로 쓰거나 immediate readback을 완료 조건으로 요구하지 않는다.
- Container은 Cloudflare API token REST를 호출하지 않고 Worker bridge만 사용한다.
- account-specific ID, secret, domain/route를 tracked source/template/image에 넣지 않는다.
- `ErrNotEnabled`/profile guard는 local-only shadow diagnostic 또는 권한 없는 관리 호출의 명시 응답에만 사용한다. 동일 경험 target인 Cloudflare capability의 구현 생략 수단으로 쓰지 않는다.

## 승인 요청 사항

- 수정된 Stage 1 parity model(P/C/V/O)과 5개 Stage 순서, 파일 범위, verification gate, commit boundary를 승인해 주세요.
- Stage 1 정정 후 Stage 2로 진행하려면 **`Stage 1 정정 승인, Stage 2 시작`**이라고 명시해 주세요.
