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
- `admin_reset_runs` 등 reset control plane의 D1 영속 상태·cursor·epoch는 이 단계에서 schema와 contract로 확정한다. 아래 "D1 FK-safe resumable reset 설계 확정"을 따른다.

### D1 FK-safe resumable reset 설계 확정

MariaDB `ResetAll`은 한 연결에서 `SET FOREIGN_KEY_CHECKS=0` 후 72개 테이블을 한 트랜잭션의 `DELETE FROM`으로 지운다. D1은 foreign key를 기본 강제하고 `batch()`로 원자성을 제공하지만, 대량 수정은 실행 한도를 넘길 수 있어 이 방식을 그대로 이식할 수 없다. 따라서 D1은 별도의 재개 가능한 reset state machine으로 구현한다.

실측 검증(SQLite 3.45.3, 저장소 의존성 `modernc.org/sqlite`와 동일 엔진)으로 확정한 제약:

- **child-first allowlist** — SQLite는 `ON DELETE CASCADE`를 지원하므로 부모를 먼저 지우면 자식이 조용히 함께 삭제된다. allowlist는 `mariaAdminResetTables`와 같은 child-first 순서를 유지하고 모든 이름을 명시한다. `WITHOUT ROWID`는 지원되나 현재 migration에 사용 사례가 없다.
- **bounded chunk + 커밋 cursor** — 1,000행을 커밋 단위 250행 4회로 나눠 삭제하는 것을 실측했고, cursor가 chunk마다 전진하므로 중단 후 재개가 가능하다. rowid/PK 범위 cursor를 쓰고 `OFFSET`을 쓰지 않는다.
- **`sqlite_*` 내부 객체 제외** — allowlist는 `sqlite_sequence`, `sqlite_schema`, `sqlite_autoindex_*`를 절대 포함하지 않는다. `sqlite_sequence`는 AUTOINCREMENT 테이블이 없으면 존재하지 않으므로 이름을 하드코딩해 지우지 않는다.
- **`sqlite_sequence`를 지우지 않는다** — MariaDB reset은 `TRUNCATE`가 아니라 `DELETE FROM`이므로 AUTO_INCREMENT 카운터가 보존된다. SQLite도 `DELETE FROM`이면 `sqlite_sequence`가 보존되어 id가 단조 증가한다. 반대로 sequence를 비우고 테이블이 비면 id가 1로 재시작해 parity가 깨진다.
- **`sqlite_autoindex_*`는 삭제 불가** — constraint가 만든 내부 인덱스이며 schema 객체다.
- **migration metadata 보존** — `mariaAdminResetTables`도 `schema_migrations`를 포함하지 않는다. Cloudflare reset도 migration/version metadata를 삭제 대상에서 제외한다.
- **ID 단조성** — MariaDB `BIGINT UNSIGNED AUTO_INCREMENT` 60개 테이블은 id를 재사용하지 않는다. D1 schema는 해당 테이블에 `AUTOINCREMENT`를 명시해야 하며, plain `INTEGER PRIMARY KEY`는 전체 삭제 후 id를 재사용하므로 금지한다.

state machine 계약:

1. **전역 maintenance lease + fencing token** — reset 중에는 다른 writer가 canonical row를 쓰지 못한다. lease와 fencing token은 D1에 영속하며 Container 재시작에도 유지된다.
2. **durable reset run record** — `admin_reset_runs`가 reset epoch, 대상 table allowlist 순서, 현재 table/index, 마지막 처리 key, 처리 행 수, 상태(`running`/`purging_vectors`/`completed`/`failed`), 오류, 재시도 횟수를 보관한다. 이 control-plane record는 application-data reset 범위에서 제외한다.
3. **per-chunk 짧은 batch + cursor checkpoint** — 각 chunk는 D1 `batch()`로 원자적으로 커밋하고, 성공 시에만 cursor를 전진시킨다. 실패한 chunk는 재시도하며 이미 커밋된 chunk는 다시 지우지 않는다.
4. **Vectorize purge와 epoch fence** — D1 삭제와 Vectorize purge는 원자적으로 커밋될 수 없다. reset epoch를 올리고 purge가 완료될 때까지 stale vector를 fence로 차단하며, purge/retry 상태를 D1에 영속해 재개한다. 완료 시에만 새 epoch의 데이터가 검색에 노출된다.
5. **presentation은 파생** — 기존 `adminJobManager`는 process memory 전용이라 stateless Container에서 reset truth를 소유할 수 없다. `/admin/jobs`는 D1 reset run을 읽어 보여주는 presentation일 뿐이며 truth는 D1이다.
6. **operator-visible semantics 유지** — 기존 `POST /admin/database-reset`은 confirmation token(`RESET_ARCHIVE_CENTER_DB`), `debug=true` 요구, authorization, audit 의미를 그대로 보존한다. 장시간 reset에서 진행 중 상태를 반환하더라도 UI가 `completed`일 때만 캐시를 비우고 성공을 표시하도록 provider-aware하게 맞춘다.

### 검증

```bash
cd go-service
go test ./internal/store ./internal/httpapi ./internal/config -count=1
go test ./... -run 'D1|Cloudflare|PrepareTurn|CompleteTurn|Rollback|Lorebook|Persona|State|Session' -count=1
cd ..
git diff --check
```

- bridge mock으로 D1 batch/transaction failure, idempotent replay, source revision conflict, rollback, JSON/current projection, session isolation을 검증한다.
- D1은 SQLite 엔진이므로, 저장소에 이미 있는 `modernc.org/sqlite` 의존성으로 동일 엔진 로컬 harness를 만들어 D1 dialect SQL(통합 schema, FK 강제, `batch()` 원자성, chunk cursor 재개, `AUTOINCREMENT` id 단조성, JSON predicate)을 검증한다. `PRAGMA foreign_keys=ON`을 명시해 D1과 같은 조건으로 실행한다.
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
- reset operator procedure는 Stage 3의 "D1 FK-safe resumable reset 설계 확정"을 따른다. confirmation/authorization/audit semantics를 보존하고, durable reset run 기반 start/status/resume API와 UI의 provider-aware 완료 처리를 완성한다.
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
