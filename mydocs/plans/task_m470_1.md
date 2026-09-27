# 수행계획서 — Cloudflare Containers 기반 D1·Vectorize 배포 옵션 추가

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

마일스톤: M470

## 목적

기존 MariaDB + ChromaDB 로컬 우선 런타임과 공개 API 동작을 보존하면서, Cloudflare Workers Paid 환경에서 실행할 수 있는 별도 배포 옵션을 추가한다. 이 옵션에서 Cloudflare Container 안의 Go backend는 Worker의 outbound binding bridge를 통해서만 D1과 Vectorize에 접근한다.

D1은 모든 canonical product data의 권위 저장소이고, Vectorize는 D1에서 재구축 가능한 검색 가속 계층으로 한정한다. Container의 로컬 파일시스템은 캐시·임시파일만 허용하며, 재기동·sleep·cold start 뒤에도 사용자 데이터와 runtime state는 D1/Vectorize만으로 복구되어야 한다.

## 배경

현재 runtime은 MariaDB를 canonical truth, ChromaDB를 삭제·재구축 가능한 vector accelerator로 사용한다. 실제 prepare-turn 경로는 Store에서 canonical rows를 읽고 VectorStore를 recall 후보 검색에 사용하며, complete-turn은 canonical write와 memory-vector outbox를 수행한다.

읽기 전용 Phase 1 조사에서 MariaDB 전용 SQL transaction/lock 및 Chroma 전용 readiness·filter·즉시 readback 가정이 확인되었다. 특히 Vectorize upsert의 query availability는 비동기이므로, current exact-readback 완료 판정을 그대로 이식하지 않고 D1 outbox 상태·recent-memory fallback·문서 ID deduplication으로 설계해야 한다.

## 범위

### 포함

- `store.Store` 및 실제 사용하는 optional Store capability를 D1 provider로 단계적으로 지원하는 provider-neutral runtime profile 설계와 최소 수직 경로 구현
- Container에서 virtual hostname outbound binding bridge로 D1/Vectorize Worker handler에 요청하는 Go-side client contract
- D1 canonical schema/migration과 normal chat·memory persistence/prepare-turn recall·memory vector outbox의 최소 end-to-end vertical slice
- Vectorize metadata contract, asynchronous mutation handling, D1 fallback, session isolation, delete/rebuild capability
- 계정 중립 Cloudflare Worker/Container build·binding template, deploy-time resource mapping과 Cloudflare runtime readiness/health
- 기존 MariaDB/Chroma local runtime에 대한 unit/contract/compatibility regression tests
- Cloudflare deployment procedure 및 local/remote test boundary 문서화

### 제외

- MariaDB 또는 ChromaDB의 제거, default runtime 변경, 강제 data migration
- Container에서 Cloudflare API token REST 호출로 D1 또는 Vectorize에 직접 접근하는 방식
- 저장소에 특정 Cloudflare account ID, D1 database ID, API token, secret, domain/route를 커밋하는 방식
- Container filesystem, R2, FUSE를 canonical database 또는 durable queue/state로 사용하는 방식
- 검증 전 전체 MariaDB SQL을 SQLite/D1 SQL로 기계 변환하는 작업
- 모든 reference library·admin/migration/session-repair capability를 최초 vertical slice에 포함하는 작업
- 외부 유료 DB, VPS, Chroma Cloud 등 추가 의존성

## 설계 방향

- 새 `RuntimeProfile`/store/vector provider 선택은 existing local profiles를 보존하고 Cloudflare profile을 명시 opt-in으로 추가한다. HTTP handlers는 D1/Vectorize SDK나 Cloudflare credential을 직접 알지 않는다.
- Worker는 D1·Vectorize bindings의 유일한 owner이고, Container backend에는 `outboundByHost` virtual host를 제공한다. Go는 `archive-d1`/`archive-vectorize` 같은 bridge host에 JSON RPC/HTTP 요청만 보낸다.
- 커밋하는 Worker configuration은 binding 이름과 계정-중립 template만 포함한다. account/resource ID와 secrets는 CI protected variables 또는 gitignore된 deploy config에서 주입하고, predeploy renderer가 tracked tree 밖 임시 config를 만들어 사용한다.
- D1 write는 idempotency key, source revision, outbox lease/state를 canonical table transaction 안에서 확정한다. MariaDB의 `FOR UPDATE`, `ON DUPLICATE KEY UPDATE`, `LAST_INSERT_ID`, JSON 표현식은 D1/SQLite semantics에 맞춘 별도 SQL과 concurrency contract로 대체한다.
- Vectorize에는 vector ID, embedding, session/source/tier/revision 등 최소 검색 metadata만 저장한다. text·authoritative payload는 D1에서 hydrate하고, recent D1 rows를 recall 결과에 병합한 뒤 ID로 deduplicate한다.
- Vector outbox는 Vectorize mutation 요청 성공과 eventual query visibility를 분리한다. 즉시 exact readback을 완료 조건으로 요구하지 않으며, 재시도·rebuild는 D1 canonical rows와 outbox 상태를 기반으로 한다.
- readiness/health는 provider-neutral check 이름과 required/degraded policy를 노출하되, local MariaDB/Chroma check compatibility를 보존한다.

## 문서 위치 판단

제품 사용자가 Cloudflare 배포·binding·migration·검증 범위를 따라야 하므로, 최종 operator 문서는 기존 사용자 진입점인 `README.md`와 Cloudflare artifact와 함께 배치하는 `deploy/cloudflare/README.md`에 둔다. task의 조사·승인·단계 기록은 제품 문서가 아니므로 `mydocs/`에만 둔다. 새 공식 문서 루트를 만들지 않는다.

| 파일 | 분류 | 대상 독자 | 선택 위치 | 대안 위치 | 선택 이유 |
|---|---|---|---|---|---|
| `README.md` | 공식 문서 | 사용자·운영자 | `README.md` | 새 `docs/` 루트 | 기존 설치·runtime architecture 진입점이므로 배포 옵션의 발견 경로로 적합하다. |
| `deploy/cloudflare/README.md` | 운영 매뉴얼 | Cloudflare 운영자 | `deploy/cloudflare/README.md` | `mydocs/manual/` | Worker/Container artifact와 binding·migration 절차를 함께 유지해야 하며, 반복되는 framework manual이 아니다. |
| `mydocs/plans/task_m470_1.md` 및 후속 task 문서 | 작업 산출물 | 작업지시자·내부 작업자 | `mydocs/` | 제품 문서 경로 | 승인 범위, 조사 근거, 단계 결과를 보존하는 내부 task 기록이다. |

## 예상 변경 파일

신규:

- `deploy/cloudflare/wrangler.template.toml`, `.gitignore`, deploy-time config renderer 또는 동등한 계정-중립 Worker configuration
- `deploy/cloudflare/worker/src/index.ts` 및 D1/Vectorize outbound bridge handler
- `deploy/cloudflare/container/Dockerfile` 및 Container runtime configuration
- `deploy/cloudflare/README.md`
- `go-service/internal/store/d1_*.go` 및 D1 provider contract/unit tests
- `go-service/internal/vector/vectorize_*.go` 및 Vectorize provider contract/unit tests
- `go-service/internal/cloudflarebridge/*.go` 및 request/response contract tests
- `deploy/cloudflare/migrations/*.sql` 또는 동등한 D1-specific migration inventory

수정:

- `go-service/internal/config/config.go` 및 관련 config tests
- `go-service/internal/httpapi/server.go`, readiness/health, prepare-turn recall, memory-vector outbox processing과 관련 tests
- `go-service/internal/store/store.go`
- `go-service/internal/vector/vector.go`
- `README.md`
- 필요한 경우 build/package/CI 설정

이번 task 산출물:

- `mydocs/orders/20260928.md`
- `mydocs/plans/task_m470_1.md`
- 승인 후 `mydocs/plans/task_m470_1_impl.md`
- 단계별 `mydocs/working/task_m470_1_stage{N}.md`
- 최종 `mydocs/report/task_m470_1_report.md`

## 잠정 단계

- **Stage 1 — MariaDB capability inventory와 D1 범위 결정**
  - `store.Store` base/optional interfaces, MariaDB 구현·schema table·transaction/SQL semantics, HTTP/runtime consumer를 전수 매핑한다.
  - 각 capability를 Cloudflare 첫 release의 required / deferred / unsupported profile guard / 후속 task 후보로 분류하고, 이 결과가 승인되기 전에는 D1 vertical slice를 확정하지 않는다.
- **Stage 2 — Cloudflare provider contract와 Worker bridge bootstrap**
  - Stage 1에서 확정한 required capability에 맞춰 provider-neutral config/readiness contract와 Worker bridge API를 구현한다.
  - local provider regression과 D1 bridge/store contract·idempotency·session isolation을 검증한다.
- **Stage 3 — D1 canonical vertical slice**
  - 승인된 inventory 결과에 따라 D1 migration 및 required canonical persistence/read path를 구현한다.
  - D1 Store capability classification, local provider regression, D1 bridge/store contract·idempotency·session isolation을 검증한다.
- **Stage 4 — Vectorize accelerator와 recall consistency**
  - Vectorize provider, canonical outbox lifecycle, recent-memory fallback, ID deduplication, delete/rebuild를 구현한다.
  - eventual consistency, failure/retry, D1 hydration, rebuild 및 local Chroma compatibility를 검증한다.
- **Stage 5 — Cloudflare deployment vertical slice·호환성·문서화**
  - Worker/Container artifacts, bindings, provider-neutral health/readiness, deployment/migration procedure를 구현·문서화한다.
  - artifact configuration review, container restart persistence boundary, full regression, local smoke 및 선택적 remote integration test를 검증한다.

## 검증 계획

### 단계별 검증

- Stage 1
  - `store.go`, `mariadb*.go`, MariaDB migration/schema, runtime configuration과 HTTP consumer의 static inventory를 상호 대조한다.
  - `cd go-service; go test ./internal/store ./internal/httpapi -count=1`로 inventory 기준선이 되는 local behavior를 확인한다.
  - capability별 consumer, canonical table, MariaDB-specific SQL/transaction assumption, D1 disposition이 빠짐없이 기록되었는지 review한다.
- Stage 2
  - `go test ./internal/config ./internal/store ./internal/httpapi ./internal/cloudflarebridge -count=1`
  - D1 bridge mock contract, configuration/readiness, idempotency, session isolation tests
- Stage 3
  - `go test ./internal/store ./internal/httpapi ./internal/config -count=1`
  - 승인된 capability matrix의 D1 persistence/read path, idempotency, rollback/session isolation tests
- Stage 4
  - `go test ./internal/vector ./internal/httpapi -count=1`
  - Vectorize outbox retry, D1 fallback, deduplication, delete/rebuild tests
- Stage 5
  - Worker configuration/binding static review와 Container build smoke
  - `node --check "Archive Center.js"`, `cd go-service; go test ./... -count=1`, `git diff --check`
  - cloud runtime health/readiness contract tests 및 선택적 authenticated remote integration test

### 통합 검증

- MariaDB + ChromaDB local runtime의 existing config, public API, tests가 유지된다.
- D1만으로 canonical data와 outbox lifecycle을 복구할 수 있고 Vectorize는 D1에서 rebuild할 수 있다.
- Cloudflare Container restart가 product data, session state, migration state, queue state에 의존하지 않는다.
- Cloudflare bindings configuration에 Container-side Cloudflare API token 또는 direct binding credential이 없고, tracked template/source/image에 account ID, resource ID, secret, domain/route가 없다.
- deploy-time account/resource mapping은 CI protected variables 또는 gitignore된 local config에서만 받아 tracked tree 밖의 temporary Wrangler config로 materialize한다.
- `git status --short`가 PR 준비 전 빈 출력이다.
- `git diff --check`가 경고 없이 통과한다.

## 리스크

- **Store capability 범위 과소평가**: Store optional interfaces가 많아 minimal D1 slice 밖의 route가 런타임 오류를 낼 수 있다. Stage 1에서 실제 consumer·table·SQL semantic까지 전수 inventory하고, 승인된 matrix에 따라 explicit `ErrNotEnabled`/profile guard를 테스트한다.
- **D1 SQL·동시성 차이**: MariaDB lock/upsert/JSON semantics를 그대로 사용하면 atomicity와 data integrity가 깨질 수 있다. D1-specific statements와 idempotency/retry contract를 별도 구현·검증한다.
- **Vectorize eventual consistency**: upsert 직후 검색이 되지 않을 수 있다. canonical D1 fallback, outbox retry, deduplication, asynchronous observability를 적용한다.
- **Cloudflare bridge contract mismatch**: Worker/Container networking and request-size/runtime limits가 Go expectations와 다를 수 있다. contract tests와 artifact configuration review, remote test boundary를 명시한다.
- **계정 귀속 configuration 유출**: `account_id`, resource IDs, routes, token/secret이 source/template/image에 들어가면 재사용성과 security가 깨진다. tracked template에는 binding 이름만 두고, CI protected variables 또는 ignored local values로 temporary deploy config를 생성하며 static check로 금지한다.
- **Local regression**: config/health refactor가 existing MariaDB/Chroma paths를 바꿀 수 있다. default/local profile regression tests와 full suite를 매 stage 실행한다.
- **D1 schema 규모**: 현재 MariaDB migration inventory는 넓다. Stage 1 capability inventory의 table/call-site/transaction classification을 먼저 승인받은 뒤 vertical slice boundary와 후속 task를 결정한다.

## 승인 요청 사항

- M470 기준으로 위 포함·제외 범위와 **5개 단계** 분할을 승인해 주세요.
- D1 authoritative / Vectorize rebuildable accelerator / Worker binding bridge / stateless Container 방향을 승인해 주세요.
- 계정-중립 template과 temporary deploy config renderer만 커밋하고, account/resource IDs·routes·tokens·secrets는 protected/ignored deploy-time values로 분리하는 것을 승인해 주세요.
- Stage 1이 MariaDB Store capability·consumer·table·MariaDB SQL/transaction semantics를 전수 inventory한 뒤 D1 required/deferred/unsupported disposition을 승인받도록 하는 것을 승인해 주세요. 이 승인 전에는 normal turn/prepare-turn/outbox만을 최초 D1 vertical slice로 고정하지 않습니다.
- `README.md`와 `deploy/cloudflare/README.md`를 최종 사용자/운영자 문서 위치로 사용하는 것을 승인해 주세요.

승인되면 `task_m470_1_impl.md`에서 단계별 산출물, 검증 명령, 커밋 메시지를 구체화한다.
