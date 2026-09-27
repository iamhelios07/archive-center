# 수행계획서 — Cloudflare Containers 기반 D1·Vectorize 배포 옵션 추가

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

마일스톤: M470

## 목적

기존 MariaDB + ChromaDB 로컬 우선 런타임과 공개 API 동작을 보존하면서, Cloudflare Workers Paid 환경에서 실행할 수 있는 별도 배포 옵션을 추가한다. Cloudflare option은 선택된 일부 흐름만 제공하는 축소판이 아니라, local runtime과 같은 사용자·운영 경험을 제공하는 functional-parity deployment를 목표로 한다. 이 옵션에서 Cloudflare Container 안의 Go backend는 Worker의 outbound binding bridge를 통해서만 D1과 Vectorize에 접근한다.

D1은 모든 canonical product data의 권위 저장소이고, Vectorize는 D1에서 재구축 가능한 검색 가속 계층으로 한정한다. Container의 로컬 파일시스템은 캐시·임시파일만 허용하며, 재기동·sleep·cold start 뒤에도 사용자 데이터와 runtime state는 D1/Vectorize만으로 복구되어야 한다.

## 배경

현재 runtime은 MariaDB를 canonical truth, ChromaDB를 삭제·재구축 가능한 vector accelerator로 사용한다. 실제 prepare-turn 경로는 Store에서 canonical rows를 읽고 VectorStore를 recall 후보 검색에 사용하며, complete-turn은 canonical write와 memory-vector outbox를 수행한다.

MariaDB 전용 SQL transaction/lock 및 Chroma 전용 readiness·filter·즉시 readback 가정이 있다. 특히 Vectorize upsert의 query availability는 비동기이므로, current exact-readback 완료 판정을 그대로 이식하지 않고 D1 outbox 상태·recent-memory fallback·문서 ID deduplication으로 설계해야 한다.

## 범위

### 포함

- `store.Store`와 실제 runtime이 사용하는 optional Store capability의 Cloudflare functional parity. 사용자 기능뿐 아니라 reset, snapshot/export, migration/recovery, canon/source discovery, explorer mutation 같은 운영 기능도 동등한 결과를 제공한다.
- Provider-neutral runtime profile과 Worker bridge. Container는 virtual hostname outbound binding으로만 D1/Vectorize Worker handler를 호출한다.
- D1 canonical schema/migration, 모든 canonical persistence/read/update/rollback capability, 그리고 Cloudflare-safe transaction·idempotency·retry contract.
- Vectorize metadata contract, D1 outbox, eventual-consistency fallback/deduplication, delete/rebuild/recovery.
- Cloudflare operator/admin capability: 인증·권한, audit, destructive-action confirmation, migration/recovery/job procedures.
- 계정 중립 Cloudflare Worker/Container build·binding template, deploy-time resource mapping, runtime readiness/health, local compatibility regression tests와 deployment procedure.

### 제외

- MariaDB 또는 ChromaDB의 제거, default runtime 변경, 강제 data migration.
- Container에서 Cloudflare API token REST 호출로 D1 또는 Vectorize에 직접 접근하는 방식.
- 저장소에 특정 Cloudflare account ID, D1 database/resource ID, route/domain, token, secret을 커밋하는 방식.
- Container filesystem, R2, FUSE를 canonical database 또는 durable queue/state로 사용하는 방식.
- 검증 전 전체 MariaDB SQL을 SQLite/D1 SQL로 기계 변환하는 작업.
- 외부 유료 DB, VPS, Chroma Cloud 등 추가 의존성.

기술적 난도나 구현 순서는 기능 제외 근거가 아니다. 동일 경험을 유지하려면 현재 노출된 product/operation capability는 모두 parity 대상이며, 미완성인 Cloudflare profile은 기능을 profile guard로 숨기지 않고 not-ready 상태로 남는다. 영구 미지원은 별도 제품 범위 축소 승인 없이는 허용하지 않는다.

## 설계 방향

- 새 `RuntimeProfile`/store/vector provider 선택은 existing local profiles를 보존하고 Cloudflare profile을 명시 opt-in으로 추가한다. HTTP handlers는 D1/Vectorize SDK나 Cloudflare credential을 직접 알지 않는다.
- Worker는 D1·Vectorize bindings의 유일한 owner이고, Container backend에는 `outboundByHost` virtual host를 제공한다. Go는 `archive-d1`/`archive-vectorize` 같은 bridge host에 JSON RPC/HTTP 요청만 보낸다.
- 커밋하는 Worker configuration은 binding 이름과 계정-중립 template만 포함한다. account/resource ID와 secrets는 CI protected variables 또는 gitignore된 deploy config에서 주입하고, predeploy renderer가 tracked tree 밖 임시 config를 만들어 사용한다.
- D1 write는 idempotency key, source revision, outbox lease/state를 canonical table transaction 안에서 확정한다. MariaDB의 `FOR UPDATE`, `ON DUPLICATE KEY UPDATE`, `LAST_INSERT_ID`, JSON 표현식은 D1/SQLite semantics에 맞춘 별도 SQL과 concurrency contract로 대체한다.
- Vectorize에는 vector ID, embedding, session/source/tier/revision 등 최소 검색 metadata만 저장한다. text·authoritative payload는 D1에서 hydrate하고, recent D1 rows를 recall 결과에 병합한 뒤 ID로 deduplicate한다.
- reset/migration/recovery/source discovery 등의 operator capability는 route를 제거하지 않는다. Cloudflare에서 D1 canonical state, durable outbox/job state, restricted authorization, audit 및 confirmation으로 동등한 결과를 제공한다.
- readiness/health는 provider-neutral check 이름과 required/degraded policy를 노출하되, local MariaDB/Chroma check compatibility를 보존한다. parity gate가 하나라도 미완료면 Cloudflare deployment는 ready가 될 수 없다.

## 문서 위치 판단

제품 사용자가 Cloudflare 배포·binding·migration·검증 범위를 따라야 하므로, 최종 operator 문서는 기존 사용자 진입점인 `README.md`와 Cloudflare artifact와 함께 배치하는 `deploy/cloudflare/README.md`에 둔다. task의 조사·승인·단계 기록은 제품 문서가 아니므로 `mydocs/`에만 둔다. 새 공식 문서 루트를 만들지 않는다.

| 파일 | 분류 | 대상 독자 | 선택 위치 | 선택 이유 |
|---|---|---|---|---|
| `README.md` | 공식 문서 | 사용자·운영자 | `README.md` | 기존 설치·runtime architecture 진입점이다. |
| `deploy/cloudflare/README.md` | 운영 매뉴얼 | Cloudflare 운영자 | `deploy/cloudflare/README.md` | Worker/Container artifact, binding, parity verification·migration 절차를 함께 유지한다. |
| `mydocs/plans/task_m470_1*.md`, `mydocs/tech/`, `mydocs/working/` | 작업 산출물 | 작업지시자·내부 작업자 | `mydocs/` | 조사 근거, gate 및 단계 결과를 보존한다. |

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
- `go-service/internal/httpapi/server.go`, health/readiness, prepare-turn, complete-turn, admin/migration/recovery route 및 관련 tests
- `go-service/internal/store/store.go`
- `go-service/internal/vector/vector.go`
- `README.md`
- 필요한 build/package/CI 설정

## 단계

- **Stage 1 — MariaDB capability parity inventory와 gate map**
  - base/optional Store capability, MariaDB 구현·schema table·transaction/SQL semantics, HTTP/runtime consumer를 전수 매핑한다.
  - 각 capability의 parity 대상 여부와 `C`(D1 canonical), `V`(Vectorize consistency), `O`(operator/admin) implementation gate를 기록한다. 현재 product capability는 범위 단축을 위해 U로 분류하지 않는다.
- **Stage 2 — Provider-neutral bootstrap과 Worker bridge contract**
  - 모든 parity capability를 전달할 수 있는 versioned generic bridge, opt-in Cloudflare profile, provider-neutral readiness contract와 계정 중립 binding template을 구현한다.
- **Stage 3 — D1 canonical parity**
  - D1 migration 및 Store provider로 canonical data, normal turn, lorebook/persona/state/world, rollback, explorer, reference/canon/session data를 local과 동등한 결과로 구현한다. D1 transaction/retry/idempotency proof를 포함한다.
- **Stage 4 — Vectorize semantic parity**
  - Vectorize provider와 D1 outbox, recall fallback/deduplication, reprocessing/recovery, delete/rebuild 및 session isolation을 구현한다.
- **Stage 5 — Cloudflare operator parity·artifact·문서화**
  - reset/snapshot/export, migration/stitch/worldline, recovery/source discovery/canon administration과 권한·audit·confirmation/job procedure를 구현한다. Container artifacts, deployment/migration runbook, full parity regression과 account-neutral validation을 완료한다.

## 검증 계획

### 단계별 검증

- Stage 1: `store.go`, `mariadb*.go`, MariaDB migration/schema, runtime configuration과 HTTP consumer의 static inventory를 상호 대조하고 `cd go-service; go test ./internal/store ./internal/httpapi -count=1`을 실행한다.
- Stage 2: `go test ./internal/config ./internal/store ./internal/httpapi ./internal/cloudflarebridge -count=1`, bridge mock/version/error/configuration/readiness tests, Worker static type/config check.
- Stage 3: `go test ./internal/store ./internal/httpapi ./internal/config -count=1`, D1 canonical parity, idempotency, JSON/current projection, rollback/session isolation tests.
- Stage 4: `go test ./internal/vector ./internal/httpapi ./internal/store -count=1`, outbox retry, Vectorize delayed visibility, D1 fallback/deduplication, reprocessing/recovery/rebuild tests.
- Stage 5: operator/admin/migration/recovery route contract tests, Container build smoke, `node --check "Archive Center.js"`, `cd go-service; go test ./... -count=1`, account-neutral configuration scan, `git diff --check`.

### 통합 검증

- MariaDB + ChromaDB local runtime의 existing config, public API, tests가 유지된다.
- D1만으로 canonical data, migration state, outbox/job lifecycle을 복구할 수 있고 Vectorize는 D1에서 rebuild할 수 있다.
- Cloudflare Container restart가 product data, session state, migration state, queue state에 의존하지 않는다.
- Cloudflare route와 operator route가 local runtime에서 제공하던 observable result를 재현한다. 권한/confirmation은 Cloudflare의 안전한 동등 구현이지 기능 제거가 아니다.
- tracked template/source/image에 account ID, resource ID, secret, domain/route가 없고 deploy-time mapping은 protected/ignored values에서만 materialize한다.

## 주요 리스크

- **Parity 범위 과소평가**: optional interfaces 또는 admin route를 initial slice 밖으로 두면 사용자·운영 경험이 달라진다. Stage 1 gate map은 구현 순서만 정하고 기능 제외를 허용하지 않는다.
- **D1 SQL·동시성 차이**: lock/upsert/JSON semantics는 D1-specific transaction, deterministic ID, retry and idempotency proof가 필요하다.
- **Vectorize eventual consistency**: mutation acknowledgement와 query visibility를 분리하고 D1 fallback/outbox/rebuild로 recall correctness를 보장한다.
- **운영 parity 누락**: reset/migration/recovery job은 product data뿐 아니라 authorization, audit, confirmation, recovery 결과를 포함해 검증한다.
- **계정 귀속 configuration 유출 및 local regression**: 계정 중립 renderer와 full local regression으로 방지한다.

## 승인 요청 사항

- 모든 현재 user/operator capability를 Cloudflare functional parity 대상으로 하고, `C`/`V`/`O`를 구현 gate로만 사용하는 방향을 승인해 주세요.
- D1 authoritative / Vectorize rebuildable accelerator / Worker binding bridge / stateless Container 방향을 승인해 주세요.
- 계정-중립 template과 temporary deploy config renderer만 커밋하고 account/resource IDs·routes·tokens·secrets를 source control에서 제외하는 것을 승인해 주세요.
- 수정된 Stage 1 parity inventory를 검토한 뒤, 다음 단계는 **`Stage 1 정정 승인, Stage 2 시작`**으로 명시 승인해 주세요.
