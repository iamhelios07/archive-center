# Task M470-1 Stage 1 완료 보고서

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

구현계획서: [task_m470_1_impl.md](../plans/task_m470_1_impl.md)

Stage: 1 — MariaDB capability parity inventory와 gate map

## 단계 목적

Cloudflare deployment option의 범위를 "최초 slice"로 임의 축소하지 않고, 현재 MariaDB Store의 실제 capability·schema·SQL/transaction semantic·runtime consumer를 전수 대조한다. Cloudflare가 local MariaDB + ChromaDB runtime과 같은 사용자·운영 경험을 제공하려면 어떤 capability가 parity target인지, 그리고 D1 canonical(`C`)·Vectorize consistency(`V`)·operator/admin(`O`) 중 무엇이 선행되어야 하는지 기록한다. 이 단계에서는 Worker, D1 schema, Vectorize, Container source/artifact를 만들지 않는다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| [task_m470_1_mariadb_capability_inventory.md](../tech/task_m470_1_mariadb_capability_inventory.md) | base Store, public optional interface 84개, production MariaDB implementation 26개, migrations/consumer/SQL semantic을 전수 기록하고 `R/D/U`를 parity target `P`와 implementation gate `C/V/O`로 재구성했다. |
| [task_m470_1.md](../plans/task_m470_1.md) | Cloudflare option의 user/operator functional parity, 단계별 parity gate와 출시 조건을 수행계획서에 반영했다. |
| [task_m470_1_impl.md](../plans/task_m470_1_impl.md) | D1 canonical, Vectorize semantic, operator parity의 Stage 2–5 구현·검증 경계를 재구성했다. |

## 정정 내용과 근거

초기 inventory의 `R/D/U`는 "최종 parity 대상"과 "구현 순서"를 섞었다. 그 결과 lorebook read, rollback/reroll, memory admission/outbox/recovery, status/world projection, persona/entity memory, explorer mutation, reference/canon/source discovery, session migration/worldline처럼 현 runtime에 노출된 capability를 D/U로 오해할 위험이 있었다.

정정 후 모든 현재 user/operator capability는 `P`다. `C`/`V`/`O`는 D1 transaction, Vectorize eventual consistency, Cloudflare operator procedure라는 구현·검증 순서만 뜻한다. local dual-write 내부 진단인 `ShadowStatusReporter`만 product capability가 아니므로 N/A로 남겼다. parity gate가 미완료인 Cloudflare profile은 route를 조용히 제거하거나 unsupported로 바꾸는 대신 deployment readiness를 통과할 수 없다.

## 본문 변경 정도 / 본문 무손실 여부

Stage 1 technical inventory와 승인 계획 문서만 정정했다. product source, MariaDB schema/migration, local MariaDB·ChromaDB runtime behavior, HTTP API와 configuration은 변경하지 않았다.

## 검증 결과

이번 Stage 1 정정 baseline 명령:

```bash
cd go-service && go test ./internal/store ./internal/httpapi -count=1
```

결과:

- `ok github.com/risulongmemory/archive-center-go/internal/store 0.970s`
- `ok github.com/risulongmemory/archive-center-go/internal/httpapi 55.540s`

정적 inventory cross-check 결과:

- public optional interface 84개 중 inventory 누락 0개.
- `mariadb.go`를 제외한 production `mariadb*.go` feature file 25개 중 누락 0개. `mariadb.go`는 별도 matrix row로 포함했다.
- 전체 production MariaDB implementation은 `mariadb.go` 포함 26개다.
- `git diff --check`는 Stage 1 최초 문서 커밋 시 통과했다. 이번 문서 정정에 대해서는 Stage 종료 검증에서 다시 실행한다.

## 잔여 위험

- MariaDB의 `FOR UPDATE`, MySQL upsert/`LAST_INSERT_ID`, JSON predicate, cross-table transaction/reset semantics는 D1/SQLite로 기계 이식할 수 없다. `C` gate마다 transaction/retry/idempotency proof가 필요하다.
- Vectorize eventual consistency, outbox acknowledgement, recent-memory fallback, ID dedupe, rebuild/recovery는 아직 구현하거나 remote Cloudflare 환경에서 검증하지 않았다.
- 운영 parity에는 단순 table migration 외에 Worker authorization, audit, confirmation, job/recovery procedure가 필요하다.

## 다음 단계 영향

- Stage 2는 모든 P capability를 전달할 수 있는 provider-neutral bootstrap과 Worker bridge contract만 구현한다. Cloudflare API token 또는 direct D1/Vectorize access를 Go backend에 추가하지 않는다.
- Stage 3–5의 완료 전에는 Cloudflare profile이 functional-parity ready deployment로 노출되지 않는다. MariaDB + ChromaDB local runtime은 계속 완전한 기본 runtime이다.

## 승인 요청

- 수정된 Stage 1 parity inventory와 `P/C/V/O` gate model을 검토·승인해 주세요.
- Stage 2를 시작하려면 **`Stage 1 정정 승인, Stage 2 시작`**이라고 명시해 주세요.
