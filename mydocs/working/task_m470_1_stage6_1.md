# Stage 6.1 완료 보고 — Durable Search overlay D1 snapshot

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_impl.md`](../plans/task_m470_1_impl.md)
설계: [`task_m470_1_durable_search_overlay.md`](../tech/task_m470_1_durable_search_overlay.md)
Stage: 6.1

## 단계 목적

Vectorize의 eventual-consistency 구간에서 accelerator 결과를 안전하게 보정할 수 있도록, D1에서 최신 outbox operation과 canonical source lifecycle을 단일 SQL snapshot으로 읽는 내부 계약을 만든다. 기존 `VectorStore` public signature는 변경하지 않는다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `go-service/internal/vector/canonical.go` | 내부 `DurableSearchOverlayProvider`와 bounded snapshot 타입을 추가했다. |
| `go-service/internal/store/d1_canonical_vector_documents.go` | session/document별 최신 operation을 판정해 visibility-pending upsert와 tombstone을 반환하는 D1 query를 구현했다. |
| `go-service/internal/store/d1_durable_search_overlay_test.go` | 최신 delete 우선, inactive source tombstone, session isolation, overflow fail-closed, bound validation을 SQLite/D1 harness로 검증했다. |
| `mydocs/tech/task_m470_1_durable_search_overlay.md` | 승인된 durable Search overlay merge·cap·readiness 설계를 기록했다. |

## 본문 변경 정도 / 본문 무손실 여부

코드 작업이다. 공개 `VectorStore`의 `Search`·`Upsert` 시그니처와 local ChromaDB 경로는 변경하지 않았다. snapshot은 초과 시 partial correction set을 노출하지 않고 `Truncated`으로 fail-closed 한다.

## 검증 결과

실행 명령:

```powershell
node --check "Archive Center.js"
cd go-service
go test ./... -count=1
cd ..
git diff --check
```

결과:

- PASS — JavaScript syntax check 통과.
- PASS — `go test ./... -count=1` 전체 통과. `internal/store` durable overlay SQL harness와 `internal/vector`을 포함한다.
- PASS — `git diff --check` 통과.
- 추가 집중 검증 PASS — `go test ./internal/store ./internal/vector -count=1` 통과.

## 잔여 위험

- Stage 6.2의 merge wrapper가 아직 연결되지 않았으므로, 현재 production `Search`는 snapshot을 아직 소비하지 않는다.
- D1 snapshot은 제공하지만 readiness/trace의 pending count·oldest age 및 soft/hard TTL 경고는 Stage 6.4에서 노출해야 한다.

## 다음 단계 영향

- Stage 6.2는 `Truncated` snapshot을 즉시 오류로 처리하고, upsert cosine score merge·tombstone masking·duplicate ID canonical precedence를 구현해야 한다.
- Stage 6.3은 Cloudflare composition root에 wrapper를 연결하되, mutation replay나 outbox completion을 Search에 추가해서는 안 된다.

## 승인 요청

- Stage 6.1 산출물과 검증 결과를 승인하면 Stage 6.2 durable merge wrapper 구현으로 진행한다.
