# Stage 6.2 완료 보고 — Durable Search merge wrapper

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_impl.md`](../plans/task_m470_1_impl.md)
설계: [`task_m470_1_durable_search_overlay.md`](../tech/task_m470_1_durable_search_overlay.md)
Stage: 6.2

## 단계 목적

Stage 6.1의 bounded D1 snapshot을 accelerator Search 결과와 read-only로 병합한다. visibility-pending upsert를 cosine score로 보충하고, durable tombstone으로 stale accelerator hit를 제거하며, public `VectorStore.Search` signature는 유지한다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `go-service/internal/vector/durable_search_overlay.go` | Search 결과를 D1 snapshot과 병합하는 optional wrapper, fail-closed overflow 및 기존 optional capability forwarding을 추가했다. |
| `go-service/internal/vector/durable_search_overlay_test.go` | pending upsert, filter, canonical replacement, tombstone, accelerator empty result, overflow, malformed vector를 검증했다. |
| `go-service/internal/vector/search_filter.go` | Chroma·Vectorize·overlay가 공통으로 쓰는 좁은 filter 해석·predicate를 분리했다. |
| `go-service/internal/vector/chroma.go` | 기존 Chroma filter 구성을 공통 helper로 전환했다. |
| `go-service/internal/vector/vectorize.go` | 기존 Vectorize filter 구성을 공통 helper로 전환했다. |
| `go-service/internal/vector/vector.go` | bounded overlay overflow를 표현하는 내부 package error를 추가했다. |

## 본문 변경 정도 / 본문 무손실 여부

코드 작업이다. `VectorStore`의 public method signature와 local Chroma profile 동작은 변경하지 않았다. overlay provider가 없으면 wrapper는 accelerator를 그대로 반환한다. Search는 D1 또는 outbox에 mutation을 쓰지 않는다.

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
- PASS — `go test ./... -count=1` 전체 통과. durable overlay unit test와 기존 Chroma/Vectorize regression을 포함한다.
- PASS — `git diff --check` 통과.

## 잔여 위험

- Stage 6.3 composition root 연결 전까지 Cloudflare runtime Search는 wrapper를 사용하지 않는다.
- bounded overlay의 pending count·oldest age와 soft/hard TTL 관측은 Stage 6.4에서 readiness/trace에 연결해야 한다.

## 다음 단계 영향

- Stage 6.3은 Cloudflare profile에서만 D1 provider와 Vectorize accelerator를 wrapper에 주입하고, local Chroma profile은 그대로 유지해야 한다.
- Stage 6.4는 snapshot의 pending count·oldest pending timestamp를 operator-readable readiness/trace 신호로 노출해야 한다.

## 승인 요청

- 작업지시자가 Stage 6.2–6.4 연속 진입을 이미 승인했다. Stage 6.2 산출물은 Stage 6.3 composition root 연결로 이어진다.
