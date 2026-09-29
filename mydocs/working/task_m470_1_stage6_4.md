# Stage 6.4 완료 보고 — Durable overlay 운영 관측 및 전체 회귀

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_impl.md`](../plans/task_m470_1_impl.md)
Stage: 6.4

## 단계 목적

D1 canonical durable Search overlay의 bounded correction 상태를 Search와 분리된 읽기 전용 관측으로 노출하고, Cloudflare readiness 및 prepare-turn trace에서 replica-lag 경고를 확인할 수 있게 한다. 마지막으로 public `VectorStore` 시그니처와 기존 readiness parity gate를 유지한 전체 회귀를 수행한다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `go-service/internal/vector/vector.go` | overlay correction cap(200), soft TTL(5분), hard alert TTL(15분)을 named internal policy constant로 추가했다. |
| `go-service/internal/vector/durable_search_overlay.go` | caller의 top-k와 분리해 항상 bounded 200-document D1 correction snapshot을 요청하도록 했다. |
| `go-service/internal/vector/canonical.go` | 빈 session ID가 observability 전용 aggregate snapshot임을 capability contract에 명시했다. |
| `go-service/internal/store/d1_canonical_vector_documents.go` | partial corrections를 비운 채 truncate 상태에서도 exact pending count와 oldest pending timestamp를 보존하도록 snapshot query를 확장했다. |
| `go-service/internal/store/d1_durable_search_overlay_test.go` | truncated snapshot이 correction은 노출하지 않지만 운영 timestamp는 보존하는지 검증했다. |
| `go-service/internal/httpapi/durable_search_overlay_observability.go` | pending count, oldest age, cap/read-failure, soft/hard TTL 상태를 읽기 전용으로 분류하고 `/ready` scalar check를 만든다. |
| `go-service/internal/httpapi/group_health.go` | Cloudflare readiness에 `durable_search_overlay` enabled/state/pending_count/oldest_pending_age/warning/error를 추가하고 degraded 상태를 반영했다. Bootstrap/parity 성공 gate는 변경하지 않았다. |
| `go-service/internal/httpapi/prepare_turn_recall.go` | prepare-turn vector trace에 session-scoped durable overlay 상태를 첨부했다. |
| `go-service/internal/httpapi/durable_search_overlay_observability_test.go` | soft/hard TTL, read failure, cap overflow, readiness scalar checks 및 prepare-turn trace를 검증했다. |
| `go-service/internal/vector/durable_search_overlay_test.go` | top-k와 독립적인 200-document correction bound를 검증하도록 갱신했다. |

## 본문 변경 정도 / 본문 무손실 여부

코드 작업이다. `VectorStore.Search`와 `VectorStore.Upsert`의 ChromaDB-compatible public signature는 변경하지 않았다. 관측은 D1 snapshot 읽기만 수행하며 outbox 상태·재시도·visibility mutation을 변경하지 않는다. local Chroma/fake profile에는 Cloudflare readiness check를 추가하지 않았고, 기존 `cloudflare_parity` bootstrap gate도 성공으로 바꾸지 않는다.

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
- PASS — `go test ./... -count=1` 전체 통과.
- PASS — durable overlay D1/vector/httpapi focused 및 전체 회귀 통과.
- PASS — `git diff --check` 통과.

## 잔여 위험

- aggregate readiness snapshot은 request-time D1 read이므로 D1 control-plane outage에서는 overlay state가 degraded로 보인다. 이는 accelerator-only 정상처럼 보이게 숨기지 않는 의도된 fail-visible 정책이다.
- initial 200 / 5분 / 15분 thresholds는 실 workload telemetry가 축적되면 별도 승인으로 재검토해야 한다.

## 다음 단계 영향

- 구현 Stage 6.1–6.4가 완료됐다. PR 게시 전 `task-final-report` 절차에서 최종 보고서, 오늘할일 완료 처리, publish branch 및 Open PR을 별도로 처리해야 한다.

## 승인 요청

- 작업지시자는 Stage 6.4까지의 연속 구현을 승인했다. 본 Stage 산출물 검토 후, 별도 최종 보고·PR 게시 진행 승인을 요청한다.
