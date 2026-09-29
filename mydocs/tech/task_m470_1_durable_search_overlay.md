# Task M470-1 — Vectorize durable Search overlay 설계

## 조사 배경

Vectorize는 mutation acknowledgement 뒤 ANN search가 잠시 이전 replica 또는 빈
결과를 반환할 수 있다. Stage 5.1은 `Upsert`와 exact readback에 caller-context
blocking facade 및 durable `visibility_pending` 상태를 추가했지만, `Search`는
여전히 accelerator 결과만 사용한다. 이 문서는 public `VectorStore.Search` 시그니처를
바꾸지 않고, D1의 canonical/outbox 진실로 그 짧은 공백을 보완하는 구현 전 설계다.

이 문서는 내부 기술 설계이며 사용자·운영자용 API/배포 문서를 새로 만들지 않는다.

## 조사 질문

- visibility-pending 문서가 Vectorize ANN 검색에 아직 없을 때도 어떤 결과를
  반환해야 하는가?
- pending upsert와 delete가 stale accelerator 결과를 각각 어떻게 보정·마스킹해야
  하는가?
- 기존 Chroma/Vectorize filter·정렬·오류 계약을 바꾸지 않고 합칠 수 있는가?
- 오래 지속되는 pending 상태를 조용히 누락시키지 않으면서 어떻게 관측·복구할
  것인가?

## 조사 대상

| 대상 | 이유 | 위치 |
| --- | --- | --- |
| public vector contract | 변경 불가 public signature와 optional capability 경계 | `go-service/internal/vector/vector.go` |
| current wrappers | canonical/exact-read fallback과 blocking visibility 범위 | `go-service/internal/vector/canonical.go`, `blocking.go`, `mutation_fence.go` |
| Vectorize provider | filter, score, 50-result metadata cap, stable sorting | `go-service/internal/vector/vectorize.go` |
| recall consumer | session별 Search 호출과 post-search source-revision filtering | `go-service/internal/httpapi/prepare_turn_recall.go` |
| D1 canonical manifest | active canonical 문서와 durable visibility marker | `go-service/internal/store/d1_canonical_vector_documents.go` |
| outbox processor | accepted write의 retry/complete semantics | `go-service/internal/httpapi/memory_vector_outbox_processor.go` |

## 발견 내용

### 현재 보장과 공백

- `visibilityBlockingStore.Upsert`는 visibility를 관찰한 뒤에만 성공을 반환한다.
  timeout/cancel은 `ErrVisibilityPending`으로 반환되고 outbox는 mutation을 재전송하지
  않은 채 D1에 `retryable + vector_visibility_pending`으로 기록한다.
- ordinary exact `GetDocuments`는 D1 canonical read와 accepted-write overlay로
  non-monotonic exact read를 가린다. outbox 검증은
  `AcceleratorExactDocumentReader`로 이 overlay를 우회한다.
- `Search`는 모든 wrapper에서 accelerator로 위임된다. 즉 persistence된 pending
  upsert가 ANN query에 보이지 않거나, pending delete가 stale ANN hit로 남을 수 있다.
- D1 canonical list는 completed upsert와 visibility-pending upsert를 함께 제공한다.
  Search overlay에는 전체 canonical rebuild manifest가 아니라 **현재 pending delta와
  tombstone**를 분리해서 읽는 provider가 필요하다.

### 기존 검색 계약

- public API는 `Search(ctx, sessionID, vector, limit, filter)`이며 이를 확장하지
  않는다.
- recall filter가 실제로 해석하는 field는 session, `tier`, `source_table`의 string
  equality다. Vectorize는 flat AND equality만 전송하고, Chroma도 같은 세 필드를
  만든다.
- Vectorize score는 cosine similarity다. 결과는 similarity 내림차순, distance
  오름차순의 stable comparator를 거치며 metadata 전체 응답은 `topK <= 50`이다.
- `prepare-turn`은 session별로 Search한 뒤 source revision을 다시 검사한다. overlay도
  이 사후 revision fence를 우회하거나 대체하지 않는다.

## 결정

### 1. optional internal snapshot provider를 추가한다

`VectorStore`와 모든 public method signature는 변경하지 않는다. `vector` package에
Cloudflare composition에서만 쓰는 optional internal capability를 둔다.

```go
type DurableSearchOverlayProvider interface {
    DurableSearchOverlay(ctx context.Context, sessionID string, maxDocuments int) (SearchOverlaySnapshot, error)
}

type SearchOverlaySnapshot struct {
    Upserts      []VectorDocument // current active, visibility-pending documents
    TombstoneIDs []string         // current canonical delete/inactive documents
    PendingCount int
    OldestPendingAt time.Time
    Truncated     bool
}
```

D1은 한 transaction/snapshot 범위에서 다음을 계산한다.

1. active source revision의 최신 upsert 중 `retryable +
   vector_visibility_pending`인 document를 `Upserts`로 낸다.
2. latest effective operation이 delete이거나 source revision이 inactive인 document ID를
   `TombstoneIDs`로 낸다.
3. 같은 ID에 upsert와 tombstone이 공존하면 tombstone이 우선한다.
4. `maxDocuments + 1`을 읽어 overflow를 `Truncated=true`로 명시한다. partial snapshot은
   절대 검색 결과로 사용하지 않는다.

D1 query는 기존 `ListCanonicalVectorDocuments`를 재사용하지 않는다. 최신 operation,
source revision lifecycle, delete mask를 하나의 snapshot 규칙으로 명시해 오래된 upsert가
newer delete를 되살리지 못하게 한다.

### 2. Search wrapper는 accelerator result와 durable delta를 합친다

`NewDurableSearchOverlayVectorStore(accelerator, overlayProvider)`는 provider가 있을 때만
생성한다. composition은 다음 순서를 사용한다.

```text
mutation fence
  └─ durable Search overlay
       └─ canonical lifecycle/exact-read wrapper
            └─ visibility-blocking Vectorize provider
```

`Search` 알고리즘은 다음과 같다.

1. accelerator `Search`를 기존 인수로 호출한다. `ErrNotFound`는 빈 accelerator set으로
   취급하고, 그 밖의 오류는 그대로 반환한다.
2. D1 overlay snapshot을 읽는다. snapshot이 `Truncated`이면
   `ErrDurableSearchOverlayOverloaded`를 반환한다. 일부 pending doc만으로 성공을 꾸며서는
   안 된다.
3. accelerator result에서 tombstone ID를 제거한다.
4. pending upsert는 session과 기존의 `tier`/`source_table` equality filter를 모두 통과한
   경우에만 후보로 넣는다. 이를 위해 filter parsing helper를 provider 공용 pure helper로
   추출한다. 지원하지 않는 filter를 새로 넓게 해석하지 않는다.
5. pending doc의 embedding과 query vector가 같은 dimension이고 norm이 0이 아니면 Go에서
   cosine을 재계산한다. `SimilarityAvailable=true`, `SimilaritySource`는
   `cosine_from_durable_search_overlay`, `Distance=1-cosine`으로 둔다. embedding 결함은
   후보를 추측해 넣지 않고 overlay snapshot 오류/metric으로 승격한다.
6. 같은 ID가 accelerator에도 있으면 canonical pending upsert가 그 후보를 교체한다.
   tombstone이 최우선이다. 이로써 이전 embedding을 가진 stale ANN hit를 반환하지 않는다.
7. accelerator의 기존 동일-score 순서는 유지한다. overlay-only tie는 ID 오름차순으로
   seed한 뒤 기존 stable comparator로 합쳐 결정론적으로 만든다. 최종 결과는 caller가
   요청한 `limit`으로만 자른다.
8. accelerator와 overlay가 모두 비면 `ErrNotFound`를 반환한다.

overlay는 읽기 전용이다. search 요청에서 mutation을 재시도하거나 outbox 상태를 완료로
바꾸지 않는다.

### 3. delete mask는 canonical truth에서 즉시 만든다

delete propagation은 positive `AwaitVisible`로 증명할 수 없다. 따라서 delete outbox가
queued/leased/retryable인 동안에도 canonical source revision이 inactive 또는 latest
operation이 delete면 Search overlay는 해당 ID를 즉시 mask한다. Vectorize delete가 실제로
사라졌음을 outbox가 확인할 때만 operation이 completed가 된다. 이 정책은 stale vector가
삭제된 기억을 회수하는 것보다 보수적이며 canonical truth와 일치한다.

삭제 visibility를 durable `visibility_pending`으로 따로 기록하거나 blocking facade의
`DeleteDocuments`에 absence waiter를 추가하는 것은 이번 overlay 단계의 선행 조건이
아니다. 그 확장은 delete retry attempt가 propagation delay로 소모된다는 실제 증거가
생겼을 때 별도 범위로 다룬다.

### 4. bounded correctness와 운영 상태를 명시한다

- initial hard cap은 **200 pending documents per session**이다. snapshot overflow는
  partial success가 아니라 `ErrDurableSearchOverlayOverloaded`이고 prepare-turn trace 및
  readiness가 degraded를 보여준다. cap 값은 named internal constant로 두고 테스트한다.
- pending state의 soft TTL은 **5분**, hard alert TTL은 **15분**이다. TTL이 지나도
  correctness overlay를 제거하지 않는다. 대신 pending count/oldest age/error state를
  health/readiness/trace에 노출하고 outbox observation retry가 계속 처리한다.
- readiness는 `durable_search_overlay`의 enabled/state/pending_count/oldest_pending_age를
  보고한다. pending 0 또는 soft TTL 이내는 healthy, soft TTL 초과·snapshot read failure·cap
  overflow는 degraded다. 이 상태가 `/ready`의 기존 bootstrap/parity gate를 성공으로
  바꾸지 않으며 `cloudflareParityComplete`는 여전히 false다.
- canonical snapshot read failure 또는 malformed pending embedding은 accelerator-only
  성공으로 숨기지 않는다. `Search` 오류로 전파해 stale/incomplete recall이 정상처럼
  보이지 않게 한다.

## 구현 단계와 검증 계획

| 단계 | 변경 범위 | 수용 기준 | 검증 |
| --- | --- | --- | --- |
| 6.1 | vector internal snapshot contract, D1 latest-op/upsert/tombstone query, D1 tests | upsert/delete precedence, source lifecycle, session isolation, overflow가 명시된다 | `go test ./internal/store ./internal/vector -count=1` |
| 6.2 | durable Search wrapper, shared filter helper, deterministic merge/rank tests | pending upsert 보정, stale hit 교체, delete mask, cosine/dimension/error/empty/tie가 검증된다 | `go test ./internal/vector -count=1` |
| 6.3 | Cloudflare composition, health/prepare-turn trace, production-shaped integration tests | wrapper order, cap/TTL/degraded state, Chroma/local regression, outbox non-mutation이 검증된다 | `go test ./internal/httpapi ./internal/vector ./internal/store -count=1` |
| 6.4 | full regression, Stage report | public signatures와 readiness gate가 유지된다 | `node --check "Archive Center.js"; cd go-service; go test ./... -count=1; cd ..; git diff --check` |

각 구현 단계는 별도 승인·단계 보고·커밋으로 진행한다. 현재 문서는 설계 승인 전이므로
코드, public API, D1 migration, 배포 문서를 변경하지 않는다.

## 비결정 / 보류

- 200-document cap과 5/15분 threshold는 initial operational default다. 실제 원격 workload
  telemetry로 바꾸려면 별도 승인과 compatibility 검토가 필요하다.
- Vectorize ANN 자체의 global read-after-write 보장, provider-wide score calibration,
  pagination을 이용한 unbounded pending scan은 이 설계가 주장하지 않는다.
- user-facing deployment runbook의 parity-complete 표기 변경은 하지 않는다.
- delete absence blocking facade는 이 단계에서 구현하지 않는다.

## 적용 영향

- 예상 수정 위치: `go-service/internal/vector/`,
  `go-service/internal/store/d1_canonical_vector_documents.go`,
  `go-service/internal/httpapi/server.go`, health/prepare-turn diagnostics 및 관련 tests.
- migration은 필요 없다. 기존 outbox status, last_error marker, document JSON, source revision
  lifecycle를 읽기만 한다.
- local Chroma profile에는 overlay provider를 주입하지 않으므로 동작을 바꾸지 않는다.

## 참고 링크

- [Stage 5 결과](../working/task_m470_1_stage5.md)
- [구현계획서](../plans/task_m470_1_impl.md)
- [Cloudflare 운영 경계](../../deploy/cloudflare/README.md)
