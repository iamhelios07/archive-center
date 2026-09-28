# Task M470-1 Stage 4 완료 보고서

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_impl.md`](../plans/task_m470_1_impl.md)
Stage: 4 — Vectorize semantic parity

## 단계 목적

local runtime의 벡터 가속기는 ChromaDB, Cloudflare 배포의 가속기는 Vectorize다. 구현계획서 205–207행이 요구하듯, **Cloudflare의 vector recall이 ChromaDB와 같은 observable result**를 내도록 provider를 이식하고, canonical text/payload는 D1에 남긴 채 vector ID·embedding·minimal filter metadata만 bridge로 송신한다.

Stage 3이 D1 canonical store의 capability 표면을 닫았다면, 이 Stage는 그 위에 **벡터 가속기 자체**를 닫는다. D1이 capability를 광고하면서 벡터를 못 쓰는 상태는 Stage 3에서 지킨 "capability를 unsupported로 강등"과 같은 결함이기 때문이다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `go-service/internal/vector/vectorize.go` (신규 1,026줄) | `VectorStore` + `DocumentDeleter`·`ExactDocumentReader`·`DocumentLister`·`ExactMetadataQuerier`·`VectorVisibilityWaiter` 구현. bridge client만 사용하며 두 번째 HTTP client를 만들지 않는다. |
| `go-service/internal/vector/vectorize_test.go` (신규 1,377줄) | 계약·유사도 정책·mutation·health·격리 테스트. |
| `go-service/internal/vector/vectorize_recovery_test.go` (신규 410줄) | startup recovery, rollback, outbox replay, float64, account neutrality. |
| `go-service/internal/vector/vectorize_isolation_test.go` (신규 205줄) | 세션 격리, 문서 계열 격리, 중복 document id, delete 멱등성. |
| `go-service/internal/vector/vector.go` | `VectorVisibilityWaiter` 계약 추가. 기존 계약 변경 없음. |
| `go-service/internal/cloudflarebridge/client.go` | `OpVectorGet` 추가. |
| `deploy/cloudflare/worker/src/index.ts` | `vector.upsert` / `vector.delete` / `vector.query` / `vector.get` / `vector.health` 구현. |
| `go-service/internal/httpapi/memory_vector_outbox_processor.go` | upsert 후 가시성 대기. |
| `go-service/internal/httpapi/memory_vector_outbox_visibility_test.go` (신규) | 비동기 store에 대한 outbox 계약 4건. |
| `go-service/internal/config/config.go` | `VectorAcceleratorConfigured` / `VectorAcceleratorEnabled` / `VectorAcceleratorName` / `VectorAcceleratorSelected`. |
| `go-service/internal/httpapi` 13개 파일 | 가속기 판정 predicate 치환(12곳) + `NewServer` wiring + trace 엔진 표기. |
| `chroma.go`, `chroma_recovery.go`, `mutation_fence.go` | **변경 없음.** |

## 본문 변경 정도 / 본문 무손실 여부

- **ChromaDB provider는 한 줄도 변경하지 않았다.** reference 구현은 그대로이고, D1/Vectorize 쪽만 새로 이식했다. 회귀 테스트도 그대로 통과한다.
- `go test ./... -count=1` 통과로 기존 Chroma 회귀가 무손실임을 확인했다.
- `Archive Center.js` 변경 없음.

## 검증 결과

구현계획서 213–217행의 검증 명령을 그대로 실행했다.

```bash
cd go-service
go test ./internal/vector ./internal/httpapi ./internal/store -count=1
go test ./... -run 'Vectorize|Outbox|Recall|Rebuild|Recovery|Dedup|Reprocess|Migration' -count=1
cd ..
git diff --check
```

결과:

```
ok  github.com/risulongmemory/archive-center-go/internal/vector     6.612s
ok  github.com/risulongmemory/archive-center-go/internal/httpapi   62.543s
ok  github.com/risulongmemory/archive-center-go/internal/store     13.755s

(두 번째 명령: FAIL 없음)

git diff --check EXIT=0
```

추가로 실행한 회귀와 범위 검증:

```bash
cd go-service
go test ./... -count=1 -timeout 900s        # FULL=0 — Chroma 회귀 포함 전량 통과
cd ../deploy/cloudflare/worker
npm run check                                # tsc --noEmit, EXIT=0
```

### mock contract 검증 (계획서 219행)

| 항목 | 상태 | 근거 |
|---|---|---|
| delayed Vectorize visibility | **수정** | `VectorVisibilityWaiter`. 변이 실험으로 확인. |
| mutation failure/retry | 충족 | `TestVectorizeBridgeErrorRetryabilityReachesTheOutbox`, `TestVectorizeOutboxReplaysADeleteAfterAPartialFailure` |
| duplicate ID | 충족 | `TestVectorizeUpsertIsIdempotentByDocumentID` (batch 내 중복 포함) |
| D1 fallback | 충족 | provider가 `vector.ErrNotFound`를 반환하고 호출부가 그 sentinel을 처리해 lexical fallback으로 내려간다. `TestVectorizeSearchSurfacesAMissingIndexAsNotFound` |
| session isolation | 충족 | `TestVectorizeRecallNeverCrossesASessionBoundary`, `TestVectorizeReferenceRecallDoesNotReturnSessionMemories` |
| delete / rebuild | 충족 | `TestVectorizeDeleteSession*`, `TestVectorizeRebuildRefusesInsteadOfPretending` |
| reprocessing / recovery | 충족 | `TestVectorizeStartupRecoveryReadsBackEveryRecoveredBatch`, `TestVectorizeTurnRollbackDeletesExactlyTheIdsItKnows` |
| Chroma regression | 충족 | Chroma 파일 무변경 + `go test ./... -count=1` 통과 |

Vectorize 테스트 36개 전부 PASS.

## 유사도 척도 — 이 Stage의 핵심 결정

겉보기에는 두 provider를 같게 만들 수 없을 것처럼 보였다. Chroma의 collection은 metric 없이 생성되어 L2 기본값이고, Vectorize는 cosine이다. 그런데 실제 recall 경로를 읽으니까 달랐다.

`chroma.Search`는 `include: embeddings`로 호출되고, 반환된 저장 embedding으로 Go가 cosine을 재계산해 inverse-distance 값을 **덮어쓴다**.

```go
doc.Similarity = inverseDistanceSimilarity(doc.Distance)   // chroma_distance_inverse
doc.Similarity = cosineSimilarity(vector, doc.Embedding)    // ← 이것이 최종값
doc.SimilaritySource = "cosine_from_query_and_stored_embedding"
```

즉 **Chroma recall이 실제로 갖는 유사도는 cosine**이고, Vectorize의 `score`도 cosine이다. production에는 이미 scale-aware threshold가 있었다(`prepare_turn_recall.go:3171` — `distance_inverse`로 끝나면 0.55, 아니면 0.30). 그러므로 양쪽에 같은 임계값이 맞다.

provider의 `SimilaritySource`는 `vectorize_cosine_score`로 끝나지 않는다. `distance_inverse`로 끝나면 cosine 0.4짜리 hit이 조용히 버려지고, 사용자는 local에서 보던 기억을 못 보며 그 이유를 알려주는 곳이 아무데도 없다. 이 Stage에서 가능한 가장 나쁜 버그라 테스트로 물었다(`TestVectorizeRecallEligibilityMatchesTheChromaCosineBand`).

## 발견한 결함

### 1. 비동기 store에서 문서가 영구히 인덱스에서 사라짐 (가장 심각)

outbox는 upsert 직후 `GetDocuments` readback을 하고 **exact count 1**을 요구한다. 동기 적용되는 쓰기를 전제한 검증이다. Vectorize는 mutationId를 돌려주고 나중에 적용하므로, ack 직후 readback은 정당한 이유로 아무것도 돌려주지 않는다.

```
failed readback   → attempt 1회 소모
attempts 소진     → MEMORY_VECTOR_RETRY_LIMIT_REACHED, 영구 park
```

**실제로 성공한 쓰기가 문서 미도달 상태로 영구 park되고**, 운영자에게는 retry limit 초과로 보인다. 조용한 데이터 손실이었다.

`vector.VectorVisibilityWaiter`를 optional capability로 추가했다. 쓰기를 비동기로 적용하는 store만 readback 전에 제한된 대기를 받고, **동기 store는 구현하지 않으므로 기존 경로가 그대로 남는다**(테스트가 고정). 일관성 모델에 대한 지식을 그 모델을 소유한 provider가 갖고, 호출자는 문서 관점에서 계속 동작한다.

변이 실험: waiter 호출을 비활성화하면 `vector upsert readback exact document count is 0`으로 retryable이 되고, 별도 테스트가 attempt 4회 뒤 `permanent` park로 끝남을 보인다.

### 2. `ChromaEndpoint == ""`이 12곳에서 "벡터 가속기 없음"을 의미함

production 12곳이 `strings.TrimSpace(s.Cfg.ChromaEndpoint) == ""`을 "가속기가 설정되었는가"의 proxy로 쓰고 있었다. Cloudflare에서는 `ChromaEndpoint`가 **의도적으로** 비어 있다(Vectorize가 bridge 경유 가속기이고, `Load`가 cloudflare vector mode에서 `ChromaEnabled=false`를 단언한다). 즉 12곳 전부 가속기가 있는 deployment에서 "가속기 없음"을 보고한다.

조용히 실패한다. recall이 lexical fill로 내려가고, 깨끗한 upsert가 skip되고, admin reindex가 할 일이 없다고 보고한다. 어느 것도 provider 부재를 알리지 않는다.

`VectorAcceleratorConfigured()` / `VectorAcceleratorEnabled()`로 대체했다. 두 predicate를 하나로 합치면 readiness 판정이 깨지는 것으로 드러났고(`TestHandleReadyAllowsLiveMariaDBAuthorityWhenStoreOpen`), 그래서 분리했다. "도달 가능한가"와 "돌리려던가"는 다른 질문이다 — core_lite fallback deployment은 둘 다 아니어야 하고, 거기에 남아 있는 `AC_CHROMA_ENDPOINT`가 그걸 뒤집으면 안 된다.

### 3. 내가 만든 contract가 인위적 제한 3건을 낳음

첫 worker들이 각각 독립적으로 보고했다 — "인덱스 dimension을 알려주는 operation이 없다", "`getByIds`가 없다". **둘 다 contract의 문제가 아니라 내가 contract를 좁게 쓴 결과였고, vendored `@cloudflare/workers-types`를 다시 보면 실제로 존재한다.**

```ts
public describe(): Promise<VectorizeIndexInfo>   // dimensions, vectorCount, processedUpToMutation
public getByIds(ids: string[]): Promise<VectorizeVector[]>
```

worker agent는 "binding이 index version을 노출하지 않는다"고 보고했는데, `describe()`는 인덱스 *이름*이 아니라 *geometry*를 준다. 결론이 맞고 근거가 틀렸고, 그 차이가 바로 결함 4번이었다.

해소된 것:

- **cold-start read-only 열거 실패.** 인덱스 차원은 생성 시 고정되고 Vectorize는 길이가 다른 query vector를 거부한다. Cloudflare Container가 scale-to-zero로 자주 cold start하므로 edge case가 아니라 상 commonplace였고, `DeleteSession`은 특히 자기 자신으로 prime할 수 없었다. 첫 provider agent는 이 실패를 "정직한 결과"로 고정하는 테스트를 썼다 — 올바른 판단이지만 **정직하면서 틀렸다.** 이제 dimension을 추론하지 않고 인덱스에 묻는다.
- **`GetDocuments`가 id당 왕복 1회 + 근사.** metadata filter는 ANN이라 인덱스가 가진 문서를 absent로 보고할 수 있고, outbox는 mutation 검증에 readback을 쓰므로 false absent는 중복 쓰기가 된다. `getByIds`는 keyed lookup이라 정확하다. 기존 테스트가 "id당 1회"를 고정하고 있어서 assertion을 **뒤집었다** — 근사 query 0회까지 함께 고정.
- **index-wide count가 불가능.** `describe()`가 `vectorCount`를 준다.

여전히 정직하게 실패하는 유일한 경우: Worker가 인덱스를 describe하지 못하고 이 프로세스가 한 번도 쓴 적 없는 경우. named error로 실패하고 조용한 빈 결과는 없다.

### 4. trace가 Cloudflare에서 Chroma라고 말함

gate를 provider 비의존으로 바꾼 뒤에도 `prepare_turn_recall`이 `source = "go_r2_chromadb_product_read"`, `chromadb_live_enabled = true`, `engine = "chromadb"`를 하드코딩하고 있었다. Cloudflare 프로필에서 trace에 거짓 엔진 이름이 들어가는 셈이고, 운영자가 그걸 근거로 "어느 인덱스를 고쳐야 하지"를 판단한다.

`VectorAcceleratorSelected()`를 분리했다. read shadow로 도는 MariaDB 배포도 "이 시스템은 어떤 엔진용인가"를 답해야 하므로 blank로 두면 라벨이 사라지고, 반대로 도달 가능성과 합치면 read shadow가 live read로 보고된다.

Chroma 값은 byte-identical하게 보존했다. response model이 source를 `go_r2_chromadb_product_read`와 비교해 live 여부를 판단하므로 바꾸면 모든 Chroma recall이 shadow로 강등된다. Cloudflare는 자기 값을 갖는다(`go_r2_vectorize_product_read`).

### 5. worker가 내 지시를 세 군데에서 반박함

제가 wire contract를 "확정"해 줬는데, agent가 vendored types를 근거로 실제 API를 확인해 **전부 다르다**고 보고했다. REST API 모양을 Workers binding에 적용한 것이었다.

| 지시 | 실제 binding |
|---|---|
| `VECTORIZE.delete(ids)` | `deleteByIds(ids)` — `delete` 없음 |
| upsert/delete → `{id}` 배열 | `{ mutationId }` |
| filter `{field, value}` / `{all:[…]}` | flat object, 각 필드 exact match, 전부 AND |

그리고 `returnMetadata`는 `"all"`이 필수다 — `"indexed"`는 긴 문자열을 잘라내는데 document text가 그 문자열이라 검색 결과 본문이 조용히 잘린다.

## 잔여 위험

- **remote Cloudflare는 검증하지 않았다.** credential이 없다. 모든 검증은 D1과 동일 엔진인 `modernc.org/sqlite` 하네스와 Worker `tsc`다. 실제 D1/Vectorize 왕복은 검증 범위 밖이다.
- **Worker 테스트 하네스가 이 repo에 없다.** `package.json`에 test 스크립트가 없고 `.github/workflows/ci.yml`도 worker를 검증하지 않는다. 따라서 Worker 조각은 **type-check만 되고 자동 테스트로 실행되지 않는다.** Vectorize 호출을 좁은 `VectorizePort`로 빼서 스텁 주입 표면은 늘려뒀지만, 도는 테스트가 없다. 없는 테스트를 있는 척하지 않는다.
- **Vectorize는 mutation acknowledgement와 query visibility를 분리하지 못한다.** upsert는 `{mutationId}`만 돌려주고, 삭제가 실제로 반영됐는지는 readback으로만 안다. `VectorVisibilityWaiter`가 이를 메우지만, 삭제는 readback이 "gone"을 확인해야 하므로 가시성 지연이 삭제 확인에 직접 영향을 준다.
- **`DeleteSession`은 원자적이지 않다.** 페이지 단위 열거 후 배치 delete이며, 중간 실패 시 세션이 부분 삭제된다. replay는 수렴한다(없는 id 삭제는 오류가 아님). 열거가 불완전하면 아무것도 삭제하지 않는다 — 반만 삭제된 세션도 recall에 응답하기 때문.
- **정렬 pin의 한계.** worldline의 `ORDER BY` 두 번째 키처럼, 이 provider에서도 정렬 키가 실제로 무는 곳을 측정했다. 무의미한 키는 parity를 위해 유지했고 이유를 주석과 보고서에 남겼다.
- **`Count("")`의 의미가 두 갈래다.** index-wide 값은 reference 라이브러리를 포함한 인덱스 전체이므로 session 개수가 아니다. canonical과의 drift 신호로만 쓰고 session 개수로 오해하면 안 된다는 것을 테스트가 고정한다.
- **Worker가 추가한 오류 코드가 contract 밖이다.** binding이 없을 때 `vector_binding_missing`을 추가했다. contract에 있던 두 코드 중 어느 것도 정직하게 설명하지 못한다(페이로드는 유효하고 Vectorize는 실패하지 않았으므로, 두 번째는 거짓말이면서 deploy 상태 문제에 영원히 재시도한다). 이미 존재하는 `bridge_not_configured`를 따랐다.

## 다음 단계 영향

- Stage 5(operator/admin·Container artifact·운영 문서)가 남았다. `cloudflareParityComplete` 상수는 Stage 4만으로 true가 되지 않으므로 **Cloudflare profile은 아직 functional-parity ready deployment으로 보고되지 않는다.**
- Worker 테스트 하네스를 Stage 5에서 세울지, CI에 worker 검증을 추가할지는 Stage 5 산출물(`deploy/cloudflare/scripts/render-wrangler-config.mjs`, operator/admin artifact validation)과 함께 결정해야 한다. 이 Stage에서는 test runner 하나를 위해 범위를 넓히지 않았다.
- `/ready`와 `/health`의 `vector_accelerator`·`vector_engine_policy` 값은 여전히 Chroma 이름을 쓴다. Stage 5의 operator surface 작업에서 `VectorAcceleratorName()`/`VectorAcceleratorSelected()`로 통일하는 것이 자연스럽다.
- `Cloudflare` 계정이 확보되면 위 "remote 미검증" 항목이 해소되어야 Stage 5 종료 조건에 들어간다.

## 승인 요청

- Stage 4 산출물과 검증 결과를 승인하면 Stage 5(Operator parity·Container artifact·운영 문서)로 진행한다.
