# Task M470-1 Stage 3 완료 보고서

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_impl.md`](../plans/task_m470_1_impl.md)
Stage: 3 — D1 canonical parity

## 단계 목적

MariaDB + ChromaDB local runtime과 **같은 사용자·운영 경험**을 Cloudflare(D1 + Vectorize) 배포 옵션에서 재현하기 위해, D1 store provider가 모든 `C` gate canonical row를 지원하도록 한다. 구현계획서 136행의 "D1 migrations와 Store provider는 모든 `C` gate canonical rows를 지원한다. immutable ledger만 구현하는 minimal slice는 Cloudflare parity release가 아니다"가 이 단계의 완료 기준이다.

이전 Stage 2까지는 provider-neutral bootstrap과 Worker bridge contract만 있었고, D1 store는 base `Store` 계약만 완성된 상태였다. 이 Stage에서 optional capability 표면 전체와 canonical 쓰기 경계를 닫는다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `go-service/internal/store/d1_*.go` (신규 40개) | D1 provider capability 구현 82개. base `Store` 외 optional capability 표면 전체. 총 50,971줄(구현+테스트). |
| `go-service/internal/store/capability_manifest.go` | capability 표면 77 → 83. manifest가 route assertion과 1:1이어야 한다는 계약을 유지. |
| `go-service/internal/store/capability_manifest_test.go` | route assertion guard가 좁은 정규식으로 표면을 과소보고하던 문제 정정(아래 "발견한 결함"). |
| `go-service/internal/store/d1_executor.go` | timestamp 레이아웃을 고정 밀리초 자릿수로 변경. `d1TimeInstant` 추가. |
| `deploy/cloudflare/migrations/002_reset_control_plane.sql` | reset control plane(lease·fencing token·epoch) schema. |
| `Archive Center.js`, MariaDB 경로, `internal/httpapi` | **변경 없음.** |

MariaDB provider 구현과 public API는 한 줄도 변경하지 않았다. D1는 새 provider이며, 모든 구현은 `mariadb_*.go`의 statement를 옮기는 것이었다.

## 본문 변경 정도 / 본문 무손실 여부

코드 작업이다. 사용자·운영 문서와 기존 provider의 동작은 건드리지 않았다.

- local default(MariaDB + ChromaDB), public API, 기존 route 동작: **보존.** `mariadb_*.go` 파일 변경 0건.
- D1의 실패는 조용한 fallback이 아니라 명시적 오류다. capability가 없으면 route가 503을 반환하고, capability가 있는데 statement가 실패하면 그 오류가 전파된다.
- 의도적으로 채택한 4가지 provider 간 차이(binary TEXT 비교, no-op UPDATE affected row, TEXT timestamp 정렬, JSON 스칼라 affinity)는 각각 테스트로 고정하거나 도달 불가로 논증했다. 차이를 감추지 않고 기록하는 것이 이 Stage의 규칙이었다.

## 검증 결과

구현계획서 165–173행의 검증 명령을 그대로 실행했다.

```bash
cd go-service
go test ./internal/store ./internal/httpapi ./internal/config -count=1
go test ./... -run 'D1|Cloudflare|PrepareTurn|CompleteTurn|Rollback|Lorebook|Persona|State|Session' -count=1
cd ..
git diff --check
```

결과:

```
ok  github.com/risulongmemory/archive-center-go/internal/store    12.797s
ok  github.com/risulongmemory/archive-center-go/internal/httpapi   60.173s
ok  github.com/risulongmemory/archive-center-go/internal/config     0.320s

ok  .../internal/cloudflarebridge  1.049s [no tests to run]
ok  .../internal/config             0.317s
ok  .../internal/httpapi            9.736s
ok  .../internal/store             12.551s
ok  .../internal/vector             0.147s

git diff --check EXIT=0
```

추가로 Stage 3 종료 시점의 회귀와 범위 검증:

```bash
cd go-service
go test ./... -count=1 -timeout 900s        # FULL EXIT=0 — MariaDB unit test와 local runtime config test 포함 전량 통과
gofmt -l internal/store/d1_*.go              # 출력 없음
go test ./internal/store -run TestCapabilityReportMeasuresEveryProviderAgainstOneSurface -count=1 -v
```

```
optional-capability coverage: D1 82/83, noop 10/83
```

### C-gate canonical row 전원 parity evidence

| 항목 | 결과 |
|---|---|
| optional capability 표면 | 83개 중 **82개 구현**. 미구현 1개는 `ShadowStatusReporter`로, local dual-write 내부 진단이라 D1 provider가 **절대 광고하면 안 되는** 항목이며 manifest 테스트가 별도로 고정한다(`the D1 provider must not advertise the local dual-write shadow reporter`). |
| canonical schema 테이블 | `001_canonical_schema.sql`의 **81개 테이블 전부** D1 capability에서 참조된다. 미참조 테이블 0개. |
| compile-time interface assertion | `d1Store`에 85개. manifest probe 83개와 일치(중복 assertion 포함). |
| stub 검사 | D1 capability 파일의 `ErrNotEnabled` 반환은 전부 nil 연결 가드다. capability를 광고하면서 본체를 비워둔 stub은 0개. |
| D1 테스트 | 403개 테스트 함수가 실제 SQLite(`modernc.org/sqlite`, D1와 동일 엔진) 위에서 `PRAGMA foreign_keys=ON`으로 실행된다. |
| reset control plane | `d1_maintenance_lease`·`d1_reset_epoch`·fencing token schema 존재. chunk cursor 재개(`TestD1ResetChunkedDeleteIsResumable`), 단일 active run 강제, reset 후 `AUTOINCREMENT` id 단조성(`d1_capabilities_test.go`) 검증. |
| `batch()` 원자성 | 실패 batch가 앞선 문장을 모두 롤백함을 검증(`TestD1BatchRollsBackAllStatements` 및 admission·canon·vector outbox 각 slice). |

## 발견한 결함

이 Stage를 진행하며 **capability 표면을 과소보고하던 결함**을 발견해 정정했다. 이 결함 없이는 아래 수치가 "완료"로 읽힐 수 없었다.

manifest guard의 정규식이 리터럴 `s.Store.(store.X)` 한 가지만 잡았다. 그러나 route는 보통 먼저 store를 필요한 authority로 좁힌 다음 그 값에 두 번째 capability를 assert하고, capability를 assert하지 않고 type switch로 분기하기도 한다.

```go
discoveryStore, ok := s.sourceDiscoveryAuthorityStore(w)
mutable, ok := discoveryStore.(store.SourceDiscoveryMutableStore)   // guard가 잡지 못함
switch s.Store.(type) { case store.WorldlineTopologySnapshotStore: }  // 역시 잡지 못함
```

결과적으로 manifest에 없던 public capability 6개가 드러났고, 그중 5개가 실제 route에서 assert되고 있었다. **그 시점의 Cloudflare는 local에서 동작하는 source discovery operator workflow에서 503을 반환하고 있었다.** 이는 Stage 1 inventory가 명시적으로 금지한 "P capability를 unsupported로 강등"에 해당한다.

이외 진행 중 발견해 수정한 실제 버그:

- **timestamp TEXT 정렬 역전.** `d1TimeLayout`이 `time.RFC3339Nano`였는데 이 레이아웃은 소수부 끝의 0을 제거한다. 정확히 정각인 시각이 `05:06:07Z`로 기록되고, 같은 초의 `05:06:07.5Z`와 TEXT로 비교하면 `'.' < 'Z'`라 **나중 instant가 먼저 정렬**된다. `ORDER BY updated_at DESC`가 뒤집히고 살아 있는 lease가 만료로 판정될 수 있었다. 고정 밀리초 자릿수로 변경했다.
- **생성 키가 SQL에 인용 없이 splice됨.** session migration의 alternate key map이 대상 값을 그대로 문장에 끼워넣었다. `memory_source_revisions.source_revision`은 생성 UUID이므로 실제 migration이 파싱 에러를 냈다. 양쪽 provider 모두에서 source revision을 가진 세션 migration을 실제로 실행해 본 적이 없었음을 드러낸 사례다.
- **`source_discovery_jobs.updated_at`가 갱신되지 않음.** MariaDB는 `ON UPDATE CURRENT_TIMESTAMP(3)`으로 암묵 갱신하지만 SQLite에 그 절도 trigger도 없다. 갱신하지 않으면 `FindLatestSourceDiscoveryJob`의 `ORDER BY updated_at DESC`가 해당 work의 **가장 오래된** 시도를 돌려주고, resume 경로가 버려진 시도를 조용히 재시작한다.
- **store가 저장 정밀도보다 높은 시각을 반환.** `d1TimeValue`는 밀리초로 자르지만 반환 `time.Time`은 나노초였다. 반환 record와 저장 row를 비교하는 호출자가 서로 다른 두 시각을 봤다.

## 승인받은 설계 결정

`MemoryAdmissionWriter`의 D1 트랜잭션 경계는 작업지시자 승인을 받아 **2-batch + 마커를 batch 2**로 확정했다.

MariaDB는 한 트랜잭션에서 `LastInsertId`로 생성 id를 얻고 그 id를 vector document id로 쓰고, 다시 그 document id를 SHA-256 해시해 `operation_key`를 만든다. D1Conn.Batch는 오류만 반환하고 문장 사이에 읽을 수 없다. subquery로 document id는 만들 수 있으나 SQLite/D1에 SHA-256 함수가 없어 operation key를 SQL에서 계산할 수 없다(로컬 테스트 하네스에만 등록하면 테스트는 통과하고 프로덕션은 실패하므로 불가능). `MAX(id)+1` 예측은 다른 Container가 끼어들면 존재하지 않는 row를 가리킨다.

```
batch 1  canonical row (memory, evidence, precise unit, dependency)
         생성 id는 커밋된 row에서 readback
batch 2  vector outbox operation + derived_admission_state committed 마커
```

마커가 커밋 지점이고 batch 2에 있는 것이 핵심이다. 두 batch 사이에서 프로세스가 죽으면 source revision이 pending으로 남아 부분 쓰기와 아무것도 안 쓰인 상태가 구별되지 않고, durable reprocessing이 같은 Critic result로 재실행하면 자기 치유한다. 이 자기 치유를 crash window 재현 테스트로 고정했고, `MemoryAdmissionProjectionInspector`로 해당 gap을 탐지할 수 있게 했다.

## 잔여 위험

- **cross-Container 동시성 미해결.** `memoryDerivationWriteMu`는 프로세스 로컬이다. 두 Cloudflare Container가 같은 turn을 동시에 커밋하면 D1은 batch 원자성과 자연키(`chat_session_id + turn_index`, turn 내 `evidence_text`, `unit_id`) 충돌에 의존한다. MariaDB의 `FOR UPDATE`와 같은 보장은 아니다. 모든 D1 쓰기 capability에 동일하게 기록되어 있다.
- **remote Cloudflare 미검증.** D1 credential이 없어 실제 D1/Vectorize에 대한 검증은 수행하지 않았다. 모든 검증은 D1과 동일 엔진인 `modernc.org/sqlite` 로컬 harness 위에서다. Worker bridge의 실제 왕복은 검증 범위 밖이다.
- **저장된 바이트 차이 가능성.** `evidence_json`의 MariaDB JSON 대 D1 TEXT, 그리고 일부 JSON 컬럼의 스칼라 affinity 차이가 남아 있다. 도달 불가로 논증한 항목은 코멘트에, 도달 가능한 항목은 테스트로 고정했다.
- **MariaDB에도 구현이 없는 계약.** `MemoryAdmissionProjectionInspector`는 선언만 있고 어떤 provider도 구현하지 않았다. inventory는 P/C gate row로 적었지만 consumer도 구현도 없었다. 이번에 D1이 처음 구현했다. **local runtime도 같은 갭을 갖고 있다는 뜻이며**, 요구를 삭제해 scope를 줄이는 것으로 처리하지 않았다.
- **정렬 pin의 한계.** worldline의 completed-turn `ORDER BY` 두 번째 키(`chat_session_id ASC`)는 이 SQLite 문장에서는 `GROUP BY`가 이미 세션 순서로 group을 내보내기 때문에 무의미하다. parity를 위해 유지했고, pin이 물지 않는 이유를 측정해 SQL 주석에 기록했다.

## 다음 단계 영향

- Stage 4는 Vectorize semantic parity다. 이 Stage에서 vector outbox(`MemoryVectorOutboxStore` + Lane + Maintenance), materialized completion, 삭제/retire 경로가 canonical row와 함께 커밋되도록 완성되었으므로, Stage 4는 Vectorize 자체의 recall 의미론과 outbox 소비에만 집중할 수 있다.
- `MemoryVectorMaterializedCompletionStore`는 벡터가 실제로 materialize되었는지 확인한 뒤에만 outbox operation을 complete한다. Stage 4가 이 경로 위에 Vectorize 실구현을 얹는다.
- Stage 5(operator/admin)의 `O` gate는 이 Stage에서 data model과 atomic persistence contract만 제공했다. authorization·audit **절차**는 Stage 5에서 완성한다.
- Cloudflare profile은 `V`와 `O` gate가 미완료이므로 **아직 functional-parity ready deployment으로 보고하지 않는다.** readiness는 false를 유지한다.

## 승인 요청

- Stage 3 산출물과 검증 결과를 승인하면 Stage 4(Vectorize semantic parity)로 진행한다.
