# Task M470-1 Stage 5 — Operator parity·Container artifact·운영 문서

## 결과

Stage 5 산출물을 모두 구현하고 검증했다. 이 단계에서 계획에 없던 결함 세 개가
발견되었고, 셋 다 테스트나 이미지 구동으로만 잡을 수 있었다.

계획서가 "성공을 주장하지 않는다"고 한 미실행 경계는 그대로 남는다. 다만 후속
원격 preview 검증에서 실제 D1·Vectorize binding을 통한 blocking write/readback 한 건을
확인했다. 이는 deploy가 아니며, 전체 product parity 완료 선언의 근거도 아니다.

## 무엇을 만들었나

### 배포 측 도구

| 파일 | 역할 |
| --- | --- |
| `deploy/cloudflare/scripts/render-wrangler-config.mjs` | account-scoped 식별자를 tracked 템플릿에서 임시 config로만 materialize |
| `deploy/cloudflare/scripts/check-artifact-neutrality.mjs` | 추적 아티팩트 14개에 account·route·credential이 없는지 검사 |
| `deploy/cloudflare/container/Dockerfile` | stateless Go backend, distroless |
| `deploy/cloudflare/container/.dockerignore` | 빌드 컨텍스트에서 secret과 account-scoped 파일 제외 |
| `deploy/cloudflare/README.md` | 정상 배포·복구·호환성·원격 경계 runbook |

### 마이그레이션

| 파일 | 내용 |
| --- | --- |
| `003_turn_preparation_settings.sql` | 로컬이 파일로 보관하던 사용자 설정을 D1로 |
| `004_admin_jobs.sql` | operator job 스냅샷과 상속 시 interrupted 처리 |

`001_canonical_schema.sql`은 MariaDB 마이그레이션에서 생성되는 빌드 산출물이라
손대지 않았다. D1 전용 테이블은 손으로 쓴 `002`와 같은 방식으로 분리했다.

### Go 계층

- `TurnPreparationSettingsStore` — multi-agent 설정과 body tracking. 분기는
  profile이 아니라 **capability** 기준이라 로컬 런타임의 검증된 경로가 그대로
  남는다.
- `AdminResetRunReader` — reset 진행을 D1에서 읽어 `/admin/jobs`가 presentation
  으로 보여준다.
- `AdminJobSnapshotStore` — 나머지 operator job의 스냅샷과 상속 처리.
- readiness에 `vector_accelerator`, `vector_engine_policy`,
  `vector_accelerator_reachable`, `turn_preparation_settings` 추가.
- startup preflight가 실제 의존성의 이름을 부른다.

## 발견한 결함

### 1. 공개된 관리 라우트

Cloudflare Container는 플랫폼이 인터넷에 공개 주소를 준다. `authMiddleware`는
`Auth.Enforce`가 false면 통과시키고 그 기본값이 모든 배포에 적용됐다. 결과는
`POST /admin/database-reset`이 아무에게나 응답하는 것이었다.

route의 confirmation token은 보완이 아니다. 바이너리에 컴파일된 상수라
오타는 막고 아무것도 권한 부여하지 않는다. **유일하게 공개되는 배포 형태가
유일하게 무방비한 형태**였다.

`Validate()`가 이제 `AC_ENFORCE_AUTH=true`와 비어있지 않은 `AC_BEARER_TOKEN`을
요구한다. 경고가 아니라 기동 실패다. 컨테이너에서 세 경우를 직접 확인했다.

### 2. 재시작 후 사라지던 진실

`adminJobManager`는 process memory 전용이고 Container는 재시작되지 않고
교체된다. 두 가지가 뒤섞여 있었다.

- **reset**은 파괴적 동작인데 목록에서 사라졌고, "reset이 실행된 적 없다"로
  읽혔다. 진실은 끝까지 D1 행에 있었다.
- **나머지 job**들도 그대로 사라졌다.

reset은 `d1_reset_runs`로 영속화했다. 나머지는 스냅샷을 저장하되
**재개하지 않고** `interrupted`로 되돌려 쓴다. 이 job들의 클로저는 새
인스턴스에 없는 in-process 상태를 캡처하고 progress 커서는 보고용이지 재개용이
아니다. 상속된 job을 `running`으로 되살리는 것은 더 편한 답이고 틀린 답이다.

### 3. preflight가 존재하지 않는 서버를 지목했다

이미지를 Cloudflare 프로파일로 돌리니 이렇게 죽었다.

```
"chromadb startup preflight failed (api_path=/api/v2)": connection refused
```

Cloudflare 배포에 ChromaDB도 `/api/v2`도 없다. `"chromadb"` 리터럴이 **세
경로에 나뉘어 있었고** 첫 번째만 고쳤더니, 컨테이너가 실제로 타는 두 번째
경로에서 여전히 `chromadb`가 나왔다. 세 경로를 `vectorPreflightError` 하나로
모았다.

unit test가 아니라 이미지를 돌려야만 보이는 종류였다.

## 내가 만든 버그 두 개

둘 다 테스트가 잡았다.

**오류 체인을 끊었다.** preflight 헬퍼가 `err.Error()` 문자열을 받아 `%s`로
포맷했다. `errors.Is(err, context.Canceled)`가 false가 되었다. `%w`로
감싸도록 바꿨다.

**쓰기 순서를 잃었다.** job 스냅샷 쓰기를 `go`로 날렸다. terminal 쓰기를 start
쓰기가 추월하면 완료된 job이 "진행 중"으로 기록된다 — 그 버그가 바로 그
혼란을 만든다. 고립 실행에서는 통과하고 전체 스위트 부하에서만 실패했다.
channel 하나를 한 goroutine이 drain하도록 바꿨고, 전체 스위트 3회 연속
통과로 확인했다.

## 문서화하지 않은 판단

**`seq185` surface의 `chromadb` 하드코딩을 고치지 않았다.** 이 빌더들은
`truth_authority: false`, `dry_run_only: true`인 **규칙 계약과 결정 기록**이다.
정책 자체는 provider 간에 동일하지만 계약은 제도가 이루어진 엔진을 기준으로
이름이 붙었고, `buildFirstLiveScopeDecision209`는 MariaDB+ChromaDB 하에서 내린
결정 기록이라 엔진을 바꾸면 그 결정이 위조된다.

값을 고치는 것은 readiness 라벨을 generic하게 만드는 것과 같은 실수다. 판단을
코드 주석과 테스트(`TestSeq185ContractSurfacesStayChromaShaped`)에 남겼다.

**Windows 무결성 레벨 문제를 "Linux/WSL"로 넘기지 않았다.** 이 저장소는 Low
mandatory integrity 레벨을 갖고, 그 안의 `workerd.exe`는 Low 프로세스로 뜬다.
기본 `%TEMP%`가 Medium라 MIC가 디스크 바인딩의 쓰기를 막는다. `run-worker-tests.mjs`
가 Low 레이블 임시 디렉터리를 준다. 원인과 대안의 왜 나쁜지까지 runbook에
적었다.

**`--legacy-peer-deps`를 제거했다.** Stage 4에서 "필수"라고 기록한 것이
틀렸고 정정했다. lockfile 커밋이 정답이다 — `npm ci`는 그래프 해석을 하지
않으므로 깨지는 단계를 밟지 않는다.

## 검증

| 명령 | 결과 |
| --- | --- |
| `node --check "Archive Center.js"` | 통과 |
| `cd go-service; go test ./... -count=1` | EXIT=0, 실패 없음 (3회 연속) |
| `git diff --check` | 통과 |
| `npm run check` (Worker tsc) | 통과 |
| `npm test` (실제 workerd + D1) | 19 passed |
| `check-artifact-neutrality.mjs` | 14개 아티팩트 통과 |
| `render-wrangler-config.mjs --check` | 통과 |
| `docker build` | 성공 |
| 컨테이너 기동·거부 동작 | 3개 경우 확인 |

추가로 newD1TestStore는 `modernc.org/sqlite`, D1과 동일한 엔진에서 실제
`ON CONFLICT` upsert와 복합 키를 포함한 스키마 전체를 적재해 400건 이상의
테스트를 돌린다.

## 후속 원격 Vectorize 검증과 동기 facade

원격 preview Worker를 실제 Vectorize binding으로 기동해 `TestCloudflareVectorizeRemoteBlockingFacade`를 실행했다. 성공한 `Upsert`는 caller context 안에서 `AwaitVisible`을 끝낸 뒤 반환하고, 즉시 이어진 exact readback도 확인했다.

그 과정에서 Vectorize의 exact read가 관측 직후 다시 빈 결과를 줄 수 있음을 확인했다. 따라서 facade는 visibility를 관측한 최신 문서를 ordinary exact read에만 overlay한다. D1 canonical adapter가 있는 product path는 D1을 우선하므로 이 overlay는 replica regression을 가리는 마지막 caller-facing fence다. outbox의 `AcceleratorExactDocumentReader`는 overlay를 우회해 실제 Vectorize readback만 검사하며, timeout/cancel은 `ErrVisibilityPending`으로 durable `visibility_pending` 상태에 기록되어 mutation을 재전송하지 않는다.

ANN search의 전역 read-after-write 보장은 Vectorize가 제공하지 않는다. Cloudflare profile은 여전히 `cloudflareParityComplete=false`이며, search parity의 운영 판단은 별도 gate다.

## 미실행 경계

배포된 Worker는 없고, preview는 배포를 대체하지 않는다. 따라서:

- **원격 검증은 blocking write/exact readback 한 gate에 한정됐다.** 전체
  D1 outbox/recovery 및 search workload는 아직 인증된 deployment 검증 대상이다.
- **Cloudflare가 이 이미지를 실제로 기동하는지 검증하지 않았다.** 로컬에서
  뜨고 설정된 프로파일을 정직하게 보고한다는 것까지만 확인했다. 이건 첫
  인증 배포에서 발견되는 종류의 간극이다.
- **Vectorize의 ANN search read-after-write를 검증하거나 보장하지 않았다.**
  `getByIds` visibility와 ANN index propagation은 같은 계약이 아니며, outbox의
  실제 오류 형태도 추가 원격 하네스가 필요하다.

`cloudflareParityComplete`는 여전히 `false`이고, 따라서
`CloudflareProfileReady()`도 false다. Cloudflare 프로파일은 **아직 기능 parity를
갖춘 배포가 아니다.** Stage 5가 operator 면·artifact·문서를 완성했지만, 그
이전 단계들의 parity 게이트가 실제 배포 가능을 열었다고 선언할 근거는 없다.

## 보고서 초안 이후에 닫은 것들

보고서를 처음 쓴 뒤 세 커밋이 더 나왔고, 셋 다 이 단계의 operator parity에
직접 속한다. 초안의 "남은 작업"에 있던 3번은 이 중 하나로 해소됐다.

### 운영자에게 보이는 값이 거짓이던 것들

| 증상 | 원인 | 커밋 |
| --- | --- | --- |
| 목록에 뜬 job의 상세 URL이 404 | 상세가 live manager와 reset run만 보고 스냅샷을 안 봤다 | `e80ef7f` |
| job 이벤트 스트림이 404 | 위와 같고, 404가 job을 지켜보는 순간에 도착해 깨진 endpoint로 읽힌다 | `e80ef7f` |
| `store_capabilities: "85/86"` | 누락의 이름도, 분모가 작은 이유도 말하지 않았다 | `97aedae` |
| `requested_by` 컬럼이 영원히 빈 값 | 마이그레이션 002에 컬럼이 있는데 Go 코드가 한 번도 쓰지 않았다 | `7861765` |

**목록과 상세의 불일치는 이 단계에서 내가 만든 것이다.** 스냅샷 영속화를
넣으면서 목록에만 병합했고, 상세와 이벤트는 손대지 않았다. 404는 "그 id가
틀렸다"고 말하는데 진실은 "그 job은 끝났다"이므로, 방금 목록에서 읽은 id를
잘못 옮겨 적었다고 결론내리게 만든다.

**이벤트 스트림은 두 종류를 구분해야 했다.** 끝난 job은 최종 상태 1건을
보내고 닫는다(미래의 갱신이 없다). reset run은 스트림이 아예 없다 — 진행이
D1에 체크포인트하는 worker 때문에 나아가지 이 프로세스가 실행해서가 아니다.
빈 스트림이나 404 대신 409와 이유를 답한다.

**능력 커버리지의 분모**에서 적용 불가 능력을 뺐다. 빠진 하나는
`ShadowStatusReporter`(MariaDB shadow로의 dual write 실패 횟수)이고
Cloudflare는 MariaDB를 쓰지 않으므로 보고할 shadow가 없다. 미구현이 아니라
적용 불가이고, 영원히 도달할 수 없는 분모는 "complete"라고 말할 수 없게 만든다.
예외는 이름과 **이유**를 함께 선언해야 하고, 이유 없는 예외는 불편한 항목을
조용히 지운 것과 구분되지 않으므로 테스트가 그걸 막는다.

**reset의 감사 컬럼**은 선언만 되어 있었다. `AdminResetActorStore`를 선택적
인터페이스로 두어 MariaDB의 공유 계약을 D1 전용 컬럼 때문에 넓히지 않았다.
actor는 요청의 label, 없으면 peer 주소이며 **Authorization 헤더에서는 절대
가져오지 않는다** — 토큰은 공유 비밀이고 control plane row는 DB를 읽을 수 있는
모든 것에게 보인다. 토큰과 같은 값의 label은 버린다. resume은 최초 요청자를
유지한다: resume은 이어받은 cursor로 계속할 뿐 새 요청이 아니고, 덮어쓰면
"resume을 누른 사람이 남이 승인한 삭제를 했다"고 기록된다.

### 내가 만든 테스트 결함 두 개

1. **초록으로 보이는 skip.** reset actor 테스트가 vector reset 경로에서 501로
   끝나 전부 `t.Skipf`로 넘어갔다. 초록이지만 아무것도 검증하지 않는다.
   `reset_vector:false`로 그 경로를 건너뛰고 Skip을 `Fatalf`로 바꿨다.
2. **의도한 실패를 구현으로 해소.** 토큰 테스트에서 내가 토큰을 label로
   넘겼고, 핸들러는 받은 label을 정직하게 기록해 실패했다. 테스트를 약화하는
   대신 "토큰과 같은 label은 버린다"는 규칙을 구현했다. 운영자가 토큰을
   label 칸에 붙여넣는 것은 실제로 막을 가치가 있는 경우였다.

## 남은 작업

1. 인증된 원격 통합 테스트 — 계정 확보가 선행 조건이다.
2. `seq185` 계약 키 이름의 provider 중립화 — 계약 파괴 여부를 별도로 결정해야
   한다. 이 단계에서는 값이 아니라 이름만 문제가 된다.
3. Stage 6 대상 후보: `cloudflareParityComplete`를 검토하려면 C/V/O parity 게이트를
   개별적으로 닫아야 한다.
4. `reindex`·`rescan`·`session-normalize`·`dedupe-cleanup`은 이제 스냅샷이
   남지만 재개되지는 않는다. 각 job의 재개 semantics를 설계하면 `interrupted`
   대신 이어받을 수 있다. 지금은 재개할 수 없는 것을 재개하는 척하지 않는 쪽을
   택했다.
