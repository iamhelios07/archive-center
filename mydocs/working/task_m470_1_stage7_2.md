# Stage 7.2 단계 보고서

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_stage7_impl.md`](../plans/task_m470_1_stage7_impl.md)
Stage: 7.2 — 안전한 operator deployment path

## 단계 목적

- 계획서 Stage 7.2("안전한 operator deployment path")에 따라, fresh clone에서 단일 명령으로 Cloudflare 전체 배포(D1 → migrations → Vectorize → metadata index → root-context Container image build/push → bridge/gateway config render → deploy → secret injection → readiness)를 수행하는 bootstrap 경로를 만든다.
- ChromaDB 호환 공개 시그니처(`Search`, `Upsert`)와 기존 브리지 계약을 유지한 채, 계정·리소스 식별자와 토큰이 어떤 tracked source·rendered config·report에도 남지 않도록 한다.
- 기존 리소스에 대한 자동 재사용·삭제·reset을 금지하고, 실패 시 rollback/cleanup도 수행하지 않는 fail-closed 정책을 스크립트 차원에서 고정한다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `deploy/cloudflare/scripts/bootstrap-cloudflare.mjs` | 신규(344행). 단일 명령 bootstrap. `--check`(무변경 preflight)와 실제 배포 모드 소유. |
| `deploy/cloudflare/gateway/scripts/render-wrangler-config.mjs` | 신규. gateway 템플릿 렌더러. `--check` 지원, missing 값 fail-closed, 값 미출력. |
| `deploy/cloudflare/scripts/render-wrangler-config.mjs` | 수정. required(optional) 필드 재구성. `AC_CLOUDFLARE_VECTORIZE_INDEX_NAME` 신설(구 `..._INDEX_ID`는 기존 provisioned 운영자 호환용 legacy로 유지). Worker/D1 이름 override 변수 추가. `index_name`은 템플릿 기존 할당 라인 교체(TOML 중복 정의 방지), `database_id`는 주석 앞 삽입. |
| `deploy/cloudflare/README.md` | 수정. "First deployment — one command" 섹션 신설, gateway 포함 topology 다이어그램 교체, 수동 D1/Vectorize 절차를 "Manual bridge resource reference"로 명칭·게이트 변경, `--propertyName` → `--property-name` 표기 정정. |
| `README.md` | 수정. Cloudflare 섹션 안내 문구에 fresh-clone one-command bootstrap 반영(2행). |
| `mydocs/orders/20260929.md` | 수정. 비고 갱신(7.1·7.2 구현 완료, 7.3 승인 대기). |

### 계획서 대비 편차

- 계획서 산출물에 `deploy/cloudflare/.gitignore` 수정이 있었으나, 기존 패턴 `wrangler.toml`(무슬래시)이 `deploy/cloudflare/gateway/wrangler.toml`을 이미 커버함을 `git check-ignore -v`로 확인해 불필요 판단. `*.tmp`도 scratch 커버. 미수정.
- bootstrap에 plan 107행("readiness 성공 후 일회 표시") 구현을 위해 인증 미통과 401 + 인증 `/ready` 검증 단계를 스크립트에 포함했다(Stage 7.3이 수동 재검증을 소유).
- 추가 경화: 생성 이름 길이 제한(D1 ≤32자, Worker ≤58자; UTC 분 타임스탬프 10자+24bit 난수=16자 접미사), `d1 execute --yes`(비 TTY 중단 방지), node_modules 완설치 시 `npm ci` 생략(잠긴 파일 EBUSY 방지), Docker `--platform linux/amd64` 고정.

## 본문 변경 정도 / 본문 무손실 여부

- 코드 작업. Go/Worker 공개 시그니처·계약 무변경(이 단계의 Go/Worker 소스 수정 0건). 브리지 계약, durable search overlay, `cloudflareParityComplete=false` 불변.
- 문서: `deploy/cloudflare/README.md`의 기존 수동 절차 본문은 원문 보존하고, 섹션 명칭과 게이트 안내만 추가/교체했다. legacy 단계에는 bootstrap을 사용하지 않는다는 경계 문구를 넣었다.
- secret 위생: bearer/bridge token은 process memory에서만 생성·전달, 완료 화면(ready 이후)에서만 일회 출력. child command 출력(account ID·URL·registry URI·응답)은 pass/fail로만 노출. rendered config는 `.gitignore` 대상.

## 검증 결과

실행 명령(계획서 Stage 7.2 검증 섹션; 루트 기준 경로로 동일 수행. 계획서의 `cd ../../go-service`는 worker 기준 경로 오류로 루트에서 `cd go-service`로 수행):

```bash
node deploy/cloudflare/gateway/scripts/render-wrangler-config.mjs --check
node deploy/cloudflare/scripts/bootstrap-cloudflare.mjs --check
npm ci (deploy/cloudflare/worker)
npm run check (deploy/cloudflare/worker)
npm test (deploy/cloudflare/worker)
go test ./... -count=1 (go-service)
git diff --check
```

결과:

- OK `render-gateway-wrangler-config: template is renderable and complete`
- OK `bootstrap-cloudflare` preflight 전부 pass(Docker engine / dependencies / Wrangler CLI / bridge renderer / gateway renderer), `check complete; no remote Cloudflare resource was changed`
- OK 음성 검증: required env 미설정 시 양쪽 렌더러 exit 1, 변수명만 출력(값 미노출)
- OK 함수형 검증: rendered bridge config dry-run(tracked wrangler 4.124.0) → `env.DB (D1)`, `env.VECTORIZE (Vectorize)` 바인딩 확인; rendered gateway config dry-run → DO 바인딩 + `BRIDGE_URL` var + container image 확인
- OK `npm ci`: 83 packages, `npm run check`: tsc 이상 없음, `npm test`: 20 passed(worker)
- OK gateway `npm run check`/`npm test`: 3 passed(회귀)
- OK `go test ./... -count=1`: 전 패키지 ok(각종 cmd/, internal/ 포함; 사용자 opt-in integration 테스트는 env 미설정 skip 확인)
- OK `git diff --check`: 오류 없음(CRLF 변환 warning만 존재)

기타 정리: 이전 세션에서 남아 포트 18801로 실행 중이던 wrangler dev 프로세스가 `worker/node_modules`를 잠가 `npm ci`를 막았다. 에이전트가 남긴 부산물로 판단해 프로세스 트리를 종료하고 `npm ci`로 node_modules를 복원했다(사용자 관련 프로세스는 그대로 유지).

## 잔여 위험

- bootstrap의 원격 경로(D1 생성, migrations, Vectorize, containers push, secret put, deploy)는 실제 Cloudflare 계정에서 아직 실행되지 않았다(Stage 7.3 범위). dry-run과 CLI 소스 검증 기반이며, 첫 원격 실행에서 wrangler 출력 형식 파서(`Pushed image:`, workers.dev URL)가 재조정될 수 있다.
- 실패 시 부분 생성 리소스는 의도적으로 남는다(자동 cleanup 금지). 계정에 생성된 isolated 리소스는 사용자가 dashboard/CLI로 직접 정리해야 한다.
- readiness는 gateway `/ready` 2xx로 판정한다. Container pull·secret 전파 지연은 120초 재시도로 흡수하지만, 그 이상 지연되면 token 미표출로 종료한다(안전 방향).
- `deploy/cloudflare/README.md`의 나머지 수동 절차는 이전 토폴로지 시절 표기를 일부 포함할 수 있으며, 필요 시 후속 단계에서 다듬을 수 있다.

## 다음 단계 영향

- Stage 7.3(원격 first-run rollout)은 `node deploy/cloudflare/scripts/bootstrap-cloudflare.mjs` 단일 실행으로 진행한다. 사용자 실제 embedder dimension을 prompt 또는 `--dimension`으로 요구하며, interactive `wrangler login`이 최초 1회 필요할 수 있다. 다중 계정이면 `CLOUDFLARE_ACCOUNT_ID` 지정.
- 성공 시 terminal에 endpoint와 bearer token이 일회 표시된다. 로그·보고서에는 값·계정 ID·리소스 ID·URL을 포함하지 않는다.
- Stage 7.3 검증은 계획서 섹션(deploy 재확인, containers list, bearer-authenticated readiness, `git diff --check`)을 따른다.

## 승인 요청

- Stage 7.2 산출물과 검증 결과를 승인해 주세요. 승인 후 Stage 7.3(Cloudflare first-run 원격 rollout)을 진행합니다.
