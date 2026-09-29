# 구현계획서 보완 — Cloudflare Container 첫 실행 bootstrap

수행계획서: [task_m470_1.md](task_m470_1.md)
기존 구현계획서: [task_m470_1_impl.md](task_m470_1_impl.md) — 작업지시자 변경분을 보존하기 위해 수정하지 않는다.
GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
마일스톤: M470

## 목적과 결정

기존 Bridge Worker는 Go backend가 D1과 Vectorize에 접근하는 private envelope 경로다. Cloudflare Containers는 Worker가 Durable Object Container binding을 통해 HTTP를 전달하는 배포 모델이므로, public request와 private bridge를 한 Worker에 섞지 않는다.

Stage 7은 단순 재배포 보조가 아니라, 사용자가 repository를 clone한 뒤 한 번 실행해 전용 Cloudflare resource, schema, bridge Worker, public gateway Worker와 Go Container까지 준비하는 **첫 실행 bootstrap**을 추가한다. Node 22+와 Docker-compatible engine은 Container image build의 외부 선행 조건이며, script가 없는 tool을 OS별로 설치하거나 Cloudflare login을 우회하지 않는다. Wrangler 인증이 없으면 interactive login을 시작하고, Docker가 없거나 실행 중이 아니면 원격 resource를 변경하기 전에 중단한다.

Stage 7은 다음 형태를 추가한다.

```text
RisuAI ──Bearer HTTP──▶ gateway Worker ──Container binding──▶ Go Container
                                  Go Container ──BRIDGE_TOKEN──▶ bridge Worker ──▶ D1 / Vectorize
```

- gateway Worker는 단일 고정 Container instance를 사용한다. Container는 stateless이고 D1이 canonical state라는 기존 경계를 보존한다.
- gateway는 request를 Container로 전달하기 전에 bearer secret을 constant-time 비교로 확인한다. Go backend도 같은 `AC_BEARER_TOKEN`을 받아 기존 인증을 계속 강제한다.
- bridge token과 bearer token은 Worker secret으로만 보관하며, source, rendered config, image, log, report에 기록하지 않는다.
- bridge URL과 Cloudflare Registry image URI는 account-scoped 값으로 generated gateway Wrangler config에만 render한다.
- Cloudflare profile parity complete flag는 이 Stage에서 임의로 true로 바꾸지 않는다. gateway deployment는 topology 실행 경로를 제공하며, parity gate 선언은 별도 원격 기능 검증 근거가 있어야 한다.

## 단계 개요

| Stage | 제목 | 주요 산출 | 검증 |
|---|---|---|---|
| 7.1 | public Container gateway artifact | gateway Worker source/config, secret-to-container injection, request-auth tests | TypeScript check/test, config parse, Docker build |
| 7.2 | 첫 실행 Cloudflare bootstrap | account-neutral renderer, one-command bootstrap, runbook/README 보완 | bootstrap dry-run/negative tests, full Go/Worker regression, `git diff --check` |
| 7.3 | Cloudflare remote rollout | bridge and gateway deployment, secrets, pushed image, authenticated readiness scenario | Wrangler deployment/status and bearer-authenticated readiness; sensitive output suppressed |

## 문서 위치 확인

| 파일 | 기존 선택 위치 | Stage 산출물 경로 | 일치 여부 | 비고 |
|---|---|---|---|---|
| 운영자 배포 절차 | `deploy/cloudflare/README.md` | `deploy/cloudflare/README.md` | OK | 기존 공식 Cloudflare runbook만 보완한다. |
| 사용자 진입 요약 | `README.md` | `README.md` | OK | topology 설명이 바뀔 때만 최소 보완한다. |
| 구현계획 보완 | `mydocs/plans/` | `mydocs/plans/task_m470_1_stage7_impl.md` | OK | 기존 uncommitted implementation plan은 수정하지 않는다. |
| 단계 보고 | `mydocs/working/` | `mydocs/working/task_m470_1_stage7_{N}.md` | OK | 각 Stage source와 함께 커밋한다. |

## Stage 7.1 — public Container gateway artifact

### 산출물

신규:

- `deploy/cloudflare/gateway/src/index.ts`
- `deploy/cloudflare/gateway/src/index.test.ts`
- `deploy/cloudflare/gateway/package.json`
- `deploy/cloudflare/gateway/package-lock.json`
- `deploy/cloudflare/gateway/tsconfig.json`
- `deploy/cloudflare/gateway/wrangler.template.toml`

수정:

- 필요한 ignore/config validation file

### 변경 내용

- `@cloudflare/containers`의 `Container` class, `defaultPort = 28080`, `max_instances = 1`, Durable Object export/binding을 선언한다.
- Worker secret `BRIDGE_TOKEN`, `AC_BEARER_TOKEN`과 rendered `BRIDGE_URL`을 Container environment로 전달하고 Cloudflare runtime profile의 고정 non-secret 값을 주입한다.
- gateway request handler는 bearer format/missing-secret/unauthorized 요청을 Container 기동 전에 거부하고, 인증 요청만 singleton Container의 `fetch`로 transparent proxy한다.
- Dockerfile build context가 repository root여야 한다는 제약을 보존한다. Wrangler config는 Dockerfile-relative 자동 build에 의존하지 않고, Cloudflare Registry에 별도 build/push한 account-scoped image URI를 renderer가 주입하도록 한다.

### 검증

```bash
cd deploy/cloudflare/gateway
npm ci
npm run check
npm test
cd ../../..
docker build -f deploy/cloudflare/container/Dockerfile .
git diff --check
```

### 커밋

```text
Task #1 Stage 7.1: Cloudflare Container gateway 추가
```

## Stage 7.2 — 안전한 operator deployment path

### 산출물

신규:

- `deploy/cloudflare/gateway/scripts/render-wrangler-config.mjs`
- `deploy/cloudflare/scripts/bootstrap-cloudflare.mjs`

수정:

- `deploy/cloudflare/.gitignore`
- `deploy/cloudflare/README.md`
- `README.md` (gateway topology 설명이 필요한 경우에 한함)

### 변경 내용

- gateway renderer는 missing `AC_CLOUDFLARE_BRIDGE_URL` 또는 `AC_CLOUDFLARE_CONTAINER_IMAGE`를 fail-closed로 거부하며 값은 출력하지 않는다.
- `bootstrap-cloudflare.mjs`는 fresh clone의 단일 interactive entry point다. Node/Docker/Wrangler authentication을 preflight하고, 사용자에게 실제 embedder dimension을 묻거나 `--dimension`으로 받는다. universal default dimension을 두지 않는다.
- script는 collision 없는 deployment suffix를 생성해 dedicated D1, Vectorize, bridge Worker, gateway Worker 이름에 사용한다. 기존 resource가 발견되면 자동 재사용·삭제·reset하지 않고 fail-closed로 중단한다. bridge renderer는 generated D1/Vectorize 이름과 ID를 함께 주입하도록 보완한다.
- script는 D1 creation → all migrations → Vectorize creation/metadata indexes → root-context Docker build → Cloudflare Registry push → bridge/gateway generated config render → deploy → secret injection 순서를 소유한다. bridge token은 process memory에서 한 번 생성해 두 Worker secret에만 전달한다.
- user bearer token은 bootstrap이 cryptographically 생성하고 gateway Worker/Container에만 secret으로 주입한다. 완료 시 해당 사용자의 local terminal에 endpoint와 token을 **한 번만** 표시하며 source, repo, rendered config, image, Cloudflare log 또는 report에는 저장하지 않는다.
- child command의 account ID, URL, registry URI, token 및 raw response는 capture하고, 일반 console에는 단계 pass/fail만 출력한다. 다만 bootstrap 완료 화면은 RisuAI 연결을 위해 local terminal에서만 endpoint/token을 일회 표시한다. `--check`는 secrets 없이 source/config/CLI precondition만 검사하고 remote mutation을 하지 않는다.
- runbook은 Bridge와 gateway Worker를 별도 이름/책임으로 설명하고, first bootstrap의 Docker root-context build → Cloudflare Registry push → generated config render → deploy 순서를 고정한다. 재실행은 initial bootstrap이 아니라 별도 controlled deployment 경로로 명시한다.

### 검증

```bash
node deploy/cloudflare/gateway/scripts/render-wrangler-config.mjs --check
node deploy/cloudflare/scripts/bootstrap-cloudflare.mjs --check
cd deploy/cloudflare/worker
npm ci
npm run check
npm test
cd ../../go-service
go test ./... -count=1
cd ..
git diff --check
```

### 커밋

```text
Task #1 Stage 7.2: Cloudflare 첫 실행 bootstrap 추가
```

## Stage 7.3 — Cloudflare first-run remote bootstrap

### 선행 조건

- Docker-compatible engine이 실행 중이며 repository-root Docker build를 수행할 수 있다.
- user가 실제 embedding model dimension을 알고 있다. bootstrap은 prompt 또는 `--dimension`으로 이를 받는다.
- authenticated Wrangler session을 사용하거나 bootstrap의 interactive login을 완료한다. D1/Vectorize는 bootstrap이 새 dedicated resource로 만든다.

### 절차

1. clone root에서 `node deploy/cloudflare/scripts/bootstrap-cloudflare.mjs`를 한 번 실행한다. script는 dimension prompt, dedicated resource creation, migration, image build/push, bridge/gateway deploy, ephemeral bridge token 양쪽 주입 및 generated bearer token 주입을 수행한다.
2. Cloudflare Container provisioning 완료 후 unauthorized request rejection과 generated bearer-token authenticated `/ready`를 검사한다. 자동 검증 출력은 pass/fail만 남긴다.
3. 성공 시 local terminal에만 RisuAI backend endpoint와 bearer token을 일회 표시한다. 사용자에게 그 값을 RisuAI 설정에 입력하라고 안내한다; agent 실행 로그·report·repo에는 값을 포함하지 않는다.

### 검증

```bash
npx wrangler deploy --config <generated bridge config>
npx wrangler deploy --config <generated gateway config>
npx wrangler containers list
# bearer-authenticated readiness scenario; output is reduced to pass/fail only
git diff --check
```

### 커밋

```text
Task #1 Stage 7.3: Cloudflare Container remote rollout 확인
```

## 위험과 대응

- **Dockerfile build context**: 현재 Dockerfile은 repository root의 `go-service/`를 COPY한다. gateway config directory에서 Dockerfile을 직접 image path로 참조하지 않고 root build/push 방식을 사용한다.
- **secret 회수 불가**: Worker secret은 readback할 수 없으므로 bridge token은 bootstrap process 내에서 양쪽 Worker에 함께 set한다. generated bearer token은 local terminal의 일회 출력 외에는 회수할 수 없으므로, 출력 전에 remote readiness가 성공해야 한다.
- **dimension 불일치**: Vectorize dimension은 생성 후 바꿀 수 없다. bootstrap은 embedder dimension을 explicit input으로 요구하고, 입력 값에 맞는 index만 새로 만든다.
- **deploy 비원자성**: gateway Worker가 먼저 live가 되어도 required secret과 Container provisioning 전에는 request가 안전하게 거부될 수 있다. public route는 final readiness check 전 사용 가능으로 주장하지 않는다.
- **state ownership**: Durable Object SQLite는 Container runtime metadata용이며 application state를 저장하지 않는다. canonical product state는 D1에만 둔다.

## 승인 요청 사항

- Stage 7.1의 gateway 분리, singleton Container, secret injection, root-context image build/push 및 검증 범위를 승인해 주세요.
- 승인 후 **`Stage 7.1 시작`**이라고 명시해 주세요. Stage 7.2와 remote rollout은 각각 Stage 보고·승인 후 진행합니다.
