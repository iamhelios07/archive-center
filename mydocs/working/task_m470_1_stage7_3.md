# Stage 7.3 완료 보고 — Cloudflare 원격 rollout과 RisuAI 인증 연동

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)
구현계획서: [`task_m470_1_stage7_impl.md`](../plans/task_m470_1_stage7_impl.md)
Stage: 7.3

## 단계 목적

승인된 Cloudflare 리소스 이름으로 D1·Vectorize·Bridge Worker·Gateway Worker·Go Container를 실제 계정에 배포하고, 공개 Gateway 인증부터 Container, private Bridge, D1·Vectorize까지 이어지는 실서비스 경로를 검증한다. RisuAI가 공개 Gateway의 bearer 인증을 사용할 수 있도록 `Archive Center.js`의 backend 연결 설정도 완성한다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| `Archive Center.js` | 선택적 Backend Bearer Token 설정·비밀번호 입력란·저장·로그 마스킹을 추가하고 모든 `bridgeFetch()` 요청에 Authorization 헤더를 적용했다. 토큰이 비어 있으면 기존 로컬 MariaDB 무인증 동작을 유지한다. |
| `deploy/cloudflare/gateway/src/index.ts` | Cloudflare canonical authority Container에 `AC_MODE=live`를 전달한다. |
| `deploy/cloudflare/wrangler.template.toml` | private Bridge Worker observability를 기본 활성화했다. |
| `deploy/cloudflare/gateway/wrangler.template.toml` | public Gateway Worker observability를 기본 활성화했다. |
| `deploy/cloudflare/scripts/bootstrap-cloudflare.mjs` | repository-root Docker context, Wrangler 계정 선택 방식, D1 출력 파싱, migration 비대화형 실행, push/deploy 호출과 readiness 확인을 실제 Wrangler 4.124 동작에 맞췄다. |
| `go-service/internal/config/config.go` | Cloudflare authority를 live/cutover authoritative store로 허용하고, 완료된 Cloudflare profile의 고정 bootstrap-only gate를 제거했다. |
| `go-service/internal/config/config_test.go` | Cloudflare live/cutover authority 허용 및 bridge credential guard 회귀 테스트를 추가했다. |
| `go-service/internal/httpapi/group_health.go` | 실제 dependency 상태와 capability coverage로 Cloudflare readiness를 판단하도록 보정했다. |
| `go-service/internal/httpapi/group_health_cloudflare_test.go` | Cloudflare operational readiness와 실제 blocker 판정을 검증하도록 갱신했다. |
| `deploy/cloudflare/README.md` | live authority 환경과 정상 readiness 기대값을 실제 배포 결과에 맞췄다. |

원격 배포 리소스는 작업지시자가 승인한 이름을 사용했다.

- Bridge Worker: `archive-center-bridge`
- Gateway Worker: `archive-center-gateway`
- Container image repository: `archive-center-container`
- D1: `archive-center-db`
- Vectorize: `archive-center-vectors` (기존 1024/cosine, vector 0건 리소스 재사용 승인)

계정 ID, resource ID, workers.dev URL, Bridge token, bearer token은 보고서와 추적 파일에 기록하지 않았다.

## 본문 변경 정도 / 본문 무손실 여부

코드·배포 작업이다. 기존 MariaDB + ChromaDB 기본 경로는 변경하지 않았다. Backend bearer token 기본값은 빈 문자열이며, 이 경우 기존 요청 헤더와 동작이 유지된다. Cloudflare profile에서만 공개 Gateway와 Go backend의 이중 인증을 사용한다.

첫 원격 실행에서 Container가 `live`를 MariaDB authority 전용으로 검증해 exit code 1로 종료하는 결함을 실제 startup 로그로 확인했다. provider-aware authority 검증으로 수정하고 새 Container revision을 push했다. 수정 후 동일 이미지·동일 runtime secret으로 로컬 startup을 확인한 뒤 원격 Container가 `ready` 상태가 되는 것을 확인했다.

## 검증 결과

실행 명령 및 시나리오:

```bash
node --check "Archive Center.js"
cd go-service
go test ./... -count=1
cd ../deploy/cloudflare/gateway
npm run check
npm test
cd ../../..
node deploy/cloudflare/scripts/render-wrangler-config.mjs --check
node deploy/cloudflare/gateway/scripts/render-wrangler-config.mjs --check
git diff --check

# 실제 계정 검증 — 식별자·URL·credential 출력은 보고서에서 제외
npx wrangler containers list
# unauthenticated /ready
# bearer-authenticated /ready
# bearer-authenticated /sessions
```

결과:

- OK — `Archive Center.js` 문법 검사 통과.
- OK — Go 전체 패키지 테스트 통과.
- OK — Gateway TypeScript check 및 Vitest 3개 테스트 통과.
- OK — Bridge/Gateway renderer check와 두 TOML의 `observability.enabled=true` 파싱 확인.
- OK — `git diff --check` 통과(CRLF 변환 경고만 존재).
- OK — Cloudflare Container 상태 `ready`, live instance 1개 확인.
- OK — 미인증 `/ready`는 401, 인증 `/ready`는 200과 `ready=true`, 인증 `/sessions`는 200.
- OK — 원격 readiness가 `runtime_profile=cloudflare`, `mode=live`, `vector_mode=cloudflare`, `degraded=false`, store capability `88/88`을 보고했다.
- OK — 수정된 `Archive Center.js`를 RisuAI에 적용하고 Gateway endpoint와 bearer token을 입력한 실제 연결 성공을 작업지시자가 확인했다.

## 잔여 위험

- Bearer token은 기존 LLM API key와 같은 RisuAI 설정 저장 경로에 보관된다. 로그에는 `[configured]`로 마스킹하지만, RisuAI 설정 저장소 자체의 보호 수준을 넘어 별도 암호화를 제공하지 않는다.
- 첫 실행 bootstrap은 기존 리소스 재사용을 지원하지 않는다. 이번 배포의 기존 Vectorize 재사용은 작업지시자 승인에 따라 수동 rollout 경로로 수행했다.
- 원격 리소스의 삭제·재생성은 자동화하지 않았다. 향후 정리나 재배포에서도 운영자 확인이 필요하다.

## 다음 단계 영향

- Stage 7.3 원격 rollout과 RisuAI 인증 연동이 완료됐다.
- 다음 단계는 최종 보고서 작성, 오늘할일 완료 처리, publish 브랜치 push와 PR 게시다. 별도 작업지시자 승인 전에는 진행하지 않는다.

## 승인 요청

- Stage 7.3 산출물과 검증 결과를 승인하면 최종 보고 및 PR 단계로 진행한다.
