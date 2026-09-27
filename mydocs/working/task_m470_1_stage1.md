# Task M470-1 Stage 1 완료 보고서

GitHub Issue: [#1](https://github.com/iamhelios07/archive-center/issues/1)

구현계획서: [task_m470_1_impl.md](../plans/task_m470_1_impl.md)

Stage: 1 — MariaDB capability inventory와 D1 scope decision

## 단계 목적

Cloudflare D1의 범위를 미리 축소하지 않고, 현재 MariaDB Store의 실제 capability·schema·SQL/transaction semantic·runtime consumer를 전수 대조하여 D1 disposition 검토 기준을 만든다. 이 단계에서는 Worker, D1 schema, Vectorize, Container source/artifact를 만들지 않는다.

## 산출물

| 파일 | 변경 요약 |
|---|---|
| [task_m470_1_mariadb_capability_inventory.md](../tech/task_m470_1_mariadb_capability_inventory.md) | base Store와 public optional interface 84개, production MariaDB implementation 26개(`mariadb.go` 포함), migrations/consumer/SQL semantic 및 R/D/U proposed disposition을 기록했다. |

## 본문 변경 정도 / 본문 무손실 여부

신규 technical inventory 문서만 추가했다. 기존 product source, MariaDB schema/migration, local MariaDB·ChromaDB runtime behavior, HTTP API와 configuration은 변경하지 않았다.

## 검증 결과

실행 명령:

```bash
cd go-service && go test ./internal/store ./internal/httpapi -count=1
```

결과:

- `ok github.com/risulongmemory/archive-center-go/internal/store 0.943s`
- `ok github.com/risulongmemory/archive-center-go/internal/httpapi 58.056s`

정적 inventory cross-check:

```powershell
# public interface 이름이 inventory에 모두 있는지, production mariadb*.go 파일이
# implementation matrix에 모두 있는지 비교하고 git diff --check 실행
```

결과:

- public optional interface 84개 중 누락 0개.
- `mariadb.go`를 제외한 production `mariadb*.go` feature file 25개 중 누락 0개. `mariadb.go`는 별도 matrix row로 포함했다.
- `git diff --check` 통과.

## 잔여 위험

- R/D/U는 source-based proposed disposition이며 product scope 승인 전에는 D1 first-release commitment가 아니다.
- MariaDB의 `FOR UPDATE`, MySQL upsert/`LAST_INSERT_ID`, JSON predicate, cross-table transaction/reset semantics는 D1/SQLite로 기계적으로 이식할 수 없다.
- Vectorize eventual consistency, outbox acknowledgement와 recent-memory fallback은 아직 구현하거나 remote Cloudflare 환경에서 검증하지 않았다.

## 다음 단계 영향

- Stage 2는 이 inventory의 D1 first-release disposition 및 profile guard boundary를 작업지시자가 승인한 뒤에만 시작할 수 있다.
- 승인 시에도 Stage 2의 범위는 provider-neutral runtime bootstrap과 Worker bridge contract까지이며 D1 canonical schema/provider implementation은 Stage 3에서 다룬다.

## 승인 요청

- Stage 1 inventory와 검증 결과를 검토·승인해 주세요.
- 다음 단계로 진행하려면 **`Stage 1 승인, Stage 2 시작`** 또는 **`다음 단계 진행`**이라고 명시해 주세요.
