# MariaDB Store capability parity inventory — Task M470-1 Stage 1

## 목적과 판정 모델

이 문서는 Cloudflare 배포 옵션이 MariaDB + ChromaDB local runtime과 **같은 사용자·운영 경험(functional parity)** 을 제공한다는 목표 아래, 현재 Archive Center runtime이 사용하는 MariaDB persistence capability를 전수 확인한 Stage 1 inventory다. 기준은 [`store.go`](../../go-service/internal/store/store.go#L537-L601)의 base `Store`, public optional capability interface, MariaDB implementation, migrations, 그리고 실제 Go runtime consumer다.

기존 `R/D/U`는 "첫 release에 넣을지"와 "최종적으로 동등하게 지원할지"를 혼합해 lorebook·reroll·memory·운영 기능을 잘못 제외할 여지를 만들었다. 이 문서는 두 축을 분리한다.

- **Parity target (P)**: Cloudflare option이 local runtime과 같은 경험을 주장하려면 최종적으로 동등한 결과를 제공해야 하는 capability다. 일반 사용자 흐름과 관리자/운영 흐름 모두 P다.
- **Implementation gate**: 구현·검증 순서다. `C`는 D1 canonical contract, `V`는 Vectorize consistency contract, `O`는 Cloudflare operator/admin contract를 뜻한다. 여러 gate를 가질 수 있다. gate가 뒤라는 것은 미지원 선언이 아니라 Cloudflare release blocker라는 뜻이다.
- **N/A**: local dual-write/shadow처럼 product capability가 아니라 local-provider 내부 진단인 항목뿐이다. Cloudflare에서 동일한 product route를 제거하는 근거가 될 수 없다.
- Cloudflare profile은 capability가 아직 완성되지 않은 상태에서 사용자 route를 profile guard로 대체하거나 silent no-op으로 처리하지 않는다. 개발 중에는 readiness를 false로 유지하며, parity gate를 모두 통과하기 전 Cloudflare option을 functional-parity deployment로 제공하지 않는다.

## 조사 범위와 증거

- production MariaDB implementation은 `mariadb*.go` 25개 feature file과 [`mariadb.go`](../../go-service/internal/store/mariadb.go#L60-L133)를 합친 26개 파일이며, static receiver scan으로 `*mariadbStore` method 312개를 확인했다. `_test.go`와 test-only fake는 제외했다.
- schema source는 [`migrations/001_schema.sql`](../../migrations/001_schema.sql)부터 [`migrations/014_character_state_field_provenance.sql`](../../migrations/014_character_state_field_provenance.sql)까지다. `mariadb.go` reset table registry도 runtime-owned table 집합의 cross-check로 사용했다.
- consumer는 `internal/httpapi`의 direct `s.Store` call, optional-interface assertion, startup/background worker와 [`internal/archive/bridge.go`](../../go-service/internal/archive/bridge.go)를 포함했다. test call만으로 판정하지 않았다.

## Store contract surface checklist

### Base `Store` (모두 MariaDB 구현됨)

| Capability family | Base methods | Current runtime use | Parity target / gate |
|---|---|---|---|
| Turn input ledger | `SaveChatLog`, `ListChatLogs`, `SaveEffectiveInput`, `GetEffectiveInput` | complete-turn, source acceptance, prepare-turn, archive bridge | P / C |
| Memory/fact retrieval | `SaveMemory`, `ListMemories`, `SaveEvidence`, `ListEvidence`, `SaveKGTriple`, `ListKGTriples` | extraction persistence, prepare-turn, explorer/admin export, reindex | P / C+V+O |
| Audit/critic/events | `SaveAuditLog`, `ListAuditLogs`, `SaveCriticFeedback`, `ListCriticFeedback`, `SaveCharacterEvent`, `ListCharacterEvents` | turn workflow, state repair, admin/explorer | P / C+O |
| Session and narrative reads | `Stats`, `ListSessions`, `GetResumePack`, `ListStorylines`, `ListWorldRules`, `ListInheritedWorldRules`, `ListCharacterStates`, `GetCharacterState`, `ListPendingThreads`, `ListActiveStates`, `ListCanonicalStateLayers`, `ListEpisodeSummaries`, `GetEpisodeSummary` | prepare-turn/extraction plus session/admin/read endpoints | P / C+O |

### Optional interface families (84개 모두 inventory 대상)

| Group | Interfaces checked | Current owner/consumer class | Parity target / gate |
|---|---|---|---|
| Health and administration | `Pinger`, `ShadowStatusReporter`, `AdminResetStore`, `SessionStateSnapshotReader`, `AuditLogCounter` | health, diagnostics, reset/export | `Pinger`, reset/snapshot/export/counter P / C+O; `ShadowStatusReporter` N/A (local dual-write diagnostic) |
| Turn window and replacement | `PrepareTurnRangeStore`, `TimelineTurnIndexStore`, `LogicalTurnReplacementStore`, `RollbackStore`, `EffectiveInputListStore` | prepare-turn, reroll, source acceptance, explorer | P / C; D1 transaction/retry proof is a gate, not an exclusion |
| Source and memory lifecycle | `SourceRevisionStore`, `CriticInputSnapshotStore`, `MemoryDerivationLifecycleAvailability`, `ActiveSourceRevisionLister`, `SourceRevisionHistoryLister`, `MemoryReprocessingJobStore`, `MemoryReprocessingWakeScheduleStore`, `MemoryReprocessingJobReopener`, `MemoryVectorOutboxStore`, `MemoryVectorOutboxLaneStore`, `MemoryVectorMaterializedCompletionStore`, `MemoryVectorOutboxMaintenanceStore`, `PreciseMemoryWriter`, `CharacterPerspectiveMemoryReader`, `ActiveInteractionMemoryReader`, `GeneralVectorPreciseMemoryReader`, `PreciseMemoryWriteAvailability`, `MemoryAdmissionProjectionInspector`, `MemoryAdmissionWriter`, `MemoryAdmissionWriteAvailability`, `VectorRecoveryCacheReader` | admission, precise memory, outbox/recovery, HUD | P / C+V+O; D1 outbox, fallback and worker recovery must reach parity |
| State and world projection | `ActiveScopeStore`, `CharacterStateHistoryStore`, `CharacterProvenanceRepairStore`, `ReversibleStatusTransitionStore`, `StatusCurrentValueStore`, `StatusSchemaProposalStore`, `StatusSchemaRegistryStore`, `StatusLifecycleStore`, `StatusChangeEventSourceLookupStore`, `StateRepairArtifactReader`, `GuidancePlanStateStore`, `ChapterSummaryStore`, `EpisodeSummaryStore`, `ArcSummaryStore`, `SagaDigestStore`, `ThemeOffscreenCarryStore`, `ConsequenceRecordStore`, `PsychologyBranchStore`, `CaptureVerificationStore` | extraction, story clock, body tracking, repair/admin, summaries | P / C+O; user-visible state plus repair/review capability both require parity |
| Entity and persona | `EntityIdentityWriter`, `EntityIdentityWriteAvailability`, `EntityIdentityLinkWriter`, `EntityIdentityCatalogReader`, `UniqueActiveEntitySurfaceIdentityResolver`, `UniqueActiveEntitySurfaceResolver`, `ReviewedEntityIdentityResolver`, `PersonaCapsuleStore`, `ProtagonistEntityMemoryOwnerIndexStore`, `ProtagonistEntityMemoryStore`, `ProtagonistEntityMemoryRepairStore`, `ProtagonistEntityMemoryManagementStore` | extraction identity, character recollection, persona/admin | P / C+O; persona/entity memory is used by prepare-turn and extraction |
| Reference and canon | `LorebookReferenceStore`, `LorebookReferenceExplorerStore`, `ReferenceLibraryStore`, `CanonPackStore`, `CanonRegistryStore`, `ReferenceCoverageStore`, `SourceDiscoveryStore`, `SourceDiscoveryMutableStore`, `SourceDiscoveryQueryStore` | lorebook prepare-turn, reference/admin, canon install/discovery | P / C+O; lorebook read and authoring, reference/canon/discovery workflows are separate gates but none is permanent U |
| Session migration/worldline | `SessionMigrationStore`, `SessionRoutingBaselineStore`, `SessionMigrationVectorStore`, `SessionMigrationVectorParityStore`, `SessionMigrationSourceLockStore`, `SessionMigrationSourceLockFenceStore`, `SessionMigrationRecoveryStore`, `SessionRouteBindingStore`, `SessionStitchStore`, `ForkLineageStore`, `WorldlineTopologySnapshotStore` | migration, route binding, stitch and worldline endpoints | P / C+V+O; user-visible worldline and operator migration need Cloudflare procedures |
| Explorer mutation | `ExplorerMutationStore`, `SupersessionResolutionStore` | memory explorer and fact review | P / C+V+O; mutation must remain available and durable |

## MariaDB implementation and schema matrix

| Implementation source | Persisted tables / operation family | Runtime consumers | MariaDB-specific dependency | Parity target / gate |
|---|---|---|---|---|
| [`mariadb.go`](../../go-service/internal/store/mariadb.go) | connection ping, complete reset, read-only session snapshot; reset registry spans all session tables | health, diagnostics, admin | `FOREIGN_KEY_CHECKS`, multi-table transaction | P / C+O; reset requires Cloudflare-safe authorization, preview/audit and D1 policy |
| [`mariadb_chat_memory.go`](../../go-service/internal/store/mariadb_chat_memory.go) | `chat_logs`, `effective_input_logs`, `memories`, `direct_evidence_records`, `kg_triples`, `audit_logs` | normal complete/prepare turn, recall, archive bridge, explorer | unique replay uses `ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)` | P / C+V+O |
| [`mariadb_logical_turn_replace.go`](../../go-service/internal/store/mariadb_logical_turn_replace.go), [`mariadb_delete.go`](../../go-service/internal/store/mariadb_delete.go) | canonical-tail replacement, rollback and session deletion across derived tables | reroll, source acceptance, deletion | one transaction, `SELECT … FOR UPDATE`, JSON provenance predicates | P / C; atomic rollback/retry tests are required |
| [`mariadb_memory_derivation.go`](../../go-service/internal/store/mariadb_memory_derivation.go), [`mariadb_memory_admission.go`](../../go-service/internal/store/mariadb_memory_admission.go), [`mariadb_precise_memory.go`](../../go-service/internal/store/mariadb_precise_memory.go) | `memory_source_revisions`, `precise_memory_units`, dependencies, jobs, `memory_vector_outbox` | turn admission, prepare recall, outbox, workflow HUD | leases and source revision conflict rows guarded by repeated `FOR UPDATE` transactions | P / C+V+O |
| [`mariadb_vector_recovery.go`](../../go-service/internal/store/mariadb_vector_recovery.go) | recovery cache/readback of memory vector material | startup vector recovery | provider recovery ordering | P / V+O; Vectorize design must not require immediate query readback |
| [`mariadb_character_feedback.go`](../../go-service/internal/store/mariadb_character_feedback.go) | `critic_feedback`, `character_events`, `entities`, `trust_states`, stats | extraction, character view, admin | append/read aggregation | P / C+O |
| [`mariadb_narrative_state.go`](../../go-service/internal/store/mariadb_narrative_state.go), [`mariadb_story_world.go`](../../go-service/internal/store/mariadb_story_world.go), [`mariadb_runtime_state.go`](../../go-service/internal/store/mariadb_runtime_state.go) | storylines, world rules, character/active/canonical/pending state, guidance and summary rows | prepare-turn, extraction, body tracking, story clock | JSON fields and latest/current projection selection | P / C+O |
| [`mariadb_status.go`](../../go-service/internal/store/mariadb_status.go), [`mariadb_state_repair_artifacts.go`](../../go-service/internal/store/mariadb_state_repair_artifacts.go) | status schema/proposals/current values/events/effects and repair artifacts | body tracking, conditions, story clock, state repair | current/event atomic transition, JSON provenance lookup, stale projection validation | P / C+O |
| [`mariadb_entity_identity.go`](../../go-service/internal/store/mariadb_entity_identity.go) | entity identities, surfaces, links, artifact bindings | extraction and character/recollection resolution | upsert, review-state mutation, locked identity changes | P / C+O |
| [`mariadb_persona.go`](../../go-service/internal/store/mariadb_persona.go) | persona capsules, entries, attachments, protagonist entity memories | recollection/persona and management routes | FK/cascade relationships, scoped reads | P / C+O |
| [`mariadb_lorebook_reference.go`](../../go-service/internal/store/mariadb_lorebook_reference.go), [`mariadb_reference_library.go`](../../go-service/internal/store/mariadb_reference_library.go), [`mariadb_reference_coverage.go`](../../go-service/internal/store/mariadb_reference_coverage.go) | lorebook entries/snapshots/scopes/locks; reference works/documents/entities/claims/coverage | prepare-turn lorebook, reference explorer/admin | lock reads and large relational reference graph | P / C+O |
| [`mariadb_canon_pack.go`](../../go-service/internal/store/mariadb_canon_pack.go), [`mariadb_canon_registry.go`](../../go-service/internal/store/mariadb_canon_registry.go) | canon pack install, registry, overlay | canon administration/search | `FOR UPDATE`, `ON DUPLICATE KEY UPDATE` lifecycle transitions | P / C+O |
| [`mariadb_session_hierarchy.go`](../../go-service/internal/store/mariadb_session_hierarchy.go), [`mariadb_session_routing.go`](../../go-service/internal/store/mariadb_session_routing.go), [`mariadb_session_stitch.go`](../../go-service/internal/store/mariadb_session_stitch.go), [`mariadb_migration.go`](../../go-service/internal/store/mariadb_migration.go) | session list/resume, routing baseline/bindings, stitch and migration manifests | migration, worldline and session controls | manifest hash/count/FK/vector parity and source locks | P / C+V+O |
| [`mariadb_source_discovery.go`](../../go-service/internal/store/mariadb_source_discovery.go) | source discovery jobs | reference/canon discovery flow | job lifecycle and locks | P / C+O |

All production `mariadb*.go` implementation files are represented in this table. A D1 design must not add a table without a matrix row or implement a row without identifying its consumer, parity target and implementation gate.

## SQL and transaction compatibility boundary

The following MariaDB behavior is present in live implementation and cannot be copied mechanically into D1:

1. **Row locking and serial workflow** — `FOR UPDATE` appears in canon-pack, identity, logical-turn, lorebook, memory admission/derivation, migration, routing, stitch and status flows. D1 implementations need an explicit transaction/retry/idempotency contract; a syntactic SQL rewrite is insufficient.
2. **MySQL upsert and generated identity** — `ON DUPLICATE KEY UPDATE` and `LAST_INSERT_ID` support replay convergence in core chat/memory and identity/reference rows. D1 must use SQLite-compatible conflict handling plus deterministic/retrievable IDs.
3. **JSON predicates and current projections** — logical replacement, status and world-state queries use `JSON_EXTRACT`/`JSON_UNQUOTE` in correctness conditions. D1 needs separately tested JSON representation and indexes, or normalized columns.
4. **Foreign-key-wide administration** — `ResetAll` disables `FOREIGN_KEY_CHECKS`; MariaDB schema also contains cascade relations. Cloudflare needs an explicit D1 reset policy with restricted authorization, confirmation, audit and recovery semantics.
5. **Migration/recovery parity** — source locks and vector/count/hash/FK manifests are operational semantics, not merely rows. Cloudflare procedures must reproduce their observable result with D1 canonical state and Vectorize rebuild/reconciliation.

## Cloudflare release gates

Before a Cloudflare profile is presented as a same-experience deployment option, the maintainer must approve and verify:

1. A D1 schema and transaction/deterministic-ID strategy for every `C` row that relies on locks, MySQL upsert or JSON predicates.
2. The Vectorize contract for every `V` row: D1 canonical write → D1 outbox → asynchronous Vectorize mutation → recent-D1 fallback and ID deduplication, plus rebuild/recovery.
3. The operator contract for every `O` row: authentication/authorization, audit, destructive-action confirmation, migration/recovery and asynchronous job procedures.
4. The exact D1 table list and migration order. Vectorize remains rebuildable only; no Vectorize row becomes canonical truth.
5. Local MariaDB + ChromaDB defaults, public API and existing behavior remain unchanged.

No P capability may be reclassified as unsupported merely to shorten a Cloudflare initial slice. A genuine product scope reduction requires a separate explicit product decision; absent that decision, incomplete gates keep the Cloudflare profile not-ready rather than changing its behavior.
