# MariaDB Store capability inventory — Task M470-1 Stage 1

## 목적과 판정 규칙

이 문서는 Cloudflare D1 provider의 범위를 임의의 "minimal slice"로 정하기 전에, 현재 Archive Center runtime이 사용하는 MariaDB persistence capability를 전수 확인한 Stage 1 inventory다. 기준은 [`store.go`](../../go-service/internal/store/store.go#L537-L601)의 base `Store`, 같은 파일의 optional capability interfaces, MariaDB implementation, migrations, 그리고 실제 Go runtime consumer다.

- **R (required candidate)**: Cloudflare profile에서 해당 runtime feature를 켰을 때 data loss 없이 동작하려면 D1이 제공해야 하는 capability다.
- **D (deferred candidate)**: first Cloudflare release의 필수 흐름은 아니지만 MariaDB local runtime에서는 계속 제공한다. D1에 넣기 전 별도 schema/concurrency 검증이 필요하다.
- **U (explicitly unsupported initially)**: Cloudflare profile에서는 profile guard로 명시적으로 차단한다. silent no-op이나 partial write는 금지한다.
- 이 문서는 proposed disposition이다. Stage 2/3은 이 matrix의 D1 disposition을 작업지시자가 review·승인한 뒤에만 시작한다.

## 조사 범위와 증거

- production MariaDB implementation은 `mariadb*.go` 25개 파일이며, static receiver scan으로 `*mariadbStore` method 312개를 확인했다. `_test.go`와 test-only fake는 이 수에서 제외한다.
- schema source는 [`migrations/001_schema.sql`](../../migrations/001_schema.sql)부터 [`migrations/014_character_state_field_provenance.sql`](../../migrations/014_character_state_field_provenance.sql)까지다. [`mariadb.go`](../../go-service/internal/store/mariadb.go#L60-L133)의 reset table registry도 runtime-owned table 집합의 cross-check로 사용했다.
- consumer는 `internal/httpapi`의 direct `s.Store` call, optional-interface assertion, startup/background worker와 [`internal/archive/bridge.go`](../../go-service/internal/archive/bridge.go)를 포함했다. test call만으로 disposition을 정하지 않았다.

## Store contract surface checklist

### Base `Store` (모두 MariaDB 구현됨)

| Capability family | Base methods | Current runtime use | Proposed D1 disposition |
|---|---|---|---|
| Turn input ledger | `SaveChatLog`, `ListChatLogs`, `SaveEffectiveInput`, `GetEffectiveInput` | complete-turn, source acceptance, prepare-turn, archive bridge | R |
| Memory/fact retrieval | `SaveMemory`, `ListMemories`, `SaveEvidence`, `ListEvidence`, `SaveKGTriple`, `ListKGTriples` | extraction persistence, prepare-turn, explorer/admin export, reindex | R |
| Audit/critic/events | `SaveAuditLog`, `ListAuditLogs`, `SaveCriticFeedback`, `ListCriticFeedback`, `SaveCharacterEvent`, `ListCharacterEvents` | turn workflow, state repair, admin/explorer | R for normal writes; D for admin-only query breadth |
| Session and narrative reads | `Stats`, `ListSessions`, `GetResumePack`, `ListStorylines`, `ListWorldRules`, `ListInheritedWorldRules`, `ListCharacterStates`, `GetCharacterState`, `ListPendingThreads`, `ListActiveStates`, `ListCanonicalStateLayers`, `ListEpisodeSummaries`, `GetEpisodeSummary` | prepare-turn/extraction plus session/admin/read endpoints | R for enabled normal prompt path; D/U per non-core endpoint after route review |

### Optional interface families (모두 inventory 대상)

| Group | Interfaces checked | Current owner/consumer class | Proposed D1 disposition |
|---|---|---|---|
| Health and administration | `Pinger`, `ShadowStatusReporter`, `AdminResetStore`, `SessionStateSnapshotReader`, `AuditLogCounter` | health, diagnostics, reset/export | `Pinger` R; shadow-only status not applicable; reset/snapshot/export D or U |
| Turn window and replacement | `PrepareTurnRangeStore`, `TimelineTurnIndexStore`, `LogicalTurnReplacementStore`, `RollbackStore`, `EffectiveInputListStore` | prepare-turn, reroll, source acceptance, explorer | range/timeline R; atomic replacement/rollback D until D1 transaction proof |
| Source and memory lifecycle | `SourceRevisionStore`, `CriticInputSnapshotStore`, `MemoryDerivationLifecycleAvailability`, `ActiveSourceRevisionLister`, `SourceRevisionHistoryLister`, `MemoryReprocessingJobStore`, `MemoryReprocessingWakeScheduleStore`, `MemoryReprocessingJobReopener`, `MemoryVectorOutboxStore`, `MemoryVectorOutboxLaneStore`, `MemoryVectorMaterializedCompletionStore`, `MemoryVectorOutboxMaintenanceStore`, `PreciseMemoryWriter`, `CharacterPerspectiveMemoryReader`, `ActiveInteractionMemoryReader`, `GeneralVectorPreciseMemoryReader`, `PreciseMemoryWriteAvailability`, `MemoryAdmissionProjectionInspector`, `MemoryAdmissionWriter`, `MemoryAdmissionWriteAvailability`, `VectorRecoveryCacheReader` | admission, precise memory, outbox/recovery, HUD | source revision/outbox lifecycle R; admission, precise memory, reprocessing and recovery D pending worker contract |
| State and world projection | `ActiveScopeStore`, `CharacterStateHistoryStore`, `CharacterProvenanceRepairStore`, `ReversibleStatusTransitionStore`, `StatusCurrentValueStore`, `StatusSchemaProposalStore`, `StatusSchemaRegistryStore`, `StatusLifecycleStore`, `StatusChangeEventSourceLookupStore`, `StateRepairArtifactReader`, `GuidancePlanStateStore`, `ChapterSummaryStore`, `EpisodeSummaryStore`, `ArcSummaryStore`, `SagaDigestStore`, `ThemeOffscreenCarryStore`, `ConsequenceRecordStore`, `PsychologyBranchStore`, `CaptureVerificationStore` | extraction, story clock, body tracking, repair/admin, summaries | normal projection read/write R only after route-by-route proof; repair, manual review, long-horizon summaries D/U |
| Entity and persona | `EntityIdentityWriter`, `EntityIdentityWriteAvailability`, `EntityIdentityLinkWriter`, `EntityIdentityCatalogReader`, `UniqueActiveEntitySurfaceIdentityResolver`, `UniqueActiveEntitySurfaceResolver`, `ReviewedEntityIdentityResolver`, `PersonaCapsuleStore`, `ProtagonistEntityMemoryOwnerIndexStore`, `ProtagonistEntityMemoryStore`, `ProtagonistEntityMemoryRepairStore`, `ProtagonistEntityMemoryManagementStore` | extraction identity, character recollection, persona/admin | active identity resolution R if extraction enabled; persona/repair/management D/U |
| Reference and canon | `LorebookReferenceStore`, `LorebookReferenceExplorerStore`, `ReferenceLibraryStore`, `CanonPackStore`, `CanonRegistryStore`, `ReferenceCoverageStore`, `SourceDiscoveryStore`, `SourceDiscoveryMutableStore`, `SourceDiscoveryQueryStore` | lorebook prepare-turn, reference/admin, canon install/discovery | lorebook read decision required; authoring/install/discovery U initially |
| Session migration/worldline | `SessionMigrationStore`, `SessionRoutingBaselineStore`, `SessionMigrationVectorStore`, `SessionMigrationVectorParityStore`, `SessionMigrationSourceLockStore`, `SessionMigrationSourceLockFenceStore`, `SessionMigrationRecoveryStore`, `SessionRouteBindingStore`, `SessionStitchStore`, `ForkLineageStore`, `WorldlineTopologySnapshotStore` | migration, route binding, stitch and worldline endpoints | U initially; does not block existing local MariaDB feature |
| Explorer mutation | `ExplorerMutationStore`, `SupersessionResolutionStore` | memory explorer and fact review | D/U; never silently omit mutation |

The optional interfaces above are a capability checklist, not a claim that every one must be D1-supported in the first release. Any interface not implemented by the D1 provider must be absent from its capability set and the owning Cloudflare route must return a deterministic profile-unsupported result.

## MariaDB implementation and schema matrix

| Implementation source | Persisted tables / operation family | Runtime consumers | MariaDB-specific dependency | Proposed disposition |
|---|---|---|---|---|
| [`mariadb.go`](../../go-service/internal/store/mariadb.go) | connection ping, complete reset, read-only session snapshot; reset registry spans all session tables | health, diagnostics, admin | `FOREIGN_KEY_CHECKS`, multi-table transaction | ping R; reset/snapshot D/U |
| [`mariadb_chat_memory.go`](../../go-service/internal/store/mariadb_chat_memory.go) | `chat_logs`, `effective_input_logs`, `memories`, `direct_evidence_records`, `kg_triples`, `audit_logs` | normal complete/prepare turn, recall, archive bridge, explorer | unique replay uses `ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)` | R |
| [`mariadb_logical_turn_replace.go`](../../go-service/internal/store/mariadb_logical_turn_replace.go), [`mariadb_delete.go`](../../go-service/internal/store/mariadb_delete.go) | canonical-tail replacement, rollback and session deletion across derived tables | reroll, source acceptance, deletion | one transaction, `SELECT … FOR UPDATE`, JSON provenance predicates | D until transaction and rollback parity tests pass |
| [`mariadb_memory_derivation.go`](../../go-service/internal/store/mariadb_memory_derivation.go), [`mariadb_memory_admission.go`](../../go-service/internal/store/mariadb_memory_admission.go), [`mariadb_precise_memory.go`](../../go-service/internal/store/mariadb_precise_memory.go) | `memory_source_revisions`, `precise_memory_units`, dependencies, jobs, `memory_vector_outbox` | turn admission, prepare recall, outbox, workflow HUD | leases and source revision conflict rows guarded by repeated `FOR UPDATE` transactions | source revision/outbox R; admission, precise memory, jobs D |
| [`mariadb_vector_recovery.go`](../../go-service/internal/store/mariadb_vector_recovery.go) | recovery cache/readback of memory vector material | startup vector recovery | provider recovery ordering | D; Vectorize design must not require immediate query readback |
| [`mariadb_character_feedback.go`](../../go-service/internal/store/mariadb_character_feedback.go) | `critic_feedback`, `character_events`, `entities`, `trust_states`, stats | extraction, character view, admin | append/read aggregation | normal event/entity writes R when extraction enabled; stats/admin breadth D |
| [`mariadb_narrative_state.go`](../../go-service/internal/store/mariadb_narrative_state.go), [`mariadb_story_world.go`](../../go-service/internal/store/mariadb_story_world.go), [`mariadb_runtime_state.go`](../../go-service/internal/store/mariadb_runtime_state.go) | storylines, world rules, character/active/canonical/pending state, guidance and summary rows | prepare-turn, extraction, body tracking, story clock | JSON fields and latest/current projection selection | R only for normal enabled projection path; long-horizon summaries D |
| [`mariadb_status.go`](../../go-service/internal/store/mariadb_status.go), [`mariadb_state_repair_artifacts.go`](../../go-service/internal/store/mariadb_state_repair_artifacts.go) | status schema/proposals/current values/events/effects and repair artifacts | body tracking, conditions, story clock, state repair | current/event atomic transition, JSON provenance lookup, stale projection validation | normal status transition R only after atomic D1 contract; repair/admin D/U |
| [`mariadb_entity_identity.go`](../../go-service/internal/store/mariadb_entity_identity.go) | entity identities, surfaces, links, artifact bindings | extraction and character/recollection resolution | upsert, review-state mutation, locked identity changes | active resolver R if identity feature is enabled; catalog/review tooling D |
| [`mariadb_persona.go`](../../go-service/internal/store/mariadb_persona.go) | persona capsules, entries, attachments, protagonist entity memories | recollection/persona and management routes | FK/cascade relationships, scoped reads | D/U initially |
| [`mariadb_lorebook_reference.go`](../../go-service/internal/store/mariadb_lorebook_reference.go), [`mariadb_reference_library.go`](../../go-service/internal/store/mariadb_reference_library.go), [`mariadb_reference_coverage.go`](../../go-service/internal/store/mariadb_reference_coverage.go) | lorebook entries/snapshots/scopes/locks; reference works/documents/entities/claims/coverage | prepare-turn lorebook, reference explorer/admin | lock reads and large relational reference graph | lorebook read requires separate approval; authoring/coverage U initially |
| [`mariadb_canon_pack.go`](../../go-service/internal/store/mariadb_canon_pack.go), [`mariadb_canon_registry.go`](../../go-service/internal/store/mariadb_canon_registry.go) | canon pack install, registry, overlay | canon administration/search | `FOR UPDATE`, `ON DUPLICATE KEY UPDATE` lifecycle transitions | U initially |
| [`mariadb_session_hierarchy.go`](../../go-service/internal/store/mariadb_session_hierarchy.go), [`mariadb_session_routing.go`](../../go-service/internal/store/mariadb_session_routing.go), [`mariadb_session_stitch.go`](../../go-service/internal/store/mariadb_session_stitch.go), [`mariadb_migration.go`](../../go-service/internal/store/mariadb_migration.go) | session list/resume, routing baseline/bindings, stitch and migration manifests | admin migration, worldline and session controls | manifest hash/count/FK/vector parity and source locks | U initially |
| [`mariadb_source_discovery.go`](../../go-service/internal/store/mariadb_source_discovery.go) | source discovery jobs | reference/canon discovery flow | job lifecycle and locks | U initially |

All production `mariadb*.go` implementation files are represented in this table. The stage report must reject a D1 design that adds a table without a matrix row or that implements a row without identifying its consumer and D1 disposition.

## SQL and transaction compatibility boundary

The following MariaDB behavior is present in live implementation and cannot be copied mechanically into D1:

1. **Row locking and serial workflow** — `FOR UPDATE` appears in canon-pack, identity, logical-turn, lorebook, memory admission/derivation, migration, routing, stitch and status flows. D1 implementations need an explicit transaction/retry/idempotency contract; a syntactic SQL rewrite is insufficient.
2. **MySQL upsert and generated identity** — `ON DUPLICATE KEY UPDATE` and `LAST_INSERT_ID` support replay convergence in core chat/memory and identity/reference rows. D1 must use SQLite-compatible conflict handling plus deterministic/retrievable IDs.
3. **JSON predicates and current projections** — logical replacement, status and world-state queries use `JSON_EXTRACT`/`JSON_UNQUOTE` in correctness conditions. D1 needs separately tested JSON representation and indexes, or normalized columns.
4. **Foreign-key-wide administration** — `ResetAll` disables `FOREIGN_KEY_CHECKS`; MariaDB schema also contains cascade relations. A stateless Cloudflare profile must not expose this destructive path until an explicit D1 reset policy exists.
5. **Migration/recovery parity** — source locks and vector/count/hash/FK manifests are MariaDB operational semantics, not merely rows. They remain explicitly unsupported until Cloudflare procedures are designed and tested.

## D1 release gate

Before any D1 schema or provider code is written, the maintainer must approve the following per-family choices:

1. Whether the Cloudflare first release includes the normal projection families (status/world/character/entity) in addition to the immutable turn ledger.
2. Whether lorebook read is part of the first release, independent of reference/canon authoring.
3. Which D/U families receive a profile guard and what HTTP/UI response exposes that boundary.
4. The D1 transaction and deterministic-ID strategy for every R row that now relies on locks, MySQL upsert, or JSON predicates.
5. The exact D1 table list. Vectorize remains rebuildable only; no Vectorize row can become canonical truth.

Until those choices are approved, MariaDB + ChromaDB remains the complete local runtime and no D1 profile may claim full Store parity.
