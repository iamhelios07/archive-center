-- Archive Center Cloudflare D1 - consolidated canonical schema.
--
-- GENERATED FILE. DO NOT EDIT BY HAND.
--
-- Produced by:
--   go-service/cmd/d1-schema-gen
-- from the MariaDB migrations:
--   migrations/001_schema.sql
--   migrations/002_canon_pack_storage.sql
--   migrations/003_entity_identity_v1.sql
--   migrations/004_precise_memory_units.sql
--   migrations/005_memory_derivation_lifecycle.sql
--   migrations/006_memory_admission_writer.sql
--   migrations/007_session_migration_manifest_and_route_binding.sql
--   migrations/008_chat_log_turn_role_uniqueness.sql
--   migrations/009_critic_input_snapshot.sql
--   migrations/010_lorebook_reference_entries.sql
--   migrations/011_session_fork_lineage_worldline.sql
--   migrations/012_session_fork_lineage_source_role.sql
--   migrations/013_precise_memory_text_fields.sql
--   migrations/014_character_state_field_provenance.sql
--
-- Why this is a consolidated schema rather than a replay of the source
-- migrations: SQLite ALTER TABLE supports only ADD COLUMN, RENAME COLUMN,
-- RENAME TO, and DROP COLUMN. It cannot MODIFY a column or add a constraint
-- afterwards, so every MariaDB ALTER is folded into each table's final
-- shape and declared once, with foreign keys inline.
--
-- Two type decisions carry parity meaning:
--   * BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY becomes
--     INTEGER PRIMARY KEY AUTOINCREMENT. A bare INTEGER PRIMARY KEY would
--     reuse ids after a full delete, while MariaDB never reuses
--     AUTO_INCREMENT values.
--   * JSON becomes TEXT. JSON predicates are applied with SQLite json_type()
--     and json_extract() through the helpers in internal/store/d1_dialect.go.
--
-- SQLite internal objects (sqlite_sequence, sqlite_schema, sqlite_autoindex_*)
-- are intentionally absent and must never be targeted by the reset allowlist.

-- chat_logs
CREATE TABLE IF NOT EXISTS "chat_logs" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "turn_index" INTEGER NOT NULL,
    "role" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "turn_index", "role")
);
CREATE INDEX IF NOT EXISTS "chat_logs_idx_session_turn" ON "chat_logs" ("chat_session_id", "turn_index");
CREATE INDEX IF NOT EXISTS "idx_session_role" ON "chat_logs" ("chat_session_id", "role");

-- effective_input_logs
CREATE TABLE IF NOT EXISTS "effective_input_logs" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "turn_index" INTEGER NOT NULL,
    "effective_input" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "effective_input_logs_idx_session_turn" ON "effective_input_logs" ("chat_session_id", "turn_index");
CREATE INDEX IF NOT EXISTS "effective_input_logs_idx_session" ON "effective_input_logs" ("chat_session_id");

-- memories
CREATE TABLE IF NOT EXISTS "memories" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "turn_index" INTEGER NOT NULL,
    "summary_json" TEXT,
    "embedding" TEXT,
    "embedding_model" TEXT,
    "importance" REAL,
    "emotional_boost" REAL,
    "evidence" TEXT,
    "emotional_intensity" REAL,
    "narrative_significance" REAL,
    "place_wing" TEXT,
    "place_room" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "memories_idx_session_turn" ON "memories" ("chat_session_id", "turn_index");
CREATE INDEX IF NOT EXISTS "idx_importance" ON "memories" ("chat_session_id", "importance");
CREATE INDEX IF NOT EXISTS "idx_wing_room" ON "memories" ("chat_session_id", "place_wing", "place_room");

-- direct_evidence_records
CREATE TABLE IF NOT EXISTS "direct_evidence_records" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "evidence_kind" TEXT NOT NULL DEFAULT 'fact_event',
    "evidence_text" TEXT NOT NULL,
    "source_turn_start" INTEGER NOT NULL,
    "source_turn_end" INTEGER NOT NULL,
    "turn_anchor" INTEGER NULL,
    "source_message_ids_json" TEXT,
    "source_hash" TEXT,
    "archive_state" TEXT NOT NULL DEFAULT 'pending_capture',
    "capture_stage" TEXT NOT NULL DEFAULT 'critic_extract',
    "capture_verification" TEXT NOT NULL DEFAULT 'pending',
    "committed_gate" TEXT,
    "lineage_json" TEXT,
    "repair_needed" INTEGER NOT NULL DEFAULT 0,
    "tombstoned" INTEGER NOT NULL DEFAULT 0,
    "superseded_by_id" INTEGER NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "direct_evidence_records_idx_session_state" ON "direct_evidence_records" ("chat_session_id", "archive_state");
CREATE INDEX IF NOT EXISTS "idx_session_kind" ON "direct_evidence_records" ("chat_session_id", "evidence_kind");
CREATE INDEX IF NOT EXISTS "idx_source_turn" ON "direct_evidence_records" ("chat_session_id", "source_turn_start", "source_turn_end");

-- kg_triples
CREATE TABLE IF NOT EXISTS "kg_triples" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "subject" TEXT NOT NULL,
    "predicate" TEXT NOT NULL,
    "object" TEXT NOT NULL,
    "valid_from" INTEGER NULL,
    "valid_to" INTEGER NULL,
    "source_turn" INTEGER NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_session_spo" ON "kg_triples" ("chat_session_id", "subject", "predicate", "object");
CREATE INDEX IF NOT EXISTS "idx_valid" ON "kg_triples" ("chat_session_id", "valid_from", "valid_to");

-- audit_logs
CREATE TABLE IF NOT EXISTS "audit_logs" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "event_type" TEXT NOT NULL,
    "chat_session_id" TEXT NULL,
    "target_type" TEXT NULL,
    "target_id" INTEGER NULL,
    "summary" TEXT,
    "details_json" TEXT,
    "source" TEXT DEFAULT 'api'
);
CREATE INDEX IF NOT EXISTS "audit_logs_idx_created" ON "audit_logs" ("created_at");
CREATE INDEX IF NOT EXISTS "idx_event" ON "audit_logs" ("event_type", "created_at");
CREATE INDEX IF NOT EXISTS "idx_session_event" ON "audit_logs" ("chat_session_id", "event_type", "created_at");

-- critic_feedback
CREATE TABLE IF NOT EXISTS "critic_feedback" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "target_type" TEXT NOT NULL,
    "target_id" INTEGER NOT NULL,
    "feedback_value" TEXT NOT NULL,
    "feedback_note" TEXT,
    "source" TEXT DEFAULT 'manual_ui'
);
CREATE INDEX IF NOT EXISTS "critic_feedback_idx_session_target" ON "critic_feedback" ("chat_session_id", "target_type", "target_id");
CREATE INDEX IF NOT EXISTS "critic_feedback_idx_created" ON "critic_feedback" ("created_at");

-- persona_memory_capsules
CREATE TABLE IF NOT EXISTS "persona_memory_capsules" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "persona_key" TEXT NOT NULL,
    "source_chat_session_id" TEXT NOT NULL,
    "source_character_name" TEXT,
    "title" TEXT NOT NULL,
    "mode" TEXT NOT NULL DEFAULT 'manual',
    "summary" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_persona_key" ON "persona_memory_capsules" ("persona_key");
CREATE INDEX IF NOT EXISTS "persona_memory_capsules_idx_source_session" ON "persona_memory_capsules" ("source_chat_session_id");
CREATE INDEX IF NOT EXISTS "idx_capsule_updated" ON "persona_memory_capsules" ("updated_at");

-- persona_memory_entries
CREATE TABLE IF NOT EXISTS "persona_memory_entries" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "capsule_id" INTEGER NOT NULL,
    "source_memory_type" TEXT NULL,
    "source_memory_id" INTEGER NULL,
    "source_turn_index" INTEGER NULL,
    "memory_text" TEXT NOT NULL,
    "emotional_weight" REAL NULL,
    "importance_10" REAL NULL,
    "portability" TEXT NOT NULL DEFAULT 'same_chat',
    "tags_json" TEXT,
    "evidence_excerpt" TEXT,
    "injection_policy" TEXT NOT NULL DEFAULT 'support_only',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("capsule_id") REFERENCES "persona_memory_capsules" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_capsule_turn" ON "persona_memory_entries" ("capsule_id", "source_turn_index");
CREATE INDEX IF NOT EXISTS "idx_source_memory_ref" ON "persona_memory_entries" ("source_memory_type", "source_memory_id");

-- protagonist_entity_memories
CREATE TABLE IF NOT EXISTS "protagonist_entity_memories" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "persona_entity_key" TEXT NOT NULL,
    "persona_entity_name" TEXT NOT NULL,
    "owner_entity_key" TEXT NOT NULL DEFAULT '',
    "owner_entity_name" TEXT NOT NULL DEFAULT '',
    "owner_entity_role" TEXT NOT NULL DEFAULT 'protagonist',
    "owner_visibility" TEXT NOT NULL DEFAULT 'player_known',
    "source_chat_session_id" TEXT NOT NULL,
    "source_character_name" TEXT,
    "source_turn_index" INTEGER NULL,
    "memory_text" TEXT NOT NULL,
    "evidence_excerpt" TEXT,
    "secret_guard" INTEGER NOT NULL DEFAULT 0,
    "portability" TEXT NOT NULL DEFAULT 'portable_persona_recollection',
    "target_reveal_policy" TEXT NOT NULL DEFAULT 'requires_explicit_attachment',
    "tags_json" TEXT,
    "importance_10" REAL NULL,
    "emotional_weight" REAL NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_entity_source" ON "protagonist_entity_memories" ("persona_entity_key", "source_chat_session_id", "source_turn_index");
CREATE INDEX IF NOT EXISTS "idx_owner_source" ON "protagonist_entity_memories" ("owner_entity_key", "source_chat_session_id", "source_turn_index");
CREATE INDEX IF NOT EXISTS "idx_owner_visibility" ON "protagonist_entity_memories" ("owner_entity_key", "owner_entity_role", "owner_visibility");
CREATE INDEX IF NOT EXISTS "idx_entity_updated" ON "protagonist_entity_memories" ("persona_entity_key", "updated_at");
CREATE INDEX IF NOT EXISTS "protagonist_entity_memories_idx_source_session" ON "protagonist_entity_memories" ("source_chat_session_id");

-- persona_capsule_attachments
CREATE TABLE IF NOT EXISTS "persona_capsule_attachments" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "capsule_id" INTEGER NOT NULL,
    "target_chat_session_id" TEXT NOT NULL,
    "injection_mode" TEXT NOT NULL DEFAULT 'subtle_deja_vu',
    "enabled" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("capsule_id", "target_chat_session_id"),
    FOREIGN KEY ("capsule_id") REFERENCES "persona_memory_capsules" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_persona_attachment_target" ON "persona_capsule_attachments" ("target_chat_session_id", "enabled");

-- character_events
CREATE TABLE IF NOT EXISTS "character_events" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "character_name" TEXT NOT NULL,
    "turn_index" INTEGER NULL,
    "event_type" TEXT NOT NULL,
    "details_json" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "character_events_idx_session_char" ON "character_events" ("chat_session_id", "character_name");
CREATE INDEX IF NOT EXISTS "character_events_idx_session_turn" ON "character_events" ("chat_session_id", "turn_index");

-- entities
CREATE TABLE IF NOT EXISTS "entities" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "entity_type" TEXT,
    "description" TEXT,
    "aliases_json" TEXT,
    "first_seen_turn" INTEGER,
    "last_seen_turn" INTEGER,
    "confidence" REAL,
    "pinned" INTEGER NOT NULL DEFAULT 0,
    "suppressed" INTEGER NOT NULL DEFAULT 0,
    "user_corrected" INTEGER NOT NULL DEFAULT 0,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_session_name" ON "entities" ("chat_session_id", "name");
CREATE INDEX IF NOT EXISTS "entities_idx_session_type" ON "entities" ("chat_session_id", "entity_type");

-- trust_states
CREATE TABLE IF NOT EXISTS "trust_states" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "target_name" TEXT NOT NULL,
    "target_type" TEXT,
    "score" REAL,
    "reason_json" TEXT,
    "source_turn" INTEGER,
    "pinned" INTEGER NOT NULL DEFAULT 0,
    "suppressed" INTEGER NOT NULL DEFAULT 0,
    "user_corrected" INTEGER NOT NULL DEFAULT 0,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "trust_states_idx_session_target" ON "trust_states" ("chat_session_id", "target_name");
CREATE INDEX IF NOT EXISTS "trust_states_idx_session_turn" ON "trust_states" ("chat_session_id", "source_turn");

-- storylines
CREATE TABLE IF NOT EXISTS "storylines" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'active',
    "entities_json" TEXT,
    "current_context" TEXT,
    "key_points_json" TEXT,
    "ongoing_tensions_json" TEXT,
    "confidence" REAL,
    "evidence_count" INTEGER,
    "last_evidence_turn" INTEGER,
    "first_turn" INTEGER,
    "last_turn" INTEGER,
    "pinned" INTEGER NOT NULL DEFAULT 0,
    "suppressed" INTEGER NOT NULL DEFAULT 0,
    "user_corrected" INTEGER NOT NULL DEFAULT 0,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "storylines_idx_session_status" ON "storylines" ("chat_session_id", "status");
CREATE INDEX IF NOT EXISTS "idx_session_last_turn" ON "storylines" ("chat_session_id", "last_turn");

-- guidance_plan_states
CREATE TABLE IF NOT EXISTS "guidance_plan_states" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "story_plan_json" TEXT,
    "director_json" TEXT,
    "state_status" TEXT NOT NULL DEFAULT 'empty',
    "last_turn" INTEGER NOT NULL DEFAULT -1,
    "warnings_json" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id")
);
CREATE INDEX IF NOT EXISTS "idx_guidance_plan_updated" ON "guidance_plan_states" ("updated_at");

-- world_rules
CREATE TABLE IF NOT EXISTS "world_rules" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "scope" TEXT NOT NULL,
    "scope_name" TEXT,
    "category" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "value_json" TEXT,
    "genre" TEXT,
    "source_turn" INTEGER,
    "pinned" INTEGER NOT NULL DEFAULT 0,
    "suppressed" INTEGER NOT NULL DEFAULT 0,
    "user_corrected" INTEGER NOT NULL DEFAULT 0,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_session_scope" ON "world_rules" ("chat_session_id", "scope", "category", "key");

-- session_active_scopes
CREATE TABLE IF NOT EXISTS "session_active_scopes" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "active_scope" TEXT NOT NULL DEFAULT 'root',
    "scope_name" TEXT,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id")
);

-- character_states
CREATE TABLE IF NOT EXISTS "character_states" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "character_name" TEXT NOT NULL,
    "appearance_json" TEXT,
    "personality_json" TEXT,
    "status_json" TEXT,
    "relationships_json" TEXT,
    "speech_style_json" TEXT,
    "field_provenance_json" TEXT NULL,
    "turn_index" INTEGER,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "character_states_idx_session_char" ON "character_states" ("chat_session_id", "character_name");
CREATE INDEX IF NOT EXISTS "character_states_idx_session_turn" ON "character_states" ("chat_session_id", "turn_index");

-- pending_threads
CREATE TABLE IF NOT EXISTS "pending_threads" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "thread_key" TEXT NOT NULL,
    "description" TEXT,
    "status" TEXT NOT NULL DEFAULT 'open',
    "created_turn" INTEGER,
    "resolved_turn" INTEGER,
    "source_turn" INTEGER,
    "priority" INTEGER,
    "hook_type" TEXT,
    "hook_metadata_json" TEXT,
    "pinned" INTEGER NOT NULL DEFAULT 0,
    "suppressed" INTEGER NOT NULL DEFAULT 0,
    "user_corrected" INTEGER NOT NULL DEFAULT 0,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "pending_threads_idx_session_status" ON "pending_threads" ("chat_session_id", "status");
CREATE INDEX IF NOT EXISTS "pending_threads_idx_session_source_turn" ON "pending_threads" ("chat_session_id", "source_turn");

-- active_states
CREATE TABLE IF NOT EXISTS "active_states" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "state_type" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    "turn_index" INTEGER NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "active_states_idx_session_type" ON "active_states" ("chat_session_id", "state_type", "turn_index");

-- canonical_state_layers
CREATE TABLE IF NOT EXISTS "canonical_state_layers" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "layer_type" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    "source_state_type" TEXT,
    "turn_index" INTEGER NOT NULL,
    "source_turn" INTEGER,
    "source_record" INTEGER,
    "last_verified_turn" INTEGER,
    "confidence" REAL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "canonical_state_layers_idx_session_type" ON "canonical_state_layers" ("chat_session_id", "layer_type", "turn_index");

-- episode_summaries
CREATE TABLE IF NOT EXISTS "episode_summaries" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "from_turn" INTEGER NOT NULL,
    "to_turn" INTEGER NOT NULL,
    "summary_text" TEXT NOT NULL,
    "key_entities" TEXT,
    "key_events" TEXT,
    "open_loops_json" TEXT,
    "relationship_changes_json" TEXT,
    "embedding_vector" TEXT,
    "embedding_model" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "episode_summaries_idx_session_turns" ON "episode_summaries" ("chat_session_id", "from_turn", "to_turn");

-- chapter_summaries
CREATE TABLE IF NOT EXISTS "chapter_summaries" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "from_turn" INTEGER NOT NULL,
    "to_turn" INTEGER NOT NULL,
    "chapter_index" INTEGER NOT NULL DEFAULT 0,
    "chapter_title" TEXT,
    "summary_text" TEXT NOT NULL,
    "open_loops_json" TEXT,
    "relationship_changes_json" TEXT,
    "world_changes_json" TEXT,
    "callback_candidates_json" TEXT,
    "resume_text" TEXT,
    "embedding_vector" TEXT,
    "embedding_model" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "chapter_summaries_idx_session_turns" ON "chapter_summaries" ("chat_session_id", "from_turn", "to_turn");
CREATE INDEX IF NOT EXISTS "idx_session_chapter" ON "chapter_summaries" ("chat_session_id", "chapter_index");

-- arc_summaries
CREATE TABLE IF NOT EXISTS "arc_summaries" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "from_turn" INTEGER NOT NULL,
    "to_turn" INTEGER NOT NULL,
    "arc_index" INTEGER NOT NULL DEFAULT 0,
    "arc_name" TEXT,
    "arc_status" TEXT NOT NULL DEFAULT 'active',
    "core_conflict" TEXT,
    "key_turning_points_json" TEXT,
    "active_promises_json" TEXT,
    "unresolved_debts_json" TEXT,
    "resolved_payoffs_json" TEXT,
    "callback_candidates_json" TEXT,
    "future_payoff_candidates_json" TEXT,
    "irreversible_turns_json" TEXT,
    "callback_debts_json" TEXT,
    "relationship_pivots_json" TEXT,
    "arc_resume_text" TEXT,
    "embedding_vector" TEXT,
    "embedding_model" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "arc_summaries_idx_session_turns" ON "arc_summaries" ("chat_session_id", "from_turn", "to_turn");
CREATE INDEX IF NOT EXISTS "idx_session_arc" ON "arc_summaries" ("chat_session_id", "arc_index");
CREATE INDEX IF NOT EXISTS "arc_summaries_idx_session_status" ON "arc_summaries" ("chat_session_id", "arc_status");

-- saga_digests
CREATE TABLE IF NOT EXISTS "saga_digests" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "from_turn" INTEGER NOT NULL,
    "to_turn" INTEGER NOT NULL,
    "era_label" TEXT,
    "saga_summary" TEXT NOT NULL,
    "persistent_facts_json" TEXT,
    "never_drop_candidates_json" TEXT,
    "resume_pack_text" TEXT,
    "embedding_vector" TEXT,
    "embedding_model" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "saga_digests_idx_session_turns" ON "saga_digests" ("chat_session_id", "from_turn", "to_turn");
CREATE INDEX IF NOT EXISTS "idx_session_created" ON "saga_digests" ("chat_session_id", "created_at");

-- session_migrations
CREATE TABLE IF NOT EXISTS "session_migrations" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "source_session_id" TEXT NOT NULL,
    "target_session_id" TEXT NOT NULL,
    "mode" TEXT NOT NULL DEFAULT 'copy_then_lock_source',
    "status" TEXT NOT NULL DEFAULT 'previewed',
    "preview_hash" TEXT NULL,
    "operator_note" TEXT,
    "counts_json" TEXT,
    "chroma_reindexed_count" INTEGER NOT NULL DEFAULT 0,
    "errors_json" TEXT,
    "started_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "completed_at" TEXT NULL,
    "locked_at" TEXT NULL,
    "cleanup_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_source" ON "session_migrations" ("source_session_id", "status");
CREATE INDEX IF NOT EXISTS "idx_session_migration_target" ON "session_migrations" ("target_session_id", "status");
CREATE INDEX IF NOT EXISTS "idx_session_migration_status" ON "session_migrations" ("status", "updated_at");

-- session_migration_row_map
CREATE TABLE IF NOT EXISTS "session_migration_row_map" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "migration_id" INTEGER NOT NULL,
    "table_name" TEXT NOT NULL,
    "source_row_id" INTEGER NOT NULL,
    "target_row_id" INTEGER NULL,
    "row_status" TEXT NOT NULL DEFAULT 'copied',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("migration_id", "table_name", "source_row_id"),
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_target_row" ON "session_migration_row_map" ("table_name", "target_row_id");

-- session_migration_reference_binding_map
CREATE TABLE IF NOT EXISTS "session_migration_reference_binding_map" (
    "migration_id" INTEGER NOT NULL,
    "source_binding_id" TEXT NOT NULL,
    "target_binding_id" TEXT NOT NULL,
    "row_status" TEXT NOT NULL DEFAULT 'copied',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    PRIMARY KEY ("migration_id", "source_binding_id"),
    UNIQUE ("migration_id", "target_binding_id"),
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);

-- session_migration_locks
CREATE TABLE IF NOT EXISTS "session_migration_locks" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "migration_id" INTEGER NOT NULL,
    "source_session_id" TEXT NOT NULL,
    "target_session_id" TEXT NOT NULL,
    "locked" INTEGER NOT NULL DEFAULT 1,
    "lock_status" TEXT NOT NULL DEFAULT 'migrated_away',
    "reason" TEXT,
    "locked_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "unlocked_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_lock_source" ON "session_migration_locks" ("source_session_id", "locked");
CREATE INDEX IF NOT EXISTS "idx_session_migration_lock_target" ON "session_migration_locks" ("target_session_id", "locked");
CREATE INDEX IF NOT EXISTS "idx_session_migration_lock_status" ON "session_migration_locks" ("lock_status", "updated_at");

-- consequence_records
CREATE TABLE IF NOT EXISTS "consequence_records" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "source_turn_start" INTEGER NOT NULL,
    "source_turn_end" INTEGER NOT NULL,
    "decision" TEXT NOT NULL,
    "immediate_result" TEXT NOT NULL,
    "delayed_effect" TEXT NOT NULL,
    "affected_relations" TEXT,
    "affected_world" TEXT,
    "status" TEXT NOT NULL DEFAULT 'active',
    "importance" REAL NOT NULL DEFAULT 0,
    "confidence" REAL NOT NULL DEFAULT 0,
    "foreground_eligible" INTEGER NOT NULL DEFAULT 0,
    "quiet_turns" INTEGER NOT NULL DEFAULT 0,
    "last_seen_turn" INTEGER NULL,
    "paid_turn" INTEGER NULL,
    "expires_after_quiet_turns" INTEGER NOT NULL DEFAULT 20,
    "source_hash" TEXT NULL,
    "evidence_json" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "consequence_records_idx_session_status" ON "consequence_records" ("chat_session_id", "status");
CREATE INDEX IF NOT EXISTS "consequence_records_idx_session_source_turn" ON "consequence_records" ("chat_session_id", "source_turn_start", "source_turn_end");
CREATE INDEX IF NOT EXISTS "consequence_records_idx_session_foreground" ON "consequence_records" ("chat_session_id", "foreground_eligible", "status");

-- psychology_branches
CREATE TABLE IF NOT EXISTS "psychology_branches" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "character_name" TEXT NOT NULL DEFAULT '',
    "branch_type" TEXT NOT NULL,
    "axis_name" TEXT NOT NULL,
    "summary" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'active',
    "confidence" REAL NOT NULL DEFAULT 0,
    "confidence_label" TEXT NULL,
    "source_kind" TEXT NULL,
    "source_turn_start" INTEGER NOT NULL,
    "source_turn_end" INTEGER NOT NULL,
    "source_hash" TEXT NULL,
    "evidence_json" TEXT,
    "quiet_turns" INTEGER NOT NULL DEFAULT 0,
    "last_seen_turn" INTEGER NULL,
    "dormant_after_quiet_turns" INTEGER NOT NULL DEFAULT 15,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "psychology_branches_idx_session_status" ON "psychology_branches" ("chat_session_id", "status");
CREATE INDEX IF NOT EXISTS "psychology_branches_idx_session_type" ON "psychology_branches" ("chat_session_id", "branch_type");
CREATE INDEX IF NOT EXISTS "idx_session_character" ON "psychology_branches" ("chat_session_id", "character_name");
CREATE INDEX IF NOT EXISTS "psychology_branches_idx_session_dormancy" ON "psychology_branches" ("chat_session_id", "status", "quiet_turns");

-- session_fork_lineage
CREATE TABLE IF NOT EXISTS "session_fork_lineage" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "contract_version" TEXT NOT NULL DEFAULT 'session_fork_lineage.v1',
    "lineage_state" TEXT NOT NULL DEFAULT 'manual',
    "chat_session_id" TEXT NOT NULL,
    "scope_id" TEXT NULL,
    "parent_scope_id" TEXT NULL,
    "copied_from_scope_id" TEXT NULL,
    "copied_from_session_id" TEXT NULL,
    "fork_turn" INTEGER NULL,
    "fork_source_message_id" TEXT NULL,
    "fork_source_role" TEXT NULL,
    "idempotency_key" TEXT NULL,
    "imported_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "divergence_marker" TEXT,
    "provenance_source" TEXT NOT NULL DEFAULT 'manual',
    "inheritance_mode" TEXT NOT NULL DEFAULT 'conservative_import',
    "inherited_items_json" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "idempotency_key")
);
CREATE INDEX IF NOT EXISTS "session_fork_lineage_idx_session" ON "session_fork_lineage" ("chat_session_id");
CREATE INDEX IF NOT EXISTS "idx_scope" ON "session_fork_lineage" ("scope_id");
CREATE INDEX IF NOT EXISTS "idx_parent_scope" ON "session_fork_lineage" ("parent_scope_id");
CREATE INDEX IF NOT EXISTS "idx_copied_from_scope" ON "session_fork_lineage" ("copied_from_scope_id");
CREATE INDEX IF NOT EXISTS "idx_provenance" ON "session_fork_lineage" ("provenance_source", "imported_at");

-- theme_offscreen_carries
CREATE TABLE IF NOT EXISTS "theme_offscreen_carries" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "surface_type" TEXT NOT NULL,
    "label" TEXT NOT NULL,
    "summary" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'active',
    "confidence" REAL NOT NULL DEFAULT 0,
    "confidence_label" TEXT NULL,
    "source_kind" TEXT NULL,
    "source_turn_start" INTEGER NOT NULL,
    "source_turn_end" INTEGER NOT NULL,
    "source_hash" TEXT NULL,
    "evidence_json" TEXT,
    "quiet_turns" INTEGER NOT NULL DEFAULT 0,
    "last_seen_turn" INTEGER NULL,
    "dormant_after_quiet_turns" INTEGER NOT NULL DEFAULT 15,
    "foreground_eligible" INTEGER NOT NULL DEFAULT 0,
    "foreground_reason_json" TEXT,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "theme_offscreen_carries_idx_session_type" ON "theme_offscreen_carries" ("chat_session_id", "surface_type");
CREATE INDEX IF NOT EXISTS "theme_offscreen_carries_idx_session_status" ON "theme_offscreen_carries" ("chat_session_id", "status");
CREATE INDEX IF NOT EXISTS "theme_offscreen_carries_idx_session_dormancy" ON "theme_offscreen_carries" ("chat_session_id", "status", "quiet_turns");
CREATE INDEX IF NOT EXISTS "theme_offscreen_carries_idx_session_foreground" ON "theme_offscreen_carries" ("chat_session_id", "surface_type", "foreground_eligible", "status");

-- capture_verification_records
CREATE TABLE IF NOT EXISTS "capture_verification_records" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "turn_index" INTEGER NOT NULL,
    "stage_name" TEXT NOT NULL,
    "verification_state" TEXT NOT NULL DEFAULT 'single-stage',
    "degraded_reason" TEXT NULL,
    "compact_metadata_json" TEXT,
    "content_hash" TEXT NULL,
    "evidence_json" TEXT,
    "previous_record_id" INTEGER NULL,
    "repaired_by_record_id" INTEGER NULL,
    "repair_attempt_count" INTEGER NOT NULL DEFAULT 0,
    "repair_evidence_json" TEXT,
    "repaired_at" TEXT NULL,
    "user_input_preserved" INTEGER NOT NULL DEFAULT 1,
    "payload_rewrite" INTEGER NOT NULL DEFAULT 0,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "capture_verification_records_idx_session_turn" ON "capture_verification_records" ("chat_session_id", "turn_index");
CREATE INDEX IF NOT EXISTS "idx_session_stage" ON "capture_verification_records" ("chat_session_id", "stage_name");
CREATE INDEX IF NOT EXISTS "capture_verification_records_idx_session_state" ON "capture_verification_records" ("chat_session_id", "verification_state");
CREATE INDEX IF NOT EXISTS "idx_previous_record" ON "capture_verification_records" ("previous_record_id");
CREATE INDEX IF NOT EXISTS "idx_repaired_by" ON "capture_verification_records" ("repaired_by_record_id");

-- status_schema_proposals
CREATE TABLE IF NOT EXISTS "status_schema_proposals" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "input_channel" TEXT NOT NULL DEFAULT 'bootstrap',
    "proposal_state" TEXT NOT NULL DEFAULT 'pending_review',
    "schema_name" TEXT NOT NULL,
    "ruleset_label" TEXT NULL,
    "schema_json" TEXT NOT NULL,
    "provenance_json" TEXT NULL,
    "review_note" TEXT NULL,
    "reviewer" TEXT NULL,
    "reviewed_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_status_schema_session" ON "status_schema_proposals" ("chat_session_id", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_status_schema_state" ON "status_schema_proposals" ("chat_session_id", "proposal_state", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_status_schema_input_channel" ON "status_schema_proposals" ("chat_session_id", "input_channel", "updated_at");

-- status_schema_registry
CREATE TABLE IF NOT EXISTS "status_schema_registry" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "source_proposal_id" INTEGER NULL,
    "schema_name" TEXT NOT NULL DEFAULT 'status_schema',
    "ruleset_label" TEXT NULL,
    "status_key" TEXT NOT NULL,
    "label" TEXT NOT NULL,
    "owner_scope" TEXT NOT NULL,
    "value_kind" TEXT NOT NULL,
    "bounds_json" TEXT NULL,
    "options_json" TEXT NULL,
    "default_value_json" TEXT NULL,
    "registry_state" TEXT NOT NULL DEFAULT 'active',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "schema_name", "status_key", "owner_scope"),
    FOREIGN KEY ("source_proposal_id") REFERENCES "status_schema_proposals" ("id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_status_registry_session" ON "status_schema_registry" ("chat_session_id", "registry_state", "status_key");
CREATE INDEX IF NOT EXISTS "idx_status_registry_proposal" ON "status_schema_registry" ("source_proposal_id");

-- status_current_values
CREATE TABLE IF NOT EXISTS "status_current_values" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "registry_id" INTEGER NOT NULL,
    "status_key" TEXT NOT NULL,
    "owner_scope" TEXT NOT NULL,
    "owner_id" TEXT NOT NULL,
    "owner_label" TEXT NULL,
    "value_kind" TEXT NOT NULL,
    "value_json" TEXT NOT NULL,
    "evidence_json" TEXT NOT NULL,
    "source_turn" INTEGER NULL,
    "write_state" TEXT NOT NULL DEFAULT 'current',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "registry_id", "owner_scope", "owner_id"),
    FOREIGN KEY ("registry_id") REFERENCES "status_schema_registry" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_status_current_session" ON "status_current_values" ("chat_session_id", "write_state", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_status_current_owner" ON "status_current_values" ("chat_session_id", "owner_scope", "owner_id", "status_key");
CREATE INDEX IF NOT EXISTS "idx_status_current_key" ON "status_current_values" ("chat_session_id", "status_key", "owner_scope");

-- status_change_events
CREATE TABLE IF NOT EXISTS "status_change_events" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "registry_id" INTEGER NOT NULL,
    "status_value_id" INTEGER NULL,
    "status_key" TEXT NOT NULL,
    "owner_scope" TEXT NOT NULL,
    "owner_id" TEXT NOT NULL,
    "event_kind" TEXT NOT NULL,
    "previous_value_json" TEXT NULL,
    "new_value_json" TEXT NULL,
    "evidence_json" TEXT NOT NULL,
    "source_turn" INTEGER NULL,
    "story_clock_json" TEXT NULL,
    "event_state" TEXT NOT NULL DEFAULT 'recorded',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("registry_id") REFERENCES "status_schema_registry" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("status_value_id") REFERENCES "status_current_values" ("id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_status_event_session" ON "status_change_events" ("chat_session_id", "created_at");
CREATE INDEX IF NOT EXISTS "idx_status_event_owner" ON "status_change_events" ("chat_session_id", "owner_scope", "owner_id", "status_key", "created_at");
CREATE INDEX IF NOT EXISTS "idx_status_event_registry" ON "status_change_events" ("registry_id", "created_at");

-- status_effects
CREATE TABLE IF NOT EXISTS "status_effects" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "registry_id" INTEGER NOT NULL,
    "status_key" TEXT NOT NULL,
    "owner_scope" TEXT NOT NULL,
    "owner_id" TEXT NOT NULL,
    "effect_kind" TEXT NOT NULL,
    "effect_label" TEXT NULL,
    "effect_payload_json" TEXT NULL,
    "evidence_json" TEXT NOT NULL,
    "source_turn" INTEGER NULL,
    "start_clock_json" TEXT NOT NULL,
    "duration_json" TEXT NULL,
    "expires_at_clock_json" TEXT NULL,
    "effect_state" TEXT NOT NULL DEFAULT 'active',
    "cleared_evidence_json" TEXT NULL,
    "cleared_turn" INTEGER NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("registry_id") REFERENCES "status_schema_registry" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_status_effect_session" ON "status_effects" ("chat_session_id", "effect_state", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_status_effect_owner" ON "status_effects" ("chat_session_id", "owner_scope", "owner_id", "status_key", "effect_state");
CREATE INDEX IF NOT EXISTS "idx_status_effect_registry" ON "status_effects" ("registry_id", "effect_state");

-- reference_works
CREATE TABLE IF NOT EXISTS "reference_works" (
    "work_id" TEXT PRIMARY KEY NOT NULL,
    "title" TEXT NOT NULL,
    "work_type" TEXT NOT NULL DEFAULT 'custom',
    "default_language" TEXT NOT NULL DEFAULT '',
    "status" TEXT NOT NULL DEFAULT 'draft',
    "metadata_json" TEXT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_reference_work_status" ON "reference_works" ("status", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_reference_work_title" ON "reference_works" ("title");

-- reference_continuities
CREATE TABLE IF NOT EXISTS "reference_continuities" (
    "continuity_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_key" TEXT NOT NULL,
    "label" TEXT NOT NULL,
    "parent_continuity_id" TEXT NULL,
    "status" TEXT NOT NULL DEFAULT 'active',
    "metadata_json" TEXT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("work_id", "continuity_key"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("parent_continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_reference_continuity_work" ON "reference_continuities" ("work_id", "status", "updated_at");

-- reference_documents
CREATE TABLE IF NOT EXISTS "reference_documents" (
    "document_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "source_type" TEXT NOT NULL DEFAULT 'manual_text',
    "source_uri" TEXT NULL,
    "content_hash" TEXT NOT NULL,
    "raw_retention" TEXT NOT NULL DEFAULT 'full',
    "raw_text" TEXT NULL,
    "import_status" TEXT NOT NULL DEFAULT 'pending',
    "provenance_json" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("work_id", "continuity_id", "content_hash"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_reference_document_status" ON "reference_documents" ("work_id", "continuity_id", "import_status", "updated_at");

-- reference_timeline_nodes
CREATE TABLE IF NOT EXISTS "reference_timeline_nodes" (
    "node_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "node_key" TEXT NOT NULL,
    "label" TEXT NOT NULL,
    "ordinal_value" INTEGER NOT NULL DEFAULT 0,
    "parent_node_id" TEXT NULL,
    "branch_key" TEXT NOT NULL DEFAULT 'main',
    "node_kind" TEXT NOT NULL DEFAULT 'event',
    "metadata_json" TEXT NULL,
    "review_status" TEXT NOT NULL DEFAULT 'pending',
    "review_source" TEXT NOT NULL DEFAULT '',
    "review_reason" TEXT NULL,
    "reviewed_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("continuity_id", "branch_key", "node_key"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE CASCADE,
    FOREIGN KEY ("parent_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_reference_timeline_order" ON "reference_timeline_nodes" ("continuity_id", "branch_key", "ordinal_value");
CREATE INDEX IF NOT EXISTS "idx_reference_timeline_review" ON "reference_timeline_nodes" ("work_id", "continuity_id", "review_status", "ordinal_value");

-- reference_entities
CREATE TABLE IF NOT EXISTS "reference_entities" (
    "entity_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "entity_type" TEXT NOT NULL,
    "canonical_name" TEXT NOT NULL,
    "description_text" TEXT NULL,
    "metadata_json" TEXT NULL,
    "review_status" TEXT NOT NULL DEFAULT 'pending',
    "review_source" TEXT NOT NULL DEFAULT '',
    "review_reason" TEXT NULL,
    "reviewed_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_reference_entity_name" ON "reference_entities" ("work_id", "continuity_id", "canonical_name");
CREATE INDEX IF NOT EXISTS "idx_reference_entity_type" ON "reference_entities" ("work_id", "continuity_id", "entity_type", "review_status");

-- reference_entity_aliases
CREATE TABLE IF NOT EXISTS "reference_entity_aliases" (
    "alias_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "entity_id" TEXT NOT NULL,
    "alias_text" TEXT NOT NULL,
    "normalized_alias" TEXT NOT NULL,
    "language_code" TEXT NOT NULL DEFAULT '',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("work_id", "continuity_id", "entity_id", "normalized_alias"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE CASCADE,
    FOREIGN KEY ("entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_reference_alias_lookup" ON "reference_entity_aliases" ("work_id", "continuity_id", "normalized_alias");

-- reference_claims
CREATE TABLE IF NOT EXISTS "reference_claims" (
    "claim_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "claim_type" TEXT NOT NULL,
    "subject_entity_id" TEXT NULL,
    "claim_text" TEXT NOT NULL,
    "evidence_excerpt" TEXT NULL,
    "temporal_scope" TEXT NOT NULL DEFAULT 'bounded',
    "valid_from_node_id" TEXT NULL,
    "valid_to_node_id" TEXT NULL,
    "reveal_from_node_id" TEXT NULL,
    "branch_key" TEXT NOT NULL DEFAULT 'main',
    "knowledge_scope" TEXT NOT NULL DEFAULT 'public_world',
    "confidence" REAL NOT NULL DEFAULT 0,
    "review_status" TEXT NOT NULL DEFAULT 'pending',
    "review_source" TEXT NOT NULL DEFAULT '',
    "review_reason" TEXT NULL,
    "reviewed_at" TEXT NULL,
    "metadata_json" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE CASCADE,
    FOREIGN KEY ("document_id") REFERENCES "reference_documents" ("document_id") ON DELETE CASCADE,
    FOREIGN KEY ("subject_entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("valid_from_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL,
    FOREIGN KEY ("valid_to_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL,
    FOREIGN KEY ("reveal_from_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_reference_claim_recall" ON "reference_claims" ("work_id", "continuity_id", "review_status", "branch_key", "reveal_from_node_id");
CREATE INDEX IF NOT EXISTS "idx_reference_claim_subject" ON "reference_claims" ("subject_entity_id", "claim_type");
CREATE INDEX IF NOT EXISTS "idx_reference_claim_document" ON "reference_claims" ("document_id", "review_status");

-- reference_claim_knowers
CREATE TABLE IF NOT EXISTS "reference_claim_knowers" (
    "claim_id" TEXT NOT NULL,
    "entity_id" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    PRIMARY KEY ("claim_id", "entity_id"),
    FOREIGN KEY ("claim_id") REFERENCES "reference_claims" ("claim_id") ON DELETE CASCADE,
    FOREIGN KEY ("entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_reference_knower_entity" ON "reference_claim_knowers" ("entity_id", "claim_id");

-- session_reference_bindings
CREATE TABLE IF NOT EXISTS "session_reference_bindings" (
    "binding_id" TEXT PRIMARY KEY NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "binding_role" TEXT NOT NULL DEFAULT 'primary',
    "reference_mode" TEXT NOT NULL DEFAULT 'supplement',
    "enabled" INTEGER NOT NULL DEFAULT 1,
    "injection_enabled" INTEGER NOT NULL DEFAULT 0,
    "anchor_mode" TEXT NOT NULL DEFAULT 'manual',
    "current_node_id" TEXT NULL,
    "reveal_ceiling_node_id" TEXT NULL,
    "divergence_node_id" TEXT NULL,
    "future_policy" TEXT NOT NULL DEFAULT 'block',
    "priority" INTEGER NOT NULL DEFAULT 0,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "work_id", "continuity_id"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE RESTRICT,
    FOREIGN KEY ("current_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL,
    FOREIGN KEY ("reveal_ceiling_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL,
    FOREIGN KEY ("divergence_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_session_reference_enabled" ON "session_reference_bindings" ("chat_session_id", "enabled", "priority");
CREATE INDEX IF NOT EXISTS "idx_reference_binding_work" ON "session_reference_bindings" ("work_id", "continuity_id", "enabled");

-- session_reference_runtime
CREATE TABLE IF NOT EXISTS "session_reference_runtime" (
    "binding_id" TEXT PRIMARY KEY NOT NULL,
    "candidate_node_id" TEXT NULL,
    "candidate_source_turn" INTEGER NULL,
    "candidate_evidence_json" TEXT NULL,
    "candidate_confirmed" INTEGER NOT NULL DEFAULT 0,
    "last_claim_ids_json" TEXT NULL,
    "diagnostics_json" TEXT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("binding_id") REFERENCES "session_reference_bindings" ("binding_id") ON DELETE CASCADE,
    FOREIGN KEY ("candidate_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL
);

-- session_reference_coverage_snapshots
CREATE TABLE IF NOT EXISTS "session_reference_coverage_snapshots" (
    "binding_id" TEXT PRIMARY KEY NOT NULL,
    "contract_version" TEXT NOT NULL,
    "context_hash" TEXT NOT NULL,
    "inventory_hash" TEXT NOT NULL,
    "snapshot_hash" TEXT NOT NULL,
    "source_message_count" INTEGER NOT NULL DEFAULT 0,
    "field_count" INTEGER NOT NULL DEFAULT 0,
    "covered_field_count" INTEGER NOT NULL DEFAULT 0,
    "stats_json" TEXT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("binding_id") REFERENCES "session_reference_bindings" ("binding_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_reference_coverage_snapshot_hash" ON "session_reference_coverage_snapshots" ("snapshot_hash");

-- session_reference_coverage_fields
CREATE TABLE IF NOT EXISTS "session_reference_coverage_fields" (
    "binding_id" TEXT NOT NULL,
    "field_key" TEXT NOT NULL,
    "work_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "reference_kind" TEXT NOT NULL,
    "source_id" TEXT NOT NULL,
    "field_name" TEXT NOT NULL,
    "field_value" TEXT NOT NULL,
    "normalized_value" TEXT NOT NULL,
    "match_values_json" TEXT NULL,
    "present_in_context" INTEGER NOT NULL DEFAULT 0,
    "matched_locations_json" TEXT NULL,
    "eligible" INTEGER NOT NULL DEFAULT 1,
    "eligibility_reason" TEXT NOT NULL DEFAULT 'eligible',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    PRIMARY KEY ("binding_id", "field_key"),
    FOREIGN KEY ("binding_id") REFERENCES "session_reference_coverage_snapshots" ("binding_id") ON DELETE CASCADE,
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE CASCADE,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_reference_coverage_source" ON "session_reference_coverage_fields" ("binding_id", "reference_kind", "source_id");
CREATE INDEX IF NOT EXISTS "idx_reference_coverage_presence" ON "session_reference_coverage_fields" ("binding_id", "present_in_context", "eligible");

-- reference_work_editions
CREATE TABLE IF NOT EXISTS "reference_work_editions" (
    "edition_row_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "stable_work_id" TEXT NOT NULL,
    "edition_id" TEXT NOT NULL,
    "identity_contract" TEXT NOT NULL DEFAULT 'canon_identity.v1',
    "original_language" TEXT NOT NULL DEFAULT '',
    "edition_language" TEXT NOT NULL DEFAULT '',
    "edition_label" TEXT NOT NULL,
    "edition_status" TEXT NOT NULL DEFAULT 'active',
    "metadata_json" TEXT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("stable_work_id", "edition_id"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    CONSTRAINT chk_reference_work_edition_status CHECK (edition_status IN ('active', 'inactive', 'deprecated'))
);
CREATE INDEX IF NOT EXISTS "idx_reference_work_edition_local" ON "reference_work_editions" ("work_id", "edition_status", "updated_at");

-- canon_pack_installs
CREATE TABLE IF NOT EXISTS "canon_pack_installs" (
    "install_id" TEXT PRIMARY KEY NOT NULL,
    "pack_id" TEXT NOT NULL,
    "pack_version" TEXT NOT NULL,
    "install_generation" INTEGER NOT NULL,
    "manifest_contract" TEXT NOT NULL,
    "manifest_sha256" TEXT NOT NULL,
    "manifest_json" TEXT NOT NULL,
    "work_id" TEXT NOT NULL,
    "edition_row_id" TEXT NOT NULL,
    "pack_status" TEXT NOT NULL,
    "review_status" TEXT NOT NULL,
    "trust_status" TEXT NOT NULL,
    "lifecycle_status" TEXT NOT NULL DEFAULT 'staged',
    "validation_report_json" TEXT NOT NULL,
    "coverage_report_json" TEXT NOT NULL,
    "installed_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "activated_at" TEXT NULL,
    "removed_at" TEXT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "active_generation_marker" INTEGER AS (CASE WHEN lifecycle_status = 'active' THEN 1 ELSE NULL END) STORED,
    UNIQUE ("pack_id", "pack_version", "install_generation"),
    UNIQUE ("pack_id", "edition_row_id", "active_generation_marker"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    CONSTRAINT chk_canon_pack_lifecycle CHECK (lifecycle_status IN ('staged', 'active', 'inactive', 'failed', 'removed')),
    CONSTRAINT chk_canon_pack_manifest_sha CHECK ((length(manifest_sha256) = 64 AND manifest_sha256 NOT GLOB '*[^0-9a-f]*'))
);
CREATE INDEX IF NOT EXISTS "idx_canon_pack_work_lifecycle" ON "canon_pack_installs" ("work_id", "edition_row_id", "lifecycle_status", "updated_at");

-- reference_source_observations
CREATE TABLE IF NOT EXISTS "reference_source_observations" (
    "observation_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "edition_row_id" TEXT NOT NULL,
    "continuity_id" TEXT NULL,
    "origin_kind" TEXT NOT NULL,
    "install_id" TEXT NULL,
    "source_key" TEXT NOT NULL,
    "source_type" TEXT NOT NULL,
    "source_uri" TEXT NULL,
    "license_json" TEXT NOT NULL,
    "access_class" TEXT NOT NULL,
    "retrieved_at" TEXT NOT NULL,
    "hash_contract" TEXT NOT NULL DEFAULT 'source_bytes_sha256.v1',
    "document_sha256" TEXT NOT NULL,
    "document_id" TEXT NULL,
    "provenance_json" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("install_id", "source_key"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE RESTRICT,
    FOREIGN KEY ("install_id") REFERENCES "canon_pack_installs" ("install_id") ON DELETE RESTRICT,
    FOREIGN KEY ("document_id") REFERENCES "reference_documents" ("document_id") ON DELETE SET NULL,
    CONSTRAINT chk_reference_source_origin CHECK (origin_kind IN ('canon_pack', 'source_discovery', 'user_local', 'legacy_local', 'local_overlay')),
    CONSTRAINT chk_reference_source_install_owner CHECK ((origin_kind = 'canon_pack' AND install_id IS NOT NULL) OR (origin_kind <> 'canon_pack' AND install_id IS NULL)),
    CONSTRAINT chk_reference_source_hash CHECK ((length(document_sha256) = 64 AND document_sha256 NOT GLOB '*[^0-9a-f]*'))
);
CREATE INDEX IF NOT EXISTS "idx_reference_source_scope" ON "reference_source_observations" ("work_id", "edition_row_id", "continuity_id", "origin_kind");
CREATE INDEX IF NOT EXISTS "idx_reference_source_hash" ON "reference_source_observations" ("hash_contract", "document_sha256");

-- reference_work_titles
CREATE TABLE IF NOT EXISTS "reference_work_titles" (
    "title_row_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "edition_row_id" TEXT NULL,
    "title_kind" TEXT NOT NULL,
    "title_text" TEXT NOT NULL,
    "language_code" TEXT NOT NULL DEFAULT '',
    "script_code" TEXT NOT NULL DEFAULT '',
    "normalization_contract" TEXT NOT NULL,
    "normalized_lookup_key" TEXT NOT NULL,
    "normalized_lookup_digest" TEXT NOT NULL,
    "source_observation_id" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "edition_scope_key" TEXT NOT NULL DEFAULT '',
    UNIQUE ("work_id", "edition_scope_key", "title_kind", "language_code", "normalization_contract", "normalized_lookup_digest"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    FOREIGN KEY ("source_observation_id") REFERENCES "reference_source_observations" ("observation_id") ON DELETE SET NULL,
    CONSTRAINT chk_reference_title_kind CHECK (title_kind IN ('original', 'translated', 'alias')),
    CONSTRAINT chk_reference_title_edition_scope CHECK (
        (edition_row_id IS NULL AND edition_scope_key = '') OR
        (edition_row_id IS NOT NULL AND edition_scope_key = edition_row_id)
    ),
    CONSTRAINT chk_reference_title_digest CHECK ((length(normalized_lookup_digest) = 64 AND normalized_lookup_digest NOT GLOB '*[^0-9a-f]*'))
);
CREATE INDEX IF NOT EXISTS "idx_reference_title_lookup" ON "reference_work_titles" ("normalized_lookup_key", "language_code", "title_kind");

-- reference_item_origins
CREATE TABLE IF NOT EXISTS "reference_item_origins" (
    "origin_membership_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "edition_row_id" TEXT NOT NULL,
    "item_kind" TEXT NOT NULL,
    "node_id" TEXT NULL,
    "entity_id" TEXT NULL,
    "claim_id" TEXT NULL,
    "origin_kind" TEXT NOT NULL,
    "origin_owner_id" TEXT NOT NULL,
    "install_id" TEXT NULL,
    "source_item_id" TEXT NOT NULL,
    "review_state" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("origin_kind", "origin_owner_id", "item_kind", "source_item_id"),
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    FOREIGN KEY ("node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE CASCADE,
    FOREIGN KEY ("entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE CASCADE,
    FOREIGN KEY ("claim_id") REFERENCES "reference_claims" ("claim_id") ON DELETE CASCADE,
    FOREIGN KEY ("install_id") REFERENCES "canon_pack_installs" ("install_id") ON DELETE RESTRICT,
    CONSTRAINT chk_reference_item_origin_target CHECK (
        (item_kind = 'timeline' AND node_id IS NOT NULL AND entity_id IS NULL AND claim_id IS NULL) OR
        (item_kind = 'entity' AND node_id IS NULL AND entity_id IS NOT NULL AND claim_id IS NULL) OR
        (item_kind = 'claim' AND node_id IS NULL AND entity_id IS NULL AND claim_id IS NOT NULL)
    ),
    CONSTRAINT chk_reference_item_origin_kind CHECK (origin_kind IN ('canon_pack', 'source_discovery', 'user_local', 'legacy_local', 'local_overlay')),
    CONSTRAINT chk_reference_item_origin_install_owner CHECK ((origin_kind = 'canon_pack' AND install_id IS NOT NULL) OR (origin_kind <> 'canon_pack' AND install_id IS NULL))
);
CREATE INDEX IF NOT EXISTS "idx_reference_item_origin_install" ON "reference_item_origins" ("install_id", "item_kind");
CREATE INDEX IF NOT EXISTS "idx_reference_item_origin_scope" ON "reference_item_origins" ("work_id", "edition_row_id", "item_kind");

-- reference_item_evidence
CREATE TABLE IF NOT EXISTS "reference_item_evidence" (
    "evidence_edge_id" TEXT PRIMARY KEY NOT NULL,
    "item_kind" TEXT NOT NULL,
    "node_id" TEXT NULL,
    "entity_id" TEXT NULL,
    "claim_id" TEXT NULL,
    "source_observation_id" TEXT NOT NULL,
    "document_hash_contract" TEXT NOT NULL,
    "document_sha256" TEXT NOT NULL,
    "locator_json" TEXT NOT NULL,
    "locator_digest" TEXT NOT NULL,
    "evidence_state" TEXT NOT NULL DEFAULT 'active',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("node_id", "source_observation_id", "document_hash_contract", "document_sha256", "locator_digest"),
    UNIQUE ("entity_id", "source_observation_id", "document_hash_contract", "document_sha256", "locator_digest"),
    UNIQUE ("claim_id", "source_observation_id", "document_hash_contract", "document_sha256", "locator_digest"),
    FOREIGN KEY ("node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE RESTRICT,
    FOREIGN KEY ("entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE RESTRICT,
    FOREIGN KEY ("claim_id") REFERENCES "reference_claims" ("claim_id") ON DELETE RESTRICT,
    FOREIGN KEY ("source_observation_id") REFERENCES "reference_source_observations" ("observation_id") ON DELETE RESTRICT,
    CONSTRAINT chk_reference_item_evidence_target CHECK (
        (item_kind = 'timeline' AND node_id IS NOT NULL AND entity_id IS NULL AND claim_id IS NULL) OR
        (item_kind = 'entity' AND node_id IS NULL AND entity_id IS NOT NULL AND claim_id IS NULL) OR
        (item_kind = 'claim' AND node_id IS NULL AND entity_id IS NULL AND claim_id IS NOT NULL)
    ),
    CONSTRAINT chk_reference_evidence_hashes CHECK ((length(document_sha256) = 64 AND document_sha256 NOT GLOB '*[^0-9a-f]*') AND (length(locator_digest) = 64 AND locator_digest NOT GLOB '*[^0-9a-f]*'))
);
CREATE INDEX IF NOT EXISTS "idx_reference_evidence_source" ON "reference_item_evidence" ("source_observation_id", "evidence_state");

-- reference_logical_facts
CREATE TABLE IF NOT EXISTS "reference_logical_facts" (
    "logical_fact_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "edition_row_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "applicability_scope_digest" TEXT NOT NULL,
    "fact_status" TEXT NOT NULL DEFAULT 'active',
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE RESTRICT,
    CONSTRAINT chk_reference_logical_fact_scope CHECK ((length(applicability_scope_digest) = 64 AND applicability_scope_digest NOT GLOB '*[^0-9a-f]*'))
);
CREATE INDEX IF NOT EXISTS "idx_reference_logical_fact_scope" ON "reference_logical_facts" ("work_id", "edition_row_id", "continuity_id", "fact_status");

-- reference_fact_identities
CREATE TABLE IF NOT EXISTS "reference_fact_identities" (
    "claim_id" TEXT PRIMARY KEY NOT NULL,
    "fingerprint_contract" TEXT NOT NULL,
    "exact_fingerprint" TEXT NOT NULL,
    "logical_fact_id" TEXT NOT NULL,
    "equivalence_status" TEXT NOT NULL,
    "equivalence_basis" TEXT NOT NULL,
    "edition_row_id" TEXT NOT NULL,
    "continuity_id" TEXT NOT NULL,
    "applicability_scope_digest" TEXT NOT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("fingerprint_contract", "exact_fingerprint"),
    FOREIGN KEY ("claim_id") REFERENCES "reference_claims" ("claim_id") ON DELETE CASCADE,
    FOREIGN KEY ("logical_fact_id") REFERENCES "reference_logical_facts" ("logical_fact_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    FOREIGN KEY ("continuity_id") REFERENCES "reference_continuities" ("continuity_id") ON DELETE RESTRICT,
    CONSTRAINT chk_reference_fact_identity_hashes CHECK ((length(exact_fingerprint) = 64 AND exact_fingerprint NOT GLOB '*[^0-9a-f]*') AND (length(applicability_scope_digest) = 64 AND applicability_scope_digest NOT GLOB '*[^0-9a-f]*')),
    CONSTRAINT chk_reference_fact_equivalence CHECK (equivalence_status IN ('exact', 'verified_equivalent', 'unresolved'))
);
CREATE INDEX IF NOT EXISTS "idx_reference_fact_logical" ON "reference_fact_identities" ("logical_fact_id", "equivalence_status");

-- reference_overlay_rules
CREATE TABLE IF NOT EXISTS "reference_overlay_rules" (
    "overlay_rule_id" TEXT PRIMARY KEY NOT NULL,
    "work_id" TEXT NOT NULL,
    "edition_row_id" TEXT NOT NULL,
    "target_logical_fact_id" TEXT NULL,
    "target_entity_id" TEXT NULL,
    "target_node_id" TEXT NULL,
    "overlay_action" TEXT NOT NULL,
    "replacement_claim_id" TEXT NULL,
    "replacement_entity_id" TEXT NULL,
    "replacement_node_id" TEXT NULL,
    "rule_status" TEXT NOT NULL DEFAULT 'active',
    "reason_text" TEXT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("work_id") REFERENCES "reference_works" ("work_id") ON DELETE RESTRICT,
    FOREIGN KEY ("edition_row_id") REFERENCES "reference_work_editions" ("edition_row_id") ON DELETE RESTRICT,
    FOREIGN KEY ("target_logical_fact_id") REFERENCES "reference_logical_facts" ("logical_fact_id") ON DELETE RESTRICT,
    FOREIGN KEY ("target_entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE RESTRICT,
    FOREIGN KEY ("target_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE RESTRICT,
    FOREIGN KEY ("replacement_claim_id") REFERENCES "reference_claims" ("claim_id") ON DELETE SET NULL,
    FOREIGN KEY ("replacement_entity_id") REFERENCES "reference_entities" ("entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("replacement_node_id") REFERENCES "reference_timeline_nodes" ("node_id") ON DELETE SET NULL,
    CONSTRAINT chk_reference_overlay_action CHECK (overlay_action IN ('supplement', 'override', 'suppress_for_retrieval', 'conflict')),
    CONSTRAINT chk_reference_overlay_status CHECK (rule_status IN ('active', 'inactive', 'dormant')),
    CONSTRAINT chk_reference_overlay_target CHECK (
        ((target_logical_fact_id IS NOT NULL) + (target_entity_id IS NOT NULL) + (target_node_id IS NOT NULL) = 1) OR
        (rule_status = 'dormant' AND target_logical_fact_id IS NULL AND target_entity_id IS NULL AND target_node_id IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS "idx_reference_overlay_scope" ON "reference_overlay_rules" ("work_id", "edition_row_id", "rule_status", "overlay_action");

-- source_discovery_jobs
CREATE TABLE IF NOT EXISTS "source_discovery_jobs" (
    "job_id" TEXT PRIMARY KEY NOT NULL,
    "contract_version" TEXT NOT NULL DEFAULT 'source-discovery-pipeline.v1',
    "work_query" TEXT NOT NULL,
    "original_title" TEXT NOT NULL DEFAULT '',
    "language_code" TEXT NOT NULL DEFAULT '',
    "edition_hint" TEXT NOT NULL DEFAULT '',
    "job_state" TEXT NOT NULL,
    "request_json" TEXT NOT NULL,
    "result_json" TEXT NOT NULL,
    "coverage_report_json" TEXT NOT NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    CONSTRAINT chk_source_discovery_state CHECK (job_state IN (
        'created','scope_ready','discovering','fetching','extracting','reconciling',
        'coverage_review','ready_for_admission','awaiting_exception_review',
        'insufficient_source_coverage','blocked_by_access_policy','failed','cancelled'
    ))
);
CREATE INDEX IF NOT EXISTS "idx_source_discovery_state" ON "source_discovery_jobs" ("job_state", "updated_at");

-- entity_identities
CREATE TABLE IF NOT EXISTS "entity_identities" (
    "stable_entity_id" TEXT PRIMARY KEY NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "identity_namespace" TEXT NOT NULL,
    "entity_kind" TEXT NOT NULL,
    "canonical_label" TEXT NOT NULL,
    "lifecycle_state" TEXT NOT NULL DEFAULT 'active',
    "review_state" TEXT NOT NULL DEFAULT 'needs_review',
    "presence_authority" TEXT NOT NULL DEFAULT 'unverified',
    "occurrence_authority" TEXT NOT NULL DEFAULT 'none',
    "source_contract" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "source_logical_turn_id" TEXT NULL,
    "source_message_id" TEXT NULL,
    "source_generation_id" TEXT NULL,
    "source_content_hash" TEXT NOT NULL,
    "source_turn" INTEGER NOT NULL,
    "source_index" INTEGER NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "mapping_revision" INTEGER NOT NULL DEFAULT 1,
    "first_seen_turn" INTEGER NOT NULL,
    "last_seen_turn" INTEGER NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "idempotency_key")
);
CREATE INDEX IF NOT EXISTS "idx_entity_identity_session" ON "entity_identities" ("chat_session_id", "identity_namespace", "lifecycle_state");
CREATE INDEX IF NOT EXISTS "idx_entity_identity_source" ON "entity_identities" ("chat_session_id", "source_revision", "source_turn");
CREATE INDEX IF NOT EXISTS "idx_entity_identity_review" ON "entity_identities" ("chat_session_id", "review_state", "updated_at");

-- entity_identity_surfaces
CREATE TABLE IF NOT EXISTS "entity_identity_surfaces" (
    "surface_id" TEXT PRIMARY KEY NOT NULL,
    "stable_entity_id" TEXT NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "identity_namespace" TEXT NOT NULL,
    "surface_kind" TEXT NOT NULL,
    "surface_text" TEXT NOT NULL,
    "normalized_surface" TEXT NOT NULL,
    "surface_scope" TEXT NOT NULL DEFAULT 'source_turn',
    "valid_from_turn" INTEGER NOT NULL,
    "valid_to_turn" INTEGER NULL,
    "source_contract" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "source_turn" INTEGER NOT NULL,
    "source_span_start" INTEGER NULL,
    "source_span_end" INTEGER NULL,
    "evidence_excerpt" TEXT NULL,
    "review_state" TEXT NOT NULL DEFAULT 'needs_review',
    "idempotency_key" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "idempotency_key"),
    FOREIGN KEY ("stable_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS "idx_entity_surface_lookup" ON "entity_identity_surfaces" ("chat_session_id", "identity_namespace", "normalized_surface");
CREATE INDEX IF NOT EXISTS "idx_entity_surface_identity" ON "entity_identity_surfaces" ("stable_entity_id", "valid_from_turn", "valid_to_turn");

-- entity_identity_links
CREATE TABLE IF NOT EXISTS "entity_identity_links" (
    "link_id" TEXT PRIMARY KEY NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "source_entity_id" TEXT NOT NULL,
    "target_entity_id" TEXT NOT NULL,
    "link_kind" TEXT NOT NULL,
    "link_state" TEXT NOT NULL DEFAULT 'needs_review',
    "evidence_json" TEXT NOT NULL,
    "mapping_revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "source_entity_id", "target_entity_id", "link_kind"),
    FOREIGN KEY ("source_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE RESTRICT,
    FOREIGN KEY ("target_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS "idx_entity_identity_link_review" ON "entity_identity_links" ("chat_session_id", "link_state", "updated_at");

-- entity_identity_artifact_bindings
CREATE TABLE IF NOT EXISTS "entity_identity_artifact_bindings" (
    "binding_id" TEXT PRIMARY KEY NOT NULL,
    "stable_entity_id" TEXT NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "artifact_kind" TEXT NOT NULL,
    "artifact_role" TEXT NOT NULL,
    "artifact_ordinal" INTEGER NOT NULL,
    "surface_text" TEXT NOT NULL,
    "review_state" TEXT NOT NULL DEFAULT 'needs_review',
    "source_contract" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "source_turn" INTEGER NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "idempotency_key"),
    FOREIGN KEY ("stable_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS "idx_entity_artifact_binding_lookup" ON "entity_identity_artifact_bindings" ("chat_session_id", "source_turn", "artifact_kind", "artifact_ordinal");
CREATE INDEX IF NOT EXISTS "idx_entity_artifact_binding_identity" ON "entity_identity_artifact_bindings" ("stable_entity_id", "source_turn");

-- speaker_attributions
CREATE TABLE IF NOT EXISTS "speaker_attributions" (
    "attribution_id" TEXT PRIMARY KEY NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "speaker_entity_id" TEXT NOT NULL,
    "identity_namespace" TEXT NOT NULL,
    "source_role" TEXT NOT NULL,
    "attribution_kind" TEXT NOT NULL,
    "attribution_state" TEXT NOT NULL,
    "review_state" TEXT NOT NULL,
    "confidence" REAL NOT NULL DEFAULT 0,
    "source_contract" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "source_logical_turn_id" TEXT NULL,
    "source_message_id" TEXT NULL,
    "source_generation_id" TEXT NULL,
    "source_content_hash" TEXT NOT NULL,
    "source_turn" INTEGER NOT NULL,
    "source_span_start" INTEGER NOT NULL,
    "source_span_end" INTEGER NOT NULL,
    "evidence_excerpt" TEXT NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("chat_session_id", "idempotency_key"),
    FOREIGN KEY ("speaker_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS "idx_speaker_attribution_source" ON "speaker_attributions" ("chat_session_id", "source_revision", "source_turn");
CREATE INDEX IF NOT EXISTS "idx_speaker_attribution_review" ON "speaker_attributions" ("chat_session_id", "review_state", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_speaker_attribution_entity" ON "speaker_attributions" ("speaker_entity_id", "source_turn");

-- precise_memory_units
CREATE TABLE IF NOT EXISTS "precise_memory_units" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "unit_id" TEXT NOT NULL,
    "contract_version" TEXT NOT NULL DEFAULT 'precise_memory_unit.v1',
    "chat_session_id" TEXT NOT NULL,
    "source_turn_start" INTEGER NOT NULL,
    "source_turn_end" INTEGER NOT NULL,
    "source_contract" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "source_logical_turn_id" TEXT NULL,
    "source_message_id" TEXT NULL,
    "source_generation_id" TEXT NULL,
    "source_content_hash" TEXT NOT NULL,
    "source_role" TEXT NOT NULL,
    "source_span_start" INTEGER NOT NULL,
    "source_span_end" INTEGER NOT NULL,
    "evidence_excerpt" TEXT NOT NULL,
    "evidence_hash" TEXT NOT NULL,
    "root_evidence_id" INTEGER NULL,
    "direct_evidence_ids_json" TEXT NOT NULL,
    "memory_kind" TEXT NOT NULL,
    "memory_subtype" TEXT NULL,
    "payload_json" TEXT NOT NULL,
    "actor_entity_id" TEXT NULL,
    "subject_entity_id" TEXT NULL,
    "affected_entity_id" TEXT NULL,
    "location_entity_id" TEXT NULL,
    "object_entity_id" TEXT NULL,
    "relationship_key" TEXT NULL,
    "truth_scope" TEXT NOT NULL,
    "epistemic_mode" TEXT NOT NULL,
    "authority_class" TEXT NOT NULL,
    "admission_state" TEXT NOT NULL,
    "review_state" TEXT NOT NULL,
    "visibility" TEXT NOT NULL,
    "knowledge_holder_entity_id" TEXT NULL,
    "reveal_condition" TEXT NULL,
    "confidence" REAL NOT NULL DEFAULT 0,
    "idempotency_key" TEXT NOT NULL,
    "lifecycle_state" TEXT NOT NULL DEFAULT 'active',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("unit_id"),
    UNIQUE ("chat_session_id", "idempotency_key"),
    FOREIGN KEY ("root_evidence_id") REFERENCES "direct_evidence_records" ("id") ON DELETE SET NULL,
    FOREIGN KEY ("actor_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("subject_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("affected_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("location_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("object_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE SET NULL,
    FOREIGN KEY ("knowledge_holder_entity_id") REFERENCES "entity_identities" ("stable_entity_id") ON DELETE SET NULL,
    CONSTRAINT chk_precise_memory_span CHECK (source_span_start >= 0 AND source_span_end > source_span_start),
    CONSTRAINT chk_precise_memory_turn_range CHECK (source_turn_start > 0 AND source_turn_end >= source_turn_start),
    CONSTRAINT chk_precise_memory_kind
    CHECK (memory_kind IN ('event', 'state', 'utterance', 'observation', 'boundary', 'profile'))
);
CREATE INDEX IF NOT EXISTS "idx_precise_memory_source" ON "precise_memory_units" ("chat_session_id", "source_revision", "source_turn_start");
CREATE INDEX IF NOT EXISTS "idx_precise_memory_kind" ON "precise_memory_units" ("chat_session_id", "memory_kind", "lifecycle_state", "source_turn_start");
CREATE INDEX IF NOT EXISTS "idx_precise_memory_review" ON "precise_memory_units" ("chat_session_id", "admission_state", "review_state", "updated_at");
CREATE INDEX IF NOT EXISTS "idx_precise_memory_root_evidence" ON "precise_memory_units" ("root_evidence_id");

-- memory_source_revisions
CREATE TABLE IF NOT EXISTS "memory_source_revisions" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "contract_version" TEXT NOT NULL DEFAULT 'memory_source_revision.v1',
    "source_revision" TEXT NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "logical_turn_id" TEXT NOT NULL,
    "turn_index" INTEGER NOT NULL,
    "source_message_id" TEXT NULL,
    "source_generation_id" TEXT NULL,
    "branch_id" TEXT NULL,
    "branch_state" TEXT NOT NULL DEFAULT 'not_exposed',
    "raw_user_content" TEXT NOT NULL,
    "raw_assistant_content" TEXT NOT NULL,
    "combined_content_hash" TEXT NOT NULL,
    "user_observed_content_hash" TEXT NULL,
    "assistant_observed_content_hash" TEXT NULL,
    "hash_algorithm" TEXT NOT NULL,
    "host_observed_at_ms" INTEGER NOT NULL,
    "lifecycle_state" TEXT NOT NULL DEFAULT 'active',
    "derived_admission_state" TEXT NOT NULL DEFAULT 'pending',
    "derived_admission_version" TEXT NOT NULL DEFAULT '',
    "derived_extractor_version" TEXT NOT NULL DEFAULT '',
    "derived_index_version" TEXT NOT NULL DEFAULT '',
    "derived_result_hash" TEXT NULL,
    "derived_result_json" TEXT NULL,
    "derived_admitted_at" TEXT NULL,
    "critic_input_snapshot_json" TEXT NULL,
    "critic_input_snapshot_hash" TEXT NULL,
    "active_logical_turn_slot" TEXT GENERATED ALWAYS AS (CASE WHEN lifecycle_state = 'active' THEN logical_turn_id ELSE NULL END) STORED,
    "superseded_by_revision" TEXT NULL,
    "invalidation_reason" TEXT NULL,
    "invalidated_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("source_revision"),
    UNIQUE ("chat_session_id", "active_logical_turn_slot"),
    CONSTRAINT chk_memory_source_lifecycle CHECK (lifecycle_state IN ('active', 'superseded', 'invalidated', 'deleted')),
    CONSTRAINT chk_memory_source_admission CHECK (derived_admission_state IN ('pending', 'committed')),
    CONSTRAINT chk_memory_source_turn CHECK (turn_index > 0),
    CONSTRAINT chk_memory_source_branch_state CHECK (branch_state IN ('observed', 'not_exposed'))
);
CREATE INDEX IF NOT EXISTS "idx_memory_source_logical_turn" ON "memory_source_revisions" ("chat_session_id", "logical_turn_id", "lifecycle_state", "host_observed_at_ms");
CREATE INDEX IF NOT EXISTS "idx_memory_source_turn" ON "memory_source_revisions" ("chat_session_id", "turn_index", "lifecycle_state");
CREATE INDEX IF NOT EXISTS "idx_memory_source_generation" ON "memory_source_revisions" ("chat_session_id", "source_generation_id");

-- memory_derivation_dependencies
CREATE TABLE IF NOT EXISTS "memory_derivation_dependencies" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "contract_version" TEXT NOT NULL DEFAULT 'memory_derivation_dependency.v1',
    "chat_session_id" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "root_source_pointer" TEXT NOT NULL,
    "child_artifact_type" TEXT NOT NULL,
    "child_artifact_id" TEXT NOT NULL,
    "parent_artifact_type" TEXT NOT NULL,
    "parent_artifact_id" TEXT NOT NULL,
    "derivation_version" TEXT NOT NULL,
    "extractor_version" TEXT NOT NULL,
    "index_version" TEXT NOT NULL,
    "lifecycle_state" TEXT NOT NULL DEFAULT 'active',
    "invalidated_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("source_revision", "child_artifact_type", "child_artifact_id", "parent_artifact_type", "parent_artifact_id", "derivation_version", "extractor_version", "index_version"),
    FOREIGN KEY ("source_revision") REFERENCES "memory_source_revisions" ("source_revision") ON DELETE RESTRICT,
    CONSTRAINT chk_memory_derivation_lifecycle CHECK (lifecycle_state IN ('active', 'invalidated', 'deleted'))
);
CREATE INDEX IF NOT EXISTS "idx_memory_derivation_child" ON "memory_derivation_dependencies" ("chat_session_id", "child_artifact_type", "child_artifact_id", "lifecycle_state");
CREATE INDEX IF NOT EXISTS "idx_memory_derivation_parent" ON "memory_derivation_dependencies" ("chat_session_id", "parent_artifact_type", "parent_artifact_id", "lifecycle_state");
CREATE INDEX IF NOT EXISTS "idx_memory_derivation_source" ON "memory_derivation_dependencies" ("chat_session_id", "source_revision", "lifecycle_state");

-- memory_reprocessing_jobs
CREATE TABLE IF NOT EXISTS "memory_reprocessing_jobs" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "contract_version" TEXT NOT NULL DEFAULT 'memory_reprocessing_job.v1',
    "idempotency_key" TEXT NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "source_contract" TEXT NOT NULL,
    "derivation_version" TEXT NOT NULL,
    "extractor_version" TEXT NOT NULL,
    "index_version" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "retry_after" TEXT NULL,
    "lease_owner" TEXT NULL,
    "lease_until" TEXT NULL,
    "last_error" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("idempotency_key"),
    FOREIGN KEY ("source_revision") REFERENCES "memory_source_revisions" ("source_revision") ON DELETE RESTRICT,
    CONSTRAINT chk_memory_reprocessing_status CHECK (status IN ('pending', 'leased', 'retryable', 'permanent', 'completed', 'stale_rejected'))
);
CREATE INDEX IF NOT EXISTS "idx_memory_reprocessing_claim" ON "memory_reprocessing_jobs" ("status", "retry_after", "lease_until", "created_at");
CREATE INDEX IF NOT EXISTS "idx_memory_reprocessing_source" ON "memory_reprocessing_jobs" ("chat_session_id", "source_revision", "status");

-- memory_vector_outbox
CREATE TABLE IF NOT EXISTS "memory_vector_outbox" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "contract_version" TEXT NOT NULL DEFAULT 'memory_vector_outbox.v1',
    "operation_key" TEXT NOT NULL,
    "operation" TEXT NOT NULL,
    "chat_session_id" TEXT NOT NULL,
    "source_revision" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "document_json" TEXT NULL,
    "embedding_ready" INTEGER NOT NULL DEFAULT 0,
    "required_source_state" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "retry_after" TEXT NULL,
    "lease_owner" TEXT NULL,
    "lease_until" TEXT NULL,
    "last_error" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("operation_key"),
    FOREIGN KEY ("source_revision") REFERENCES "memory_source_revisions" ("source_revision") ON DELETE RESTRICT,
    CONSTRAINT chk_memory_vector_operation CHECK (operation IN ('delete', 'upsert')),
    CONSTRAINT chk_memory_vector_required_source CHECK (required_source_state IN ('active', 'inactive')),
    CONSTRAINT chk_memory_vector_status CHECK (status IN ('pending', 'leased', 'retryable', 'permanent', 'completed', 'stale_rejected', 'needs_embedding'))
);
CREATE INDEX IF NOT EXISTS "idx_memory_vector_claim" ON "memory_vector_outbox" ("status", "embedding_ready", "retry_after", "lease_until", "created_at");
CREATE INDEX IF NOT EXISTS "idx_memory_vector_source" ON "memory_vector_outbox" ("chat_session_id", "source_revision", "status");
CREATE INDEX IF NOT EXISTS "idx_memory_vector_document" ON "memory_vector_outbox" ("document_id", "operation", "status");

-- session_route_bindings
CREATE TABLE IF NOT EXISTS "session_route_bindings" (
    "binding_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "contract_version" TEXT NOT NULL,
    "stable_character_id" TEXT NOT NULL,
    "host_chat_id" TEXT NOT NULL,
    "canonical_session_id" TEXT NOT NULL,
    "binding_state" TEXT NOT NULL DEFAULT 'active',
    "binding_reason" TEXT NOT NULL,
    "redirected_from_session_id" TEXT NULL,
    "redirect_migration_id" INTEGER NULL,
    "revision" INTEGER NOT NULL DEFAULT 1,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("stable_character_id", "host_chat_id"),
    FOREIGN KEY ("redirect_migration_id") REFERENCES "session_migrations" ("id") ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS "idx_session_route_canonical" ON "session_route_bindings" ("canonical_session_id", "binding_state");
CREATE INDEX IF NOT EXISTS "idx_session_route_redirect" ON "session_route_bindings" ("redirect_migration_id");

-- session_migration_artifact_parity
CREATE TABLE IF NOT EXISTS "session_migration_artifact_parity" (
    "parity_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "migration_id" INTEGER NOT NULL,
    "manifest_version" TEXT NOT NULL,
    "table_name" TEXT NOT NULL,
    "parent_table_name" TEXT NULL,
    "session_column_name" TEXT NULL,
    "migration_policy" TEXT NOT NULL,
    "source_row_count" INTEGER NULL,
    "source_content_hash" TEXT NULL,
    "target_row_count" INTEGER NULL,
    "target_content_hash" TEXT NULL,
    "row_map_expected_count" INTEGER NULL,
    "row_map_verified_count" INTEGER NULL,
    "fk_expected_count" INTEGER NULL,
    "fk_verified_count" INTEGER NULL,
    "vector_expected_count" INTEGER NULL,
    "vector_expected_id_hash" TEXT NULL,
    "vector_actual_count" INTEGER NULL,
    "vector_actual_id_hash" TEXT NULL,
    "parity_state" TEXT NOT NULL DEFAULT 'unverified',
    "blocker_code" TEXT NULL,
    "verified_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("migration_id", "manifest_version", "table_name"),
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_parity_state" ON "session_migration_artifact_parity" ("migration_id", "parity_state");

-- session_migration_artifact_row_map
CREATE TABLE IF NOT EXISTS "session_migration_artifact_row_map" (
    "migration_id" INTEGER NOT NULL,
    "table_name" TEXT NOT NULL,
    "key_column_name" TEXT NOT NULL,
    "source_key" TEXT NOT NULL,
    "target_key" TEXT NOT NULL,
    "row_status" TEXT NOT NULL DEFAULT 'copied',
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    PRIMARY KEY ("migration_id", "table_name", "key_column_name", "source_key"),
    UNIQUE ("migration_id", "table_name", "key_column_name", "target_key"),
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_artifact_row_status" ON "session_migration_artifact_row_map" ("migration_id", "row_status");

-- session_migration_vector_expected_ids
CREATE TABLE IF NOT EXISTS "session_migration_vector_expected_ids" (
    "migration_id" INTEGER NOT NULL,
    "document_id" TEXT NOT NULL,
    "source_table" TEXT NOT NULL,
    "source_row_id" TEXT NOT NULL,
    "observed" INTEGER NOT NULL DEFAULT 0,
    "observed_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    PRIMARY KEY ("migration_id", "document_id"),
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_vector_observed" ON "session_migration_vector_expected_ids" ("migration_id", "observed");

-- session_migration_saga_steps
CREATE TABLE IF NOT EXISTS "session_migration_saga_steps" (
    "step_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "migration_id" INTEGER NOT NULL,
    "phase" TEXT NOT NULL,
    "phase_state" TEXT NOT NULL DEFAULT 'pending',
    "attempt_count" INTEGER NOT NULL DEFAULT 0,
    "request_hash" TEXT NULL,
    "result_json" TEXT NULL,
    "last_error" TEXT NULL,
    "started_at" TEXT NULL,
    "completed_at" TEXT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    UNIQUE ("migration_id", "phase"),
    FOREIGN KEY ("migration_id") REFERENCES "session_migrations" ("id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_session_migration_saga_state" ON "session_migration_saga_steps" ("phase_state", "updated_at");

-- lorebook_reference_session_locks
CREATE TABLE IF NOT EXISTS "lorebook_reference_session_locks" (
    "chat_session_id" TEXT NOT NULL PRIMARY KEY,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);

-- lorebook_reference_scopes
CREATE TABLE IF NOT EXISTS "lorebook_reference_scopes" (
    "scope_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "chat_session_id" TEXT NOT NULL,
    "character_index" INTEGER NULL,
    "chat_index" INTEGER NULL,
    "enabled_modules_json" TEXT NOT NULL,
    "scope_identity_json" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_lorebook_scope_session" ON "lorebook_reference_scopes" ("chat_session_id");
CREATE INDEX IF NOT EXISTS "idx_lorebook_scope_host" ON "lorebook_reference_scopes" ("chat_session_id", "character_index", "chat_index");

-- lorebook_reference_snapshots
CREATE TABLE IF NOT EXISTS "lorebook_reference_snapshots" (
    "snapshot_id" TEXT NOT NULL PRIMARY KEY,
    "scope_id" INTEGER NOT NULL,
    "contract_version" TEXT NOT NULL,
    "consent_state" TEXT NOT NULL,
    "observation_state" TEXT NOT NULL,
    "complete_snapshot" INTEGER NOT NULL DEFAULT 0,
    "entry_count" INTEGER NOT NULL DEFAULT 0,
    "provenance_json" TEXT NOT NULL,
    "observed_at" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("scope_id") REFERENCES "lorebook_reference_scopes" ("scope_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_lorebook_snapshot_scope" ON "lorebook_reference_snapshots" ("scope_id", "created_at");

-- lorebook_reference_entries
CREATE TABLE IF NOT EXISTS "lorebook_reference_entries" (
    "entry_record_id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "scope_id" INTEGER NOT NULL,
    "snapshot_id" TEXT NOT NULL,
    "host_entry_id" TEXT NULL,
    "entry_ordinal" INTEGER NOT NULL,
    "source_kind" TEXT NOT NULL DEFAULT 'current_host_aggregate',
    "source_identity" TEXT NULL,
    "entry_key" TEXT NOT NULL,
    "second_key" TEXT NOT NULL,
    "entry_comment" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    "normalized_search_text" TEXT NOT NULL,
    "entry_mode" TEXT NULL,
    "always_active" INTEGER NULL,
    "selective" INTEGER NULL,
    "use_regex" INTEGER NULL,
    "insert_order" INTEGER NULL,
    "activation_percent" REAL NULL,
    "book_version" INTEGER NULL,
    "folder" TEXT NULL,
    "extensions_json" TEXT NOT NULL,
    "content_hash" TEXT NULL,
    "lifecycle_state" TEXT NOT NULL DEFAULT 'catalog_current',
    "is_current" INTEGER NOT NULL DEFAULT 1,
    "first_seen_at" TEXT NOT NULL,
    "last_seen_at" TEXT NOT NULL,
    "created_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    "updated_at" TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')) NOT NULL,
    FOREIGN KEY ("scope_id") REFERENCES "lorebook_reference_scopes" ("scope_id") ON DELETE CASCADE,
    FOREIGN KEY ("snapshot_id") REFERENCES "lorebook_reference_snapshots" ("snapshot_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_lorebook_entry_current" ON "lorebook_reference_entries" ("scope_id", "is_current", "lifecycle_state");
CREATE INDEX IF NOT EXISTS "idx_lorebook_entry_host_id" ON "lorebook_reference_entries" ("scope_id", "host_entry_id");
CREATE INDEX IF NOT EXISTS "idx_lorebook_entry_snapshot" ON "lorebook_reference_entries" ("snapshot_id");

