package store

import "strings"

// D1 administrative reset: child-first delete allowlist and the bounded,
// resumable chunk statements that drive it.
//
// MariaDB's ResetAll disables foreign keys and deletes the whole registry in one
// transaction. D1 enforces foreign keys and bounds a single modification, so the
// D1 reset walks the same child-first allowlist in committed chunks and
// checkpoints its cursor in "d1_reset_runs".
//
// Verified SQLite behaviour this file depends on:
//   - Parent-first deletion cascades into children through ON DELETE CASCADE, so
//     the allowlist order must stay child-first.
//   - Each committed chunk advances a rowid cursor, so an interrupted reset
//     resumes instead of restarting or double-deleting.
//   - DELETE preserves sqlite_sequence, matching MariaDB's DELETE-based reset,
//     so AUTOINCREMENT ids stay monotonic across a reset.

// d1ResetChunkSize bounds how many rows one committed reset chunk deletes. D1
// warns that a very large modification can exceed execution limits, so the
// reset never issues an unbounded DELETE.
const d1ResetChunkSize = 250

// d1ResetMaintenanceLeaseName is the single lease a reset must hold.
const d1ResetMaintenanceLeaseName = "admin_reset"

// d1ResetControlPlaneTables own reset state and must never be deleted by a
// reset. Excluding them keeps progress and resumability intact across the reset
// itself and across Container restarts.
var d1ResetControlPlaneTables = []string{
	"d1_maintenance_lease",
	"d1_reset_epoch",
	"d1_reset_runs",
}

// d1AdminResetTables is the D1 child-first delete allowlist. It must stay
// identical in membership and order to mariaAdminResetTables so the two
// providers cannot silently drift; TestD1ResetAllowlistMatchesMariaDB enforces
// that, and TestD1ResetAllowlistTablesExistInSchema checks every entry exists in
// the D1 canonical schema.
var d1AdminResetTables = []string{
	"lorebook_reference_entries",
	"lorebook_reference_snapshots",
	"lorebook_reference_scopes",
	"lorebook_reference_session_locks",
	"source_discovery_jobs",
	"session_reference_coverage_fields",
	"session_reference_coverage_snapshots",
	"session_reference_runtime",
	"session_reference_bindings",
	"reference_overlay_rules",
	"reference_item_evidence",
	"reference_item_origins",
	"reference_fact_identities",
	"reference_logical_facts",
	"reference_source_observations",
	"reference_work_titles",
	"canon_pack_installs",
	"reference_work_editions",
	"reference_claim_knowers",
	"reference_claims",
	"reference_entity_aliases",
	"reference_entities",
	"reference_timeline_nodes",
	"reference_documents",
	"reference_continuities",
	"reference_works",
	"persona_capsule_attachments",
	"persona_memory_entries",
	"persona_memory_capsules",
	"protagonist_entity_memories",
	"effective_input_logs",
	"chat_logs",
	"memory_vector_outbox",
	"memory_reprocessing_jobs",
	"memory_derivation_dependencies",
	"precise_memory_units",
	"memory_source_revisions",
	"memories",
	"direct_evidence_records",
	"kg_triples",
	"audit_logs",
	"critic_feedback",
	"consequence_records",
	"psychology_branches",
	"session_fork_lineage",
	"theme_offscreen_carries",
	"capture_verification_records",
	"status_effects",
	"status_change_events",
	"status_current_values",
	"status_schema_registry",
	"status_schema_proposals",
	"character_events",
	"trust_states",
	"storylines",
	"world_rules",
	"session_active_scopes",
	"character_states",
	"pending_threads",
	"active_states",
	"canonical_state_layers",
	"guidance_plan_states",
	"episode_summaries",
	"chapter_summaries",
	"arc_summaries",
	"saga_digests",
	"speaker_attributions",
	"entity_identity_artifact_bindings",
	"entity_identity_links",
	"entity_identity_surfaces",
	"entity_identities",
	"entities",
}

// d1AdminResetTableAllowlist returns a copy of the child-first allowlist so
// callers cannot mutate the registry.
func d1AdminResetTableAllowlist() []string {
	out := make([]string, len(d1AdminResetTables))
	copy(out, d1AdminResetTables)
	return out
}

// d1ResetReservedName reports whether a name belongs to a SQLite internal object
// rather than application data. Reset must never target these: sqlite_sequence
// holds AUTOINCREMENT counters and cannot be recreated by the reset, and
// sqlite_schema/sqlite_autoindex_* are schema objects.
func d1ResetReservedName(name string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(name)), "sqlite_")
}

// d1ResetChunkSelectSQL returns the cursor read for one reset chunk. rowid is
// used instead of a table-specific primary key so the same statement works for
// every allowlist table; the D1 canonical schema has no WITHOUT ROWID table.
func d1ResetChunkSelectSQL(table string) string {
	return "SELECT rowid FROM " + d1QuoteIdent(table) + " WHERE rowid > ? ORDER BY rowid LIMIT ?"
}

// d1ResetChunkDeleteSQL returns the bounded delete for one reset chunk. It
// re-selects the same bounded range so the delete and the cursor read agree even
// if the caller batches them together, and it never issues an unbounded DELETE.
func d1ResetChunkDeleteSQL(table string) string {
	quoted := d1QuoteIdent(table)
	return "DELETE FROM " + quoted + " WHERE rowid IN (SELECT rowid FROM " + quoted +
		" WHERE rowid > ? ORDER BY rowid LIMIT ?)"
}
