package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// d1SessionMigrationSeedChatLog writes one source turn.
func d1SessionMigrationSeedChatLog(t *testing.T, conn *sqliteD1Conn, sessionID string, turn int, role, content string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		sessionID, turn, role, content, "2026-03-04T05:06:07.000Z"); err != nil {
		t.Fatalf("seed chat log %s/%d: %v", sessionID, turn, err)
	}
}

// d1SessionMigrationSeedKGTriple writes one source triple. source_turn is
// positive because the kg_triple vector plan treats a zero context turn as
// unknown, and a triple with no turn publishes no context for recall to use.
func d1SessionMigrationSeedKGTriple(t *testing.T, conn *sqliteD1Conn, sessionID, subject, predicate, object string, turn int) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO kg_triples (chat_session_id, subject, predicate, object, source_turn, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		sessionID, subject, predicate, object, turn, "2026-03-04T05:06:07.000Z"); err != nil {
		t.Fatalf("seed kg triple %s: %v", sessionID, err)
	}
}

// d1SessionMigrationSeedMigration writes a migration ledger row directly, so a
// test can start from a status the store would otherwise have to reach through
// the whole copy.
func d1SessionMigrationSeedMigration(
	t *testing.T, conn *sqliteD1Conn, id int64, sourceID, targetID, mode, status string,
) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO session_migrations (id, source_session_id, target_session_id, mode, status)
		VALUES (?, ?, ?, ?, ?)`, id, sourceID, targetID, mode, status); err != nil {
		t.Fatalf("seed migration %d: %v", id, err)
	}
}

// d1SessionMigrationSeedSource populates a source session that exercises both a
// plain copy table and a vector-publishing one.
func d1SessionMigrationSeedSource(t *testing.T, conn *sqliteD1Conn, sessionID string) {
	t.Helper()
	d1SessionMigrationSeedChatLog(t, conn, sessionID, 1, "user", "first turn")
	d1SessionMigrationSeedChatLog(t, conn, sessionID, 2, "assistant", "second turn")
	d1SessionMigrationSeedKGTriple(t, conn, sessionID, "Mira", "carries", "a lantern", 3)
}

// TestD1SessionMigrationOccupancyCountsEveryDirectManifestTable pins the gate the
// copy refuses to pass. A target is refused on any direct table with a row, not
// only on the legacy counters, so a table the counters ignore must still appear.
func TestD1SessionMigrationOccupancyCountsEveryDirectManifestTable(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedSource(t, conn, "source-a")

	occupancy, err := st.InspectSessionMigrationOccupancy(ctx, "source-a")
	if err != nil {
		t.Fatalf("InspectSessionMigrationOccupancy: %v", err)
	}
	if occupancy.DirectTableCounts["chat_logs"] != 2 {
		t.Errorf("chat_logs count = %d, want 2", occupancy.DirectTableCounts["chat_logs"])
	}
	if occupancy.DirectTableCounts["kg_triples"] != 1 {
		t.Errorf("kg_triples count = %d, want 1", occupancy.DirectTableCounts["kg_triples"])
	}
	if _, ok := occupancy.DirectTableCounts["memory_reprocessing_jobs"]; !ok {
		t.Error("a direct manifest table must be reported even when it is empty")
	}
	if occupancy.TotalDirectRows < 3 {
		t.Errorf("total direct rows = %d, want at least 3", occupancy.TotalDirectRows)
	}
	if _, blocking := occupancy.BlockingTables["chat_logs"]; !blocking {
		t.Error("a session holding turns must be reported as blocking")
	}

	// A session that holds only the turn-zero assistant starter is replaceable,
	// and every other table is empty, so nothing may block it.
	d1SessionMigrationSeedChatLog(t, conn, "starter-only", 0, "assistant", "hello")
	replaceable, err := st.InspectSessionMigrationOccupancy(ctx, "starter-only")
	if err != nil {
		t.Fatalf("InspectSessionMigrationOccupancy starter: %v", err)
	}
	if !replaceable.ReplaceableStarterOnly || len(replaceable.BlockingTables) != 0 {
		t.Errorf("starter-only session = %+v, want replaceable with no blockers", replaceable)
	}

	empty, err := st.InspectSessionMigrationOccupancy(ctx, "   ")
	if err != nil {
		t.Fatalf("InspectSessionMigrationOccupancy blank: %v", err)
	}
	if empty.TotalDirectRows != 0 || empty.DirectTableCounts == nil || empty.BlockingTables == nil {
		t.Errorf("blank session = %+v, want an empty but non-nil occupancy", empty)
	}
}

// TestD1SessionMigrationCopyCommitsParityAndVectorLedger walks the whole write
// phase: the copy, the row maps, the per-table parity, the vector expected-ID
// ledger, and the status flip that makes all of it visible.
func TestD1SessionMigrationCopyCommitsParityAndVectorLedger(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedSource(t, conn, "source-a")

	result, err := st.CompleteSessionMigration(ctx, SessionMigrationCompleteRequest{
		SourceSessionID: "source-a", TargetSessionID: "target-a", Mode: SessionMigrationModeCopyKeepSource,
	})
	if err != nil {
		t.Fatalf("CompleteSessionMigration: %v", err)
	}
	if result.Status != "copied" {
		t.Fatalf("status = %q, want copied", result.Status)
	}
	if result.MigrationID <= 0 {
		t.Fatalf("migration id = %d, want the committed ledger id", result.MigrationID)
	}
	if result.Counts.ChatLogs != 2 || result.Counts.KGTriples != 1 {
		t.Errorf("counts = %+v, want 2 chat logs and 1 kg triple", result.Counts)
	}

	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'target-a'`); got != 2 {
		t.Errorf("target chat logs = %d, want 2", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM kg_triples WHERE chat_session_id = 'target-a'`); got != 1 {
		t.Errorf("target kg triples = %d, want 1", got)
	}
	// The source must be untouched: a copy is a copy, and the later source lock
	// and cleanup phases are what make a source removable.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'source-a'`); got != 2 {
		t.Errorf("source chat logs = %d, want the copy to leave the source alone", got)
	}

	// The numeric row map is what the routing baseline and the rollback delete
	// both key on, so a copy without it would be a copy nobody can undo.
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM session_migration_row_map WHERE migration_id = ? AND table_name = 'chat_logs'`,
		result.MigrationID); got != 2 {
		t.Errorf("chat_logs row-map rows = %d, want 2", got)
	}
	var rowMapTarget int64
	if err := conn.QueryRow(ctx, `
		SELECT target_row_id FROM session_migration_row_map
		WHERE migration_id = ? AND table_name = 'chat_logs' AND source_row_id = 1`,
		result.MigrationID).Scan(&rowMapTarget); err != nil {
		t.Fatalf("read row map: %v", err)
	}
	var mappedContent string
	if err := conn.QueryRow(ctx, `SELECT content FROM chat_logs WHERE id = ?`, rowMapTarget).Scan(&mappedContent); err != nil {
		t.Fatalf("read mapped row: %v", err)
	}
	if mappedContent != "first turn" {
		t.Errorf("row map points at content %q, want the row the same source row produced", mappedContent)
	}

	// Every manifest table must carry a proof row, or the resume and lock gates
	// would be comparing against a partial manifest.
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM session_migration_artifact_parity WHERE migration_id = ?`,
		result.MigrationID); got != len(SessionMigrationManifest()) {
		t.Errorf("parity rows = %d, want one per manifest entry (%d)", got, len(SessionMigrationManifest()))
	}
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM session_migration_saga_steps WHERE migration_id = ? AND phase = 'relational_copy' AND phase_state = 'completed'`,
		result.MigrationID); got != 1 {
		t.Errorf("relational_copy saga step completed = %d, want 1", got)
	}

	// The exact expected document set is the whole point of the vector ledger.
	expected, err := st.GetSessionMigrationVectorParityContext(ctx, result.MigrationID)
	if err != nil {
		t.Fatalf("GetSessionMigrationVectorParityContext: %v", err)
	}
	if expected.TargetSessionID != "target-a" {
		t.Errorf("parity target = %q, want target-a", expected.TargetSessionID)
	}
	if len(expected.ExpectedIDs) != 1 || !strings.HasPrefix(expected.ExpectedIDs[0], "kg_triple:target-a:") {
		t.Fatalf("expected ids = %v, want one kg_triple document for the target session", expected.ExpectedIDs)
	}

	docs, err := st.ListSessionMigrationVectorDocuments(ctx, result.MigrationID)
	if err != nil {
		t.Fatalf("ListSessionMigrationVectorDocuments: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("vector documents = %d, want 1", len(docs))
	}
	if docs[0].ID != expected.ExpectedIDs[0] {
		t.Errorf("document id = %q, want the ledger id %q", docs[0].ID, expected.ExpectedIDs[0])
	}
	if docs[0].DocumentText != "Mira\ncarries\na lantern" {
		t.Errorf("document text = %q, want the kg_triple text format", docs[0].DocumentText)
	}
	if docs[0].ContextTurnIndex != 3 || !docs[0].ContextTurnKnown {
		t.Errorf("context turn = %d (known=%t), want 3", docs[0].ContextTurnIndex, docs[0].ContextTurnKnown)
	}
	if docs[0].MigratedFromSessionID != "source-a" {
		t.Errorf("migrated from = %q, want source-a", docs[0].MigratedFromSessionID)
	}

	resumed, err := st.GetSessionMigrationResumeContext(ctx, SessionMigrationCompleteRequest{
		SourceSessionID: "source-a", TargetSessionID: "target-a", Mode: SessionMigrationModeCopyKeepSource,
	})
	if err != nil {
		t.Fatalf("GetSessionMigrationResumeContext: %v", err)
	}
	if resumed.MigrationID != result.MigrationID || resumed.Status != "copied" {
		t.Errorf("resume context = %+v, want the committed migration", resumed)
	}
}

// TestD1SessionMigrationSourceLockFenceIsExclusive pins the compare-and-swap that
// stops a live turn from writing into a session being drained.
//
// The second claim must not succeed. A fence that two migrations both believed
// they held would let one of them lock a source the other is still draining, and
// neither would ever notice.
func TestD1SessionMigrationSourceLockFenceIsExclusive(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedMigration(t, conn, 11, "source-a", "target-a",
		SessionMigrationModeCopyThenLockSource, "vector_reindexed")
	d1SessionMigrationSeedMigration(t, conn, 12, "source-a", "target-b",
		SessionMigrationModeCopyThenLockSource, "vector_reindexed")

	lock, err := st.PrepareSessionMigrationSourceLock(ctx, 11, "draining source-a")
	if err != nil {
		t.Fatalf("PrepareSessionMigrationSourceLock: %v", err)
	}
	if lock.MigrationID != 11 || !lock.Locked {
		t.Fatalf("lock = %+v, want migration 11 holding the source", lock)
	}
	// The fence is pending, not retired. A pending lock is one the router refuses
	// to follow, which is what keeps a half-verified migration from redirecting
	// traffic onto a target that has not been proven.
	if lock.LockStatus != "lock_pending_verification" {
		t.Errorf("lock status = %q, want lock_pending_verification", lock.LockStatus)
	}

	refusal, err := st.PrepareSessionMigrationSourceLock(ctx, 12, "racing migration")
	if err == nil {
		t.Fatal("a second migration must not be able to fence the same source")
	}
	if !strings.Contains(err.Error(), "source_session_locked_by_other_migration") {
		t.Errorf("second claim error = %v, want the exclusive-claim blocker", err)
	}
	if refusal != nil {
		t.Errorf("refused claim returned a lock: %+v", refusal)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_migration_locks WHERE source_session_id = 'source-a'`); got != 1 {
		t.Fatalf("lock rows = %d, want exactly 1 after a refused claim", got)
	}

	// Re-preparing the SAME migration is an idempotent replay, not a second
	// claim: an operator retrying the prepare step must not deadlock themselves.
	replay, err := st.PrepareSessionMigrationSourceLock(ctx, 11, "draining source-a")
	if err != nil {
		t.Fatalf("replayed PrepareSessionMigrationSourceLock: %v", err)
	}
	if replay.MigrationID != lock.MigrationID {
		t.Errorf("replayed lock migration = %d, want %d", replay.MigrationID, lock.MigrationID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_migration_locks WHERE source_session_id = 'source-a'`); got != 1 {
		t.Errorf("lock rows = %d after an idempotent replay, want 1", got)
	}

	stored, err := st.GetSessionMigrationSourceLock(ctx, "source-a")
	if err != nil {
		t.Fatalf("GetSessionMigrationSourceLock: %v", err)
	}
	if stored.MigrationID != 11 || stored.TargetSessionID != "target-a" {
		t.Errorf("stored lock = %+v, want the fenced migration", stored)
	}
	if _, err := st.GetSessionMigrationSourceLock(ctx, "  "); !errors.Is(err, ErrNotFound) {
		t.Errorf("blank source lock error = %v, want ErrNotFound", err)
	}

	// A promoted lock must be out of reach of the release path. Verification has
	// already retired the source at that point, and a late rollback of a failed
	// verification must not reopen it.
	if _, err := conn.Exec(ctx, `
		UPDATE session_migration_locks SET lock_status = 'migrated_away' WHERE migration_id = 11`); err != nil {
		t.Fatalf("promote lock: %v", err)
	}
	if err := st.ReleaseSessionMigrationSourceLockFence(ctx, 11, "late release"); err != nil {
		t.Fatalf("ReleaseSessionMigrationSourceLockFence on a promoted lock: %v", err)
	}
	promoted, err := st.GetSessionMigrationSourceLock(ctx, "source-a")
	if err != nil {
		t.Fatalf("GetSessionMigrationSourceLock after a refused release: %v", err)
	}
	if promoted.LockStatus != "migrated_away" || !promoted.Locked {
		t.Errorf("lock = %+v, want the promoted lock untouched", promoted)
	}
}

// TestD1SessionMigrationFenceReleaseReopensTheSource covers the other half of the
// two-phase fence: a verification that failed must leave the source usable again,
// otherwise a failed migration would strand the session for ever.
func TestD1SessionMigrationFenceReleaseReopensTheSource(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedMigration(t, conn, 21, "source-a", "target-a",
		SessionMigrationModeCopyThenLockSource, "vector_reindexed")

	if _, err := st.PrepareSessionMigrationSourceLock(ctx, 21, "draining source-a"); err != nil {
		t.Fatalf("PrepareSessionMigrationSourceLock: %v", err)
	}
	if err := st.ReleaseSessionMigrationSourceLockFence(ctx, 21, "vector verification failed"); err != nil {
		t.Fatalf("ReleaseSessionMigrationSourceLockFence: %v", err)
	}
	if _, err := st.GetSessionMigrationSourceLock(ctx, "source-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("released source still reports a lock: %v", err)
	}
	if got := d1Count(t, conn, `
		SELECT COUNT(*) FROM session_migration_locks
		WHERE migration_id = 21 AND lock_status = 'lock_verification_failed' AND unlocked_at IS NOT NULL`); got != 1 {
		t.Errorf("released lock rows = %d, want the fence recorded as released", got)
	}
	// Releasing again is a no-op, not a second release: the row is already out of
	// the pending state and must not be touched again.
	if err := st.ReleaseSessionMigrationSourceLockFence(ctx, 21, "again"); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if got := d1Count(t, conn, `
		SELECT COUNT(*) FROM session_migration_saga_steps
		WHERE migration_id = 21 AND phase = 'source_lock_fence' AND phase_state = 'failed'`); got != 1 {
		t.Errorf("source_lock_fence saga rows = %d, want 1", got)
	}
}

// TestD1SessionMigrationSourceLockRequiresTheFenceAndFullParity pins both halves
// of the promotion gate: without a claimed fence there is nothing to promote, and
// with a fence but no verified parity the promotion still must not happen.
func TestD1SessionMigrationSourceLockRequiresTheFenceAndFullParity(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedMigration(t, conn, 31, "source-a", "target-a",
		SessionMigrationModeCopyThenLockSource, "vector_reindexed")

	_, err := st.LockSessionMigrationSource(ctx, 31, "retiring source-a")
	if err == nil {
		t.Fatal("a source lock without a claimed fence must be refused")
	}
	if !strings.Contains(err.Error(), "source_lock_fence_required") {
		t.Errorf("error = %v, want the missing-fence blocker", err)
	}

	if _, err := st.PrepareSessionMigrationSourceLock(ctx, 31, "draining source-a"); err != nil {
		t.Fatalf("PrepareSessionMigrationSourceLock: %v", err)
	}
	_, err = st.LockSessionMigrationSource(ctx, 31, "retiring source-a")
	if err == nil {
		t.Fatal("a source lock with no verified parity must be refused")
	}
	if !strings.Contains(err.Error(), "source lock blocked") {
		t.Errorf("error = %v, want the durable parity refusal", err)
	}
	if stored, readErr := st.GetSessionMigrationSourceLock(ctx, "source-a"); readErr != nil {
		t.Fatalf("GetSessionMigrationSourceLock: %v", readErr)
	} else if stored.LockStatus != "lock_pending_verification" {
		t.Errorf("lock status = %q, want the fence left pending after a refused promotion", stored.LockStatus)
	}
}

// TestD1SessionMigrationVectorParityCountsComeFromTheLedger pins the exact-set
// comparison and the counts it writes back.
//
// An under-reported count is worse than an error: the source-lock gate consumes
// the recorded observed counts, so a missing vector that is not detected here
// would let a destructive phase run against an index that cannot find it.
func TestD1SessionMigrationVectorParityCountsComeFromTheLedger(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedSource(t, conn, "source-a")

	result, err := st.CompleteSessionMigration(ctx, SessionMigrationCompleteRequest{
		SourceSessionID: "source-a", TargetSessionID: "target-a", Mode: SessionMigrationModeCopyThenLockSource,
	})
	if err != nil {
		t.Fatalf("CompleteSessionMigration: %v", err)
	}
	context, err := st.GetSessionMigrationVectorParityContext(ctx, result.MigrationID)
	if err != nil {
		t.Fatalf("GetSessionMigrationVectorParityContext: %v", err)
	}
	if len(context.ExpectedIDs) == 0 {
		t.Fatal("the copy must publish an expected vector document set")
	}

	// A vector index that is missing a document must be reported, not absorbed.
	missing := context.ExpectedIDs[:len(context.ExpectedIDs)-1]
	short, err := st.VerifySessionMigrationVectorParity(ctx, result.MigrationID,
		SessionMigrationProofOperationSourceLock, missing)
	if err == nil {
		t.Fatal("a parity check against an incomplete index must fail")
	}
	if !strings.Contains(err.Error(), "current_vector_id_drift") {
		t.Errorf("error = %v, want the vector id drift blocker", err)
	}
	if len(short.MissingIDs) != len(context.ExpectedIDs)-len(missing) {
		t.Errorf("missing ids = %v, want every absent document reported", short.MissingIDs)
	}
	// A failed verification must not leave observations behind, because the
	// source-lock gate counts observed ledger rows rather than re-running the
	// comparison.
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM session_migration_vector_expected_ids WHERE migration_id = ? AND observed = 1`,
		result.MigrationID); got != 0 {
		t.Errorf("observed ledger rows = %d after a failed verification, want 0", got)
	}

	// An unexpected extra document is also drift, unless the operation allows it.
	extra := append(append([]string{}, context.ExpectedIDs...), "kg_triple:target-a:99999")
	if _, err := st.VerifySessionMigrationVectorParity(ctx, result.MigrationID,
		SessionMigrationProofOperationSourceLock, extra); err == nil {
		t.Fatal("an unexpected vector document must fail an exact parity check")
	}

	verified, err := st.VerifySessionMigrationVectorParity(ctx, result.MigrationID,
		SessionMigrationProofOperationSourceLock, context.ExpectedIDs)
	if err != nil {
		t.Fatalf("VerifySessionMigrationVectorParity: %v", err)
	}
	if !verified.Verified {
		t.Fatalf("parity result = %+v, want verified", verified)
	}
	if len(verified.MissingIDs) != 0 || len(verified.UnexpectedIDs) != 0 {
		t.Errorf("verified result still reports drift: %+v", verified)
	}

	// The recorded actual counts must be the ledger rows that were observed, and
	// the recorded actual hash must be the hash of exactly those ids.
	var observedRows, recordedCount int
	var recordedHash string
	if err := conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM session_migration_vector_expected_ids WHERE migration_id = ? AND observed = 1`,
		result.MigrationID).Scan(&observedRows); err != nil {
		t.Fatalf("count observed ledger rows: %v", err)
	}
	if observedRows != len(context.ExpectedIDs) {
		t.Errorf("observed ledger rows = %d, want %d", observedRows, len(context.ExpectedIDs))
	}
	if err := conn.QueryRow(ctx, `
		SELECT COALESCE(vector_actual_count, -1), COALESCE(vector_actual_id_hash, '')
		FROM session_migration_artifact_parity
		WHERE migration_id = ? AND table_name = 'kg_triples'`,
		result.MigrationID).Scan(&recordedCount, &recordedHash); err != nil {
		t.Fatalf("read vector actual parity: %v", err)
	}
	if recordedCount != observedRows {
		t.Errorf("recorded vector_actual_count = %d, want the observed ledger count %d", recordedCount, observedRows)
	}
	if recordedHash != sessionMigrationHashIDs(verified.ActualIDs) {
		t.Errorf("recorded vector_actual_id_hash = %q, want the hash of the observed ids %q",
			recordedHash, sessionMigrationHashIDs(verified.ActualIDs))
	}
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM session_migration_saga_steps
		 WHERE migration_id = ? AND phase = 'vector_exact_id_parity' AND phase_state = 'completed'`,
		result.MigrationID); got != 1 {
		t.Errorf("vector exact-id saga step = %d, want 1", got)
	}
}

// TestD1SessionMigrationVectorStatusUpdateDefaults pins the ledger write the
// reindexer performs, including the defaults that keep a caller that sends no
// status or no error payload from writing an unusable row.
func TestD1SessionMigrationVectorStatusUpdateDefaults(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedMigration(t, conn, 41, "source-a", "target-a",
		SessionMigrationModeCopyKeepSource, "copied")

	if err := st.UpdateSessionMigrationVectorStatus(ctx, 41, "", 3, ""); err != nil {
		t.Fatalf("UpdateSessionMigrationVectorStatus: %v", err)
	}
	var status string
	var count int
	var errorsJSON string
	if err := conn.QueryRow(ctx,
		`SELECT status, chroma_reindexed_count, errors_json FROM session_migrations WHERE id = 41`).
		Scan(&status, &count, &errorsJSON); err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if status != "vector_reindexed" {
		t.Errorf("status = %q, want the vector_reindexed default", status)
	}
	if count != 3 {
		t.Errorf("reindexed count = %d, want 3", count)
	}
	if errorsJSON != "[]" {
		t.Errorf("errors_json = %q, want the empty-array default", errorsJSON)
	}
	if err := st.UpdateSessionMigrationVectorStatus(ctx, 0, "vector_reindexed", 0, "[]"); !errors.Is(err, ErrNotFound) {
		t.Errorf("zero migration error = %v, want ErrNotFound", err)
	}
}

// TestD1SessionMigrationInterruptedCopyIsDiscardedAndRetried pins the interrupted
// sequence the recovery store exists for.
//
// A migration that died between the copy and its parity proof has copied target
// rows and no proof. A retry must not be blocked by its own leftovers, and it
// must not double the target.
func TestD1SessionMigrationInterruptedCopyIsDiscardedAndRetried(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedSource(t, conn, "source-a")

	// Reproduce an interruption: a copying migration whose target rows were
	// already written and whose row maps point at them.
	d1SessionMigrationSeedMigration(t, conn, 51, "source-a", "target-a",
		SessionMigrationModeCopyKeepSource, "copying")
	if _, err := conn.Exec(ctx, `
		INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at)
		VALUES ('target-a', 1, 'user', 'partial', '2026-03-04T05:06:07.000Z')`); err != nil {
		t.Fatalf("seed partial target row: %v", err)
	}
	var partialID int64
	if err := conn.QueryRow(ctx,
		`SELECT id FROM chat_logs WHERE chat_session_id = 'target-a' AND turn_index = 1`).Scan(&partialID); err != nil {
		t.Fatalf("read partial row id: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO session_migration_artifact_row_map
			(migration_id, table_name, key_column_name, source_key, target_key, row_status)
		VALUES (51, 'chat_logs', 'id', '1', ?, 'copied')`,
		strconv.FormatInt(partialID, 10)); err != nil {
		t.Fatalf("seed artifact row map: %v", err)
	}

	result, err := st.CompleteSessionMigration(ctx, SessionMigrationCompleteRequest{
		SourceSessionID: "source-a", TargetSessionID: "target-a", Mode: SessionMigrationModeCopyKeepSource,
	})
	if err != nil {
		t.Fatalf("retry after an interrupted copy: %v", err)
	}
	if result.MigrationID == 51 {
		t.Error("the interrupted migration must be reverted, not resumed into")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'target-a'`); got != 2 {
		t.Errorf("target chat logs = %d, want exactly the two copied turns", got)
	}
	var content string
	if err := conn.QueryRow(ctx,
		`SELECT content FROM chat_logs WHERE chat_session_id = 'target-a' AND turn_index = 1`).Scan(&content); err != nil {
		t.Fatalf("read copied turn: %v", err)
	}
	if content != "first turn" {
		t.Errorf("copied turn content = %q, want the source content, not the interrupted partial", content)
	}
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM session_migrations WHERE id = 51 AND status = 'rolled_back'`); got != 1 {
		t.Error("the interrupted migration must be left rolled back")
	}
}

// TestD1SessionMigrationRollbackIsIdempotentAndLedgerScoped pins the recovery
// contract: a rollback removes only what the migration copied, and running it
// again changes nothing.
func TestD1SessionMigrationRollbackIsIdempotentAndLedgerScoped(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedSource(t, conn, "source-a")

	result, err := st.CompleteSessionMigration(ctx, SessionMigrationCompleteRequest{
		SourceSessionID: "source-a", TargetSessionID: "target-a", Mode: SessionMigrationModeCopyKeepSource,
	})
	if err != nil {
		t.Fatalf("CompleteSessionMigration: %v", err)
	}
	// An operator row the migration did not create must survive the rollback.
	if _, err := conn.Exec(ctx, `
		INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at)
		VALUES ('target-a', 99, 'user', 'operator note', '2026-03-04T05:06:07.000Z')`); err != nil {
		t.Fatalf("seed operator row: %v", err)
	}

	rolled, err := st.RollbackSessionMigration(ctx, result.MigrationID, "operator cancelled")
	if err != nil {
		t.Fatalf("RollbackSessionMigration: %v", err)
	}
	if rolled.Status != "rolled_back" {
		t.Errorf("status = %q, want rolled_back", rolled.Status)
	}
	if rolled.Counts.ChatLogs != 2 {
		t.Errorf("deleted chat logs = %d, want 2", rolled.Counts.ChatLogs)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'target-a'`); got != 1 {
		t.Errorf("target chat logs after rollback = %d, want only the operator row", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM kg_triples WHERE chat_session_id = 'target-a'`); got != 0 {
		t.Errorf("target kg triples after rollback = %d, want 0", got)
	}
	// The source is the whole point of a rollback: it must be fully intact so the
	// chat can continue where it left off.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'source-a'`); got != 2 {
		t.Errorf("source chat logs after rollback = %d, want 2", got)
	}

	again, err := st.RollbackSessionMigration(ctx, result.MigrationID, "operator cancelled")
	if err != nil {
		t.Fatalf("second RollbackSessionMigration: %v", err)
	}
	if again.Status != "rolled_back" {
		t.Errorf("replayed status = %q, want rolled_back", again.Status)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'target-a'`); got != 1 {
		t.Errorf("replayed rollback changed the target: %d rows remain, want 1", got)
	}
	if _, err := st.RollbackSessionMigration(ctx, 0, "x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("zero migration rollback error = %v, want ErrNotFound", err)
	}
}

// TestD1SessionMigrationSourceCleanupPreviewIsFailClosed pins the preview the
// destructive phase is gated on. A migration that has not been locked must never
// look cleanable, whatever its counts are.
func TestD1SessionMigrationSourceCleanupPreviewIsFailClosed(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionMigrationSeedSource(t, conn, "source-a")
	d1SessionMigrationSeedMigration(t, conn, 61, "source-a", "target-a",
		SessionMigrationModeCopyThenLockSource, "copied")

	preview, err := st.PreviewSessionMigrationSourceCleanup(ctx, 61)
	if err != nil {
		t.Fatalf("PreviewSessionMigrationSourceCleanup: %v", err)
	}
	if preview.ReadyForCleanup {
		t.Fatal("a migration that is not source-locked must not be cleanable")
	}
	joined := strings.Join(preview.BlockedReasons, ",")
	if !strings.Contains(joined, "migration_status_not_source_locked") ||
		!strings.Contains(joined, "active_source_lock_not_found_for_migration") {
		t.Errorf("blocked reasons = %v, want the status and lock reasons", preview.BlockedReasons)
	}
	if preview.Counts.ChatLogs != 2 {
		t.Errorf("source counts = %+v, want the source inventory", preview.Counts)
	}

	if _, err := st.CleanupSessionMigrationSource(ctx, 61, "clean"); err == nil {
		t.Fatal("cleanup must be refused while the migration is not source-locked")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'source-a'`); got != 2 {
		t.Errorf("source chat logs = %d after a refused cleanup, want 2", got)
	}
}
