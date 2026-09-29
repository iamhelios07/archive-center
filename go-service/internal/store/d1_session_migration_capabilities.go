package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// D1 session migration capability: moving a whole canonical session between
// chat_session_ids, and the ledgers that prove the move was complete.
//
// What this family is
//
// A migration drains one session into another. The source is not deleted; it is
// locked, so a live turn already writing into it can neither finish into a
// half-migrated session nor resurrect retired rows. The target is filled from a
// manifest of every session-owned table, and every copied row is recorded in a
// row map so the move can be undone without touching rows the migration did not
// create. None of that is allowed to be approximate: a target that silently lost
// one table looks complete, and a rollback that deleted one row too many is
// unrecoverable.
//
// The shape of the D1 port
//
// The reference does the whole copy in one MariaDB transaction and learns each
// generated id from LastInsertId as it goes, feeding it straight into the row
// map, the deferred foreign keys, and the vector expected-ID ledger. D1 cannot
// reproduce that shape: D1Conn.Batch is the atomic boundary, it returns only an
// error, and nothing can read between two of its statements.
//
// So the copy is emitted as an ordered statement list in which each copy INSERT
// is immediately followed by the row-map rows that need the engine-assigned id,
// and those rows resolve the id with a batch-internal scalar subquery over the
// row the preceding statement just wrote. The subquery is scoped to the copy
// target through the same ancestor chain the read uses, and the copy loop walks
// each table in ascending primary-key order, so the newest row in that scope is
// always the row the previous statement created. MAX(id) + 1 is deliberately not
// used: a second Cloudflare Container inserting between the read and the write
// would make the ledger point at a row that does not exist, and a copy ledger
// that points at the wrong row is worse than no ledger.
//
// Where the batches break
//
// The reference transaction is split into phases, and the boundaries are load
// bearing rather than incidental:
//
//	phase 1 - the migration ledger row,
//	phase 2 - the opening saga steps, the starter replacement, the ordered copy,
//	          the row maps, the deferred foreign keys, and the copied public
//	          projection receipts,
//	phase 3 - the per-table parity rows, the vector expected-ID ledger, and the
//	          status flip to copied.
//
// Phase 3 is the commit point, for the same reason the second batch of the memory
// admission is. A crash between phase 2 and phase 3 leaves a migration row in
// status copying with a complete set of copied target rows and no parity proof.
// Nothing downstream can act on it, because every later phase requires status
// copied or better, and a retry disposes of it through exactly the same
// ledger-scoped rollback the recovery store uses before starting a fresh copy.
// That is what makes an interrupted migration resumable instead of a target that
// stays blocked for ever.
//
// Locking
//
// The reference takes SELECT ... FOR UPDATE on the migration row, on the active
// source lock, and on the derivation leases it refuses to lock through. D1 has
// no row locks, so each of those is restated as a guard inside the statement
// that would otherwise have been protected, and the affected-row count is what
// proves the claim. That count is the whole point: a second Prepare or Lock
// that finds the row already claimed writes zero rows and is reported as a
// blocker, so two Containers cannot both believe they own the fence.
//
// A migration that claimed a source lock and then stopped would leave the
// session permanently unusable, so the fence is two-phase by design.
// PrepareSessionMigrationSourceLock claims lock_pending_verification, vector and
// relational verification runs, and LockSessionMigrationSource promotes that
// same row to migrated_away. ReleaseSessionMigrationSourceLockFence undoes the
// claim when verification fails, and its guarded UPDATE can only touch a row
// still in the pending state, so a late rollback cannot release a lock that
// verification already promoted.

var _ SessionMigrationStore = (*d1Store)(nil)
var _ SessionMigrationRecoveryStore = (*d1Store)(nil)
var _ SessionMigrationSourceLockStore = (*d1Store)(nil)
var _ SessionMigrationSourceLockFenceStore = (*d1Store)(nil)
var _ SessionMigrationVectorStore = (*d1Store)(nil)
var _ SessionMigrationVectorParityStore = (*d1Store)(nil)

// d1SessionMigrationBatchChunk bounds one D1 batch.
//
// D1Conn.Batch is the only transaction boundary this provider has, and a real
// session copy emits several statements per copied row across roughly fifty
// manifest tables. Emitting all of them in one call would exceed what the binding
// accepts, so the list is emitted in order, in bounded chunks. Each chunk is
// atomic; the phase boundaries above are what decide what a crash between chunks
// means.
const d1SessionMigrationBatchChunk = 60

// d1SessionMigrationInClauseChunk bounds one IN list.
//
// A trailing comma in an IN list is a parse error rather than an empty set, so
// the placeholder string is always built from an explicit count, and an empty
// list becomes a predicate that is false rather than a malformed fragment.
//
// The bound is d1BoundParameterCeiling rather than a local number: the earlier
// value of 200 cleared the local SQLite harness, which allows far more bindings
// than D1, and then failed against D1 for a wide migration.
const d1SessionMigrationInClauseChunk = d1BoundParameterCeiling

// d1SessionMigrationRevertedStatuses is the set of statuses a rollback considers
// already finished. The resume lookup and the rollback result share it so the two
// cannot disagree about what a reverted migration is.
const d1SessionMigrationRevertedStatuses = "('rolled_back', 'rollback_partial')"

// ---------------------------------------------------------------------------
// statement builders
// ---------------------------------------------------------------------------

// d1SessionMigrationSagaStatement is the saga step upsert.
//
// MariaDB writes ON DUPLICATE KEY UPDATE against uq_session_migration_saga_phase;
// the SQLite form is the same upsert against the (migration_id, phase) unique
// key. The attempt counter increments on every call including the insert, so an
// interrupted phase leaves an honest attempt trail instead of looking like a
// phase that never ran.
func d1SessionMigrationSagaStatement(
	migrationID int64, phase, state, requestHash, resultJSON, lastError string,
) D1Statement {
	if strings.TrimSpace(resultJSON) == "" {
		resultJSON = "{}"
	}
	return D1Statement{
		SQL: `
		INSERT INTO session_migration_saga_steps (
			migration_id, phase, phase_state, attempt_count, request_hash,
			result_json, last_error, started_at, completed_at
		) VALUES (?, ?, ?, 1, ?, ?, ?, ` + d1NowExpression + `,
		          CASE WHEN ? = 'completed' THEN ` + d1NowExpression + ` ELSE NULL END)
		ON CONFLICT (migration_id, phase) DO UPDATE SET
			phase_state = excluded.phase_state,
			attempt_count = session_migration_saga_steps.attempt_count + 1,
			request_hash = excluded.request_hash,
			result_json = excluded.result_json,
			last_error = excluded.last_error,
			completed_at = excluded.completed_at,
			updated_at = ` + d1NowExpression,
		Args: []any{migrationID, phase, state, d1NullableString(requestHash),
			resultJSON, d1NullableString(lastError), state},
	}
}

// d1SessionMigrationKeyMapStatement is one artifact provenance row.
//
// targetExpr may be a literal or a batch-internal scalar subquery over the row the
// preceding copy statement wrote. Because a subquery is spliced into the SQL
// rather than bound, its own placeholders come BEFORE the trailing row_status
// placeholder, so targetArgs is spliced in at that position and row_status is
// bound last. Binding row_status before the subquery arguments would silently
// shift every argument and write the provenance of one row under another row's
// key.
//
// The (migration_id, table_name, key_column_name, source_key) primary key is what
// makes a replayed copy an idempotent no-op rather than a duplicate, so a
// non-identical collision surfaces as a constraint failure rather than as a
// silent overwrite.
func d1SessionMigrationKeyMapStatement(
	migrationID int64, table, column, source, targetExpr, rowStatus string, targetArgs []any,
) (D1Statement, error) {
	if rowStatus != "copied" && rowStatus != "alternate_key" {
		return D1Statement{}, fmt.Errorf("unsupported session migration row-map status %q", rowStatus)
	}
	args := make([]any, 0, 6+len(targetArgs))
	args = append(args, migrationID, table, column, source)
	args = append(args, targetArgs...)
	args = append(args, rowStatus)
	return D1Statement{
		SQL: `
		INSERT INTO session_migration_artifact_row_map (
			migration_id, table_name, key_column_name, source_key, target_key, row_status
		) VALUES (?, ?, ?, ?, (` + targetExpr + `), ?)
		ON CONFLICT (migration_id, table_name, key_column_name, source_key) DO NOTHING`,
		Args: args,
	}, nil
}

// d1SessionMigrationRowMapStatement is the numeric row provenance consumed by
// rollback, the routing baseline, and the cleanup count. It is emitted only for
// auto-increment tables, the only case where both ends are numbers and neither is
// a precomputed key. targetExpr carries the same spliced-subquery shape as the
// artifact row map, so its arguments land in the fourth slot.
func d1SessionMigrationRowMapStatement(
	migrationID int64, table string, sourceRowID int64, targetExpr string, targetArgs []any,
) D1Statement {
	args := make([]any, 0, 4+len(targetArgs))
	args = append(args, migrationID, table, sourceRowID)
	args = append(args, targetArgs...)
	return D1Statement{
		SQL: `
		INSERT INTO session_migration_row_map (
			migration_id, table_name, source_row_id, target_row_id, row_status
		) VALUES (?, ?, ?, (` + targetExpr + `), 'copied')
		ON CONFLICT (migration_id, table_name, source_row_id) DO NOTHING`,
		Args: args,
	}
}

// d1SessionMigrationPlaceholders renders n comma-separated bind markers.
func d1SessionMigrationPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// d1SessionMigrationAnyArgs converts a key list into bind arguments.
func d1SessionMigrationAnyArgs(values []string) []any {
	args := make([]any, 0, len(values))
	for _, value := range values {
		args = append(args, value)
	}
	return args
}

// d1SessionMigrationLiteralOrSubquery renders a target key for the row maps.
//
// A value that is itself a SELECT is the batch-internal subquery over the row the
// preceding copy statement wrote; anything else is a key literal. Detecting the
// subquery by shape rather than by a parallel flag keeps the two row-map
// builders from disagreeing about which form they are binding.
func d1SessionMigrationLiteralOrSubquery(target string) string {
	trimmed := strings.TrimSpace(target)
	if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT ") ||
		strings.HasPrefix(strings.ToUpper(trimmed), "(SELECT ") {
		return trimmed
	}
	return d1StringLiteral(target)
}

// d1SessionMigrationInClause returns a complete "col IN (...)" fragment with its
// bound values.
func d1SessionMigrationInClause(column string, values []string) (string, []any) {
	if len(values) == 0 {
		return "1 = 0", nil
	}
	args := make([]any, 0, len(values))
	for _, value := range values {
		args = append(args, value)
	}
	return column + " IN (" + d1SessionMigrationPlaceholders(len(values)) + ")", args
}

// d1SessionMigrationAncestors walks an indirect manifest entry up to its direct
// session-scoped root, nearest parent first.
//
// The copy needs the chain both to build the read JOINs and to scope the
// batch-internal id subquery. A chain that cannot be walked is a manifest defect,
// not a runtime condition, so it is reported rather than silently treated as
// direct.
func d1SessionMigrationAncestors(
	entry SessionMigrationManifestEntry,
) ([]SessionMigrationManifestEntry, []SessionMigrationExecutionPlan, error) {
	entries := []SessionMigrationManifestEntry{}
	plans := []SessionMigrationExecutionPlan{}
	current := entry
	for !current.Direct {
		parent, ok := sessionMigrationManifestEntryByTable(current.ParentTable)
		if !ok {
			return nil, nil, fmt.Errorf("session migration parent manifest entry %q not found", current.ParentTable)
		}
		plan, ok := SessionMigrationExecutionPlanFor(parent.Table)
		if !ok || len(plan.PrimaryKey) != 1 {
			return nil, nil, fmt.Errorf("session migration parent plan %q has an unsupported key", parent.Table)
		}
		entries = append(entries, parent)
		plans = append(plans, plan)
		current = parent
	}
	return entries, plans, nil
}

// d1SessionMigrationDerivedGuard is the memory_derivation_dependencies filter.
//
// Replacement keeps invalidated units but physically removes their evidence, and
// an explicitly invalidated edge to that absent parent has no operational target
// to copy. The same selection is used for the copy and for the parity readback,
// so the two can never see different row sets.
const d1SessionMigrationDerivedGuard = ` AND NOT (
		t0.lifecycle_state = 'invalidated'
		AND t0.child_artifact_type IN ('status_change_event', 'precise_memory_unit')
		AND t0.parent_artifact_type = 'direct_evidence'
		AND EXISTS (
			SELECT 1 FROM memory_source_revisions source_revision
			WHERE source_revision.chat_session_id = t0.chat_session_id
			  AND source_revision.source_revision = t0.source_revision
			  AND source_revision.lifecycle_state IN ('superseded', 'invalidated', 'deleted')
		)
		AND NOT EXISTS (
			SELECT 1 FROM direct_evidence_records parent_evidence
			WHERE parent_evidence.chat_session_id = t0.chat_session_id
			  AND CAST(parent_evidence.id AS TEXT) = t0.parent_artifact_id
		)
	)`

// d1SessionMigrationManifestSelect builds the manifest read for one table and
// one session.
//
// It is the direct translation of sessionMigrationReadManifestRowsMode, and the
// same text serves the source snapshot, the pre-copy target snapshot, the parity
// readback, and the revalidation: a parity proof computed over a different row
// set than the copy proved nothing.
func d1SessionMigrationManifestSelect(
	entry SessionMigrationManifestEntry, plan SessionMigrationExecutionPlan,
) (string, error) {
	if len(plan.PrimaryKey) == 0 || len(plan.Columns) == 0 {
		return "", fmt.Errorf("session migration plan for %s is incomplete", entry.Table)
	}
	columns := make([]string, 0, len(plan.Columns))
	for _, column := range plan.Columns {
		columns = append(columns, "t0."+d1QuoteIdent(column))
	}
	query := "SELECT " + strings.Join(columns, ",") + " FROM " + d1QuoteIdent(entry.Table) + " t0"

	if entry.Direct {
		query += " WHERE t0." + d1QuoteIdent(entry.SessionColumn) + " = ?"
	} else {
		ancestors, ancestorPlans, err := d1SessionMigrationAncestors(entry)
		if err != nil {
			return "", err
		}
		currentPlan := plan
		for index, ancestor := range ancestors {
			alias := strconv.Itoa(index + 1)
			query += " JOIN " + d1QuoteIdent(ancestor.Table) + " t" + alias +
				" ON t" + strconv.Itoa(index) + "." + d1QuoteIdent(currentPlan.ParentColumn) +
				" = t" + alias + "." + d1QuoteIdent(ancestorPlans[index].PrimaryKey[0])
			currentPlan = ancestorPlans[index]
		}
		root := ancestors[len(ancestors)-1]
		query += " WHERE t" + strconv.Itoa(len(ancestors)) + "." + d1QuoteIdent(root.SessionColumn) + " = ?"
	}
	if entry.Table == "memory_derivation_dependencies" {
		query += d1SessionMigrationDerivedGuard
	}
	order := make([]string, 0, len(plan.PrimaryKey))
	for _, column := range plan.PrimaryKey {
		order = append(order, "t0."+d1QuoteIdent(column))
	}
	return query + " ORDER BY " + strings.Join(order, ","), nil
}

// d1SessionMigrationNewRowIDSQL resolves the id the engine assigned to the row
// the preceding copy statement wrote.
//
// The subquery is scoped to the copy target through the same ancestor chain the
// read uses, so a concurrent writer into a different session cannot be picked up,
// and it takes the newest row because the copy loop walks each table in ascending
// primary-key order. Only auto-increment tables reach this path: a precomputed or
// preserved key is already known in Go and needs no lookup at all.
func d1SessionMigrationNewRowIDSQL(
	entry SessionMigrationManifestEntry, plan SessionMigrationExecutionPlan,
) (string, error) {
	if entry.Direct {
		return "SELECT q.id FROM " + d1QuoteIdent(entry.Table) + " q" +
			" WHERE q." + d1QuoteIdent(entry.SessionColumn) + " = ?" +
			" ORDER BY q.id DESC LIMIT 1", nil
	}
	ancestors, ancestorPlans, err := d1SessionMigrationAncestors(entry)
	if err != nil {
		return "", err
	}
	query := "SELECT q.id FROM " + d1QuoteIdent(entry.Table) + " q"
	currentPlan := plan
	for index, ancestor := range ancestors {
		alias := "j" + strconv.Itoa(index)
		query += " JOIN " + d1QuoteIdent(ancestor.Table) + " " + alias +
			" ON q." + d1QuoteIdent(currentPlan.ParentColumn) +
			" = " + alias + "." + d1QuoteIdent(ancestorPlans[index].PrimaryKey[0])
		currentPlan = ancestorPlans[index]
	}
	root := ancestors[len(ancestors)-1]
	return query + " WHERE " + d1QuoteIdent(root.Table) + "." + d1QuoteIdent(root.SessionColumn) +
		" = ? ORDER BY q.id DESC LIMIT 1", nil
}

// d1SessionMigrationCopyStatement renders one manifest row copy.
//
// The primary key is omitted for an auto-increment table so the engine allocates
// it, and a database-generated column is omitted entirely, because the target
// generated value is not the source one and copying it would collide with the
// uniqueness that generates it.
func d1SessionMigrationCopyStatement(
	entry SessionMigrationManifestEntry, plan SessionMigrationExecutionPlan, target sessionMigrationRow,
) (string, []any, error) {
	columns := make([]string, 0, len(plan.Columns))
	args := make([]any, 0, len(plan.Columns))
	for _, column := range plan.Columns {
		if len(plan.PrimaryKey) == 1 && column == plan.PrimaryKey[0] &&
			plan.PrimaryKeyMode == SessionMigrationKeyAutoIncrement {
			continue
		}
		if sessionMigrationPlanDatabaseGenerated(plan, column) {
			continue
		}
		columns = append(columns, d1QuoteIdent(column))
		if value := target.Values[column]; value.Valid {
			args = append(args, value.Text)
		} else {
			args = append(args, nil)
		}
	}
	if len(columns) == 0 {
		return "", nil, fmt.Errorf("session migration copy of %s has no writable columns", entry.Table)
	}
	return "INSERT INTO " + d1QuoteIdent(entry.Table) + " (" + strings.Join(columns, ",") +
		") VALUES (" + d1SessionMigrationPlaceholders(len(columns)) + ")", args, nil
}

// ---------------------------------------------------------------------------
// manifest reads
// ---------------------------------------------------------------------------

// d1SessionMigrationReadRows reads one manifest table for one session.
//
// Every column is read as an untyped value and normalised through the shared
// sessionMigrationNormalizeDatabaseValue, because the parity hash compares source
// text against target text and a driver-dependent rendering (a DATETIME, a
// []byte) would make the two providers hash the same row differently.
func (s *d1Store) d1SessionMigrationReadRows(
	ctx context.Context, entry SessionMigrationManifestEntry,
	plan SessionMigrationExecutionPlan, sessionID string,
) ([]sessionMigrationRow, error) {
	query, err := d1SessionMigrationManifestSelect(entry, plan)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Query(ctx, query, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []sessionMigrationRow{}
	for rows.Next() {
		cells := make([]any, len(plan.Columns))
		dest := make([]any, len(cells))
		for index := range cells {
			dest[index] = &cells[index]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		item := sessionMigrationRow{Values: make(map[string]sessionMigrationCell, len(plan.Columns))}
		for index, column := range plan.Columns {
			cell, err := sessionMigrationNormalizeDatabaseValue(cells[index])
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", entry.Table, column, err)
			}
			item.Values[column] = cell
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if entry.Table == "precise_memory_units" {
		if err := s.d1SessionMigrationProjectPreciseEvidenceIDs(ctx, sessionID, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// d1SessionMigrationProjectPreciseEvidenceIDs reproduces the reference's
// evidence-link projection for a rerolled unit.
//
// The reference builds it in SQL with JSON_TABLE and JSON_ARRAYAGG. SQLite has
// neither, so the same rule is applied after the rows are read: an invalidated
// unit whose source revision is superseded, invalidated, or deleted has its direct
// evidence id list projected down to the ids that still resolve to a stored
// evidence row, in the stored order, and the result is the empty array when
// nothing survives.
//
// It matters that this runs for the source read AND the target parity read,
// because the reference selects the identical SQL expression in both places. If
// only one side projected, the two hashes could never agree for a rerolled
// session and every reroll migration would report a false parity mismatch.
//
// Elements that are not JSON integers, and integers outside the positive int64
// range, are kept untouched. They are wrong-session or malformed historical
// references the ordinary remapper must still see, not rows to silently erase.
func (s *d1Store) d1SessionMigrationProjectPreciseEvidenceIDs(
	ctx context.Context, sessionID string, rows []sessionMigrationRow,
) error {
	parsed := make([][]string, len(rows))
	needed := make([]bool, len(rows))
	ids := []int64{}
	for index, row := range rows {
		if strings.TrimSpace(d1SessionMigrationCellText(row.Values["lifecycle_state"])) != "invalidated" {
			continue
		}
		raw := strings.TrimSpace(d1SessionMigrationCellText(row.Values["direct_evidence_ids_json"]))
		if !d1SessionMigrationIsJSONArray(raw) {
			continue
		}
		var elements []json.RawMessage
		if json.Unmarshal([]byte(raw), &elements) != nil {
			continue
		}
		needed[index] = true
		parsed[index] = make([]string, 0, len(elements))
		for _, element := range elements {
			trimmed := strings.TrimSpace(string(element))
			parsed[index] = append(parsed[index], trimmed)
			if id, ok := d1SessionMigrationPositiveInt64(trimmed); ok {
				ids = append(ids, id)
			}
		}
	}
	if !d1SessionMigrationAnySet(needed) {
		return nil
	}
	live, err := s.d1SessionMigrationExistingEvidenceIDs(ctx, ids)
	if err != nil {
		return err
	}
	nonActive, err := s.d1SessionMigrationNonActiveRevisions(ctx, sessionID)
	if err != nil {
		return err
	}
	for index, row := range rows {
		if !needed[index] {
			continue
		}
		if !nonActive[strings.TrimSpace(d1SessionMigrationCellText(row.Values["source_revision"]))] {
			continue
		}
		kept := make([]json.RawMessage, 0, len(parsed[index]))
		for _, element := range parsed[index] {
			if id, ok := d1SessionMigrationPositiveInt64(element); !ok || live[id] {
				kept = append(kept, json.RawMessage(element))
			}
		}
		encoded, err := json.Marshal(kept)
		if err != nil {
			return err
		}
		row.Values["direct_evidence_ids_json"] = sessionMigrationCell{Valid: true, Text: string(encoded)}
	}
	return nil
}

func d1SessionMigrationAnySet(flags []bool) bool {
	for _, flag := range flags {
		if flag {
			return true
		}
	}
	return false
}

func d1SessionMigrationIsJSONArray(raw string) bool {
	if !json.Valid([]byte(raw)) {
		return false
	}
	var probe any
	if json.Unmarshal([]byte(raw), &probe) != nil {
		return false
	}
	_, ok := probe.([]any)
	return ok
}

func d1SessionMigrationPositiveInt64(raw string) (int64, bool) {
	var number json.Number
	if json.Unmarshal([]byte(raw), &number) != nil {
		return 0, false
	}
	value, err := number.Int64()
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}

func (s *d1Store) d1SessionMigrationExistingEvidenceIDs(
	ctx context.Context, ids []int64,
) (map[int64]bool, error) {
	unique := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		key := strconv.FormatInt(id, 10)
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, key)
	}
	live := map[int64]bool{}
	for start := 0; start < len(unique); start += d1SessionMigrationInClauseChunk {
		end := start + d1SessionMigrationInClauseChunk
		if end > len(unique) {
			end = len(unique)
		}
		clause, args := d1SessionMigrationInClause("id", unique[start:end])
		rows, err := s.conn.Query(ctx, "SELECT id FROM direct_evidence_records WHERE "+clause, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			live[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return live, nil
}

func (s *d1Store) d1SessionMigrationNonActiveRevisions(
	ctx context.Context, sessionID string,
) (map[string]bool, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT source_revision
		FROM memory_source_revisions
		WHERE chat_session_id = ?
		  AND lifecycle_state IN ('superseded', 'invalidated', 'deleted')
	`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var revision string
		if err := rows.Scan(&revision); err != nil {
			return nil, err
		}
		out[strings.TrimSpace(revision)] = true
	}
	return out, rows.Err()
}

func d1SessionMigrationCellText(cell sessionMigrationCell) string {
	if !cell.Valid {
		return ""
	}
	return cell.Text
}

// d1SessionMigrationRun emits an ordered statement list in bounded batches.
//
// The order is never reordered. Several statements depend on the ones before them
// having run, and a copy that reached the target in a different order would
// produce the same rows but would not be reviewable against the reference.
func (s *d1Store) d1SessionMigrationRun(ctx context.Context, statements []D1Statement) error {
	for start := 0; start < len(statements); start += d1SessionMigrationBatchChunk {
		end := start + d1SessionMigrationBatchChunk
		if end > len(statements) {
			end = len(statements)
		}
		if err := s.conn.Batch(ctx, statements[start:end]...); err != nil {
			return err
		}
	}
	return nil
}

// d1SessionMigrationReadMigration reads the ledger row a phase acts on.
func (s *d1Store) d1SessionMigrationReadMigration(
	ctx context.Context, migrationID int64,
) (sourceID, targetID, mode, status string, reindexedCount int, lockedAt *time.Time, err error) {
	err = s.conn.QueryRow(ctx, `
		SELECT source_session_id, target_session_id, mode, status,
		       chroma_reindexed_count, locked_at
		FROM session_migrations
		WHERE id = ?
	`, migrationID).Scan(&sourceID, &targetID, &mode, &status, &reindexedCount, &lockedAt)
	if errors.Is(err, errD1NoRows) {
		err = ErrNotFound
	}
	return sourceID, targetID, mode, status, reindexedCount, lockedAt, err
}

// d1SessionMigrationCopyingID reads back the id phase 1 assigned.
//
// The lookup is scoped to the same triple and to status copying, and a retry
// disposes of any earlier copying row for the same triple first, so the newest
// such row is the one this call wrote. The copy also runs under the store write
// mutex, which is what makes that true within one service instance; two Cloudflare
// Containers copying the same pair concurrently is unresolved here exactly as it
// is for every other write in this provider.
func (s *d1Store) d1SessionMigrationCopyingID(
	ctx context.Context, req SessionMigrationCompleteRequest,
) (int64, error) {
	var id int64
	err := s.conn.QueryRow(ctx, `
		SELECT id
		FROM session_migrations
		WHERE source_session_id = ? AND target_session_id = ? AND mode = ? AND status = 'copying'
		ORDER BY id DESC
		LIMIT 1
	`, req.SourceSessionID, req.TargetSessionID, req.Mode).Scan(&id)
	if errors.Is(err, errD1NoRows) {
		return 0, errors.New("session migration ledger row was not created")
	}
	return id, err
}

// ---------------------------------------------------------------------------
// SessionMigrationStore
// ---------------------------------------------------------------------------

// InspectSessionMigrationOccupancy reports how much a session already holds.
//
// DirectTableCounts covers every direct manifest table, including the queued work
// and lineage tables the legacy counters ignore, because the copy refuses a
// non-empty target on any of them.
func (s *d1Store) InspectSessionMigrationOccupancy(ctx context.Context, sessionID string) (SessionMigrationOccupancy, error) {
	sid := strings.TrimSpace(sessionID)
	if sid == "" {
		return classifySessionMigrationOccupancy(map[string]int{}, false), nil
	}
	counts := make(map[string]int)
	for _, entry := range SessionMigrationManifest() {
		if !entry.Direct {
			continue
		}
		if _, ok := SessionMigrationExecutionPlanFor(entry.Table); !ok {
			return SessionMigrationOccupancy{}, fmt.Errorf("session migration manifest plan missing for %s", entry.Table)
		}
		var count int
		err := s.conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM "+d1QuoteIdent(entry.Table)+" WHERE "+d1QuoteIdent(entry.SessionColumn)+" = ?",
			sid).Scan(&count)
		if err != nil {
			return SessionMigrationOccupancy{}, fmt.Errorf("session migration occupancy %s: %w", entry.Table, err)
		}
		counts[entry.Table] = count
	}
	starter := false
	if counts["chat_logs"] == 1 {
		var turnIndex int
		var role string
		if err := s.conn.QueryRow(ctx, `
			SELECT turn_index, role
			FROM chat_logs
			WHERE chat_session_id = ?
			LIMIT 1
		`, sid).Scan(&turnIndex, &role); err != nil {
			return SessionMigrationOccupancy{}, fmt.Errorf("session migration occupancy chat_logs starter: %w", err)
		}
		starter = turnIndex == 0 && strings.EqualFold(strings.TrimSpace(role), "assistant")
	}
	return classifySessionMigrationOccupancy(counts, starter), nil
}

// GetSessionMigrationResumeContext finds the migration a repeated complete call
// would resume.
//
// A reverted migration is excluded rather than treated as a resume target: its
// copied rows are gone, so resuming it would claim parity it no longer has.
func (s *d1Store) GetSessionMigrationResumeContext(
	ctx context.Context, req SessionMigrationCompleteRequest,
) (*SessionMigrationResumeContext, error) {
	sourceID := strings.TrimSpace(req.SourceSessionID)
	targetID := strings.TrimSpace(req.TargetSessionID)
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = SessionMigrationModeCopyThenLockSource
	}
	if sourceID == "" || targetID == "" {
		return nil, ErrNotFound
	}
	result := &SessionMigrationResumeContext{}
	err := s.conn.QueryRow(ctx, `
		SELECT id, status, source_session_id, target_session_id, mode
		FROM session_migrations
		WHERE source_session_id = ?
		  AND target_session_id = ?
		  AND mode = ?
		  AND status NOT IN `+d1SessionMigrationRevertedStatuses+`
		ORDER BY id DESC
		LIMIT 1
	`, sourceID, targetID, mode).Scan(
		&result.MigrationID, &result.Status, &result.SourceSessionID,
		&result.TargetSessionID, &result.Mode,
	)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CompleteSessionMigration copies one whole session into an empty target.
func (s *d1Store) CompleteSessionMigration(
	ctx context.Context, req SessionMigrationCompleteRequest,
) (*SessionMigrationCompleteResult, error) {
	sourceID := strings.TrimSpace(req.SourceSessionID)
	targetID := strings.TrimSpace(req.TargetSessionID)
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = SessionMigrationModeCopyThenLockSource
	}
	if sourceID == "" || targetID == "" {
		return nil, errors.New("source_session_id and target_session_id are required")
	}
	if sourceID == targetID {
		return nil, errors.New("source and target sessions must differ")
	}
	if mode != SessionMigrationModeCopyThenLockSource && mode != SessionMigrationModeCopyKeepSource {
		return nil, errors.New("unsupported session migration mode")
	}
	if blockers := SessionMigrationManifestReleaseBlockers(); len(blockers) > 0 {
		return nil, fmt.Errorf("session migration manifest unsupported: %s", strings.Join(blockers, ","))
	}
	if err := s.d1SessionMigrationValidateManifestSchema(ctx); err != nil {
		return nil, err
	}
	if resumed, err := s.d1SessionMigrationResume(ctx, sourceID, targetID, mode); err != nil {
		return nil, err
	} else if resumed != nil {
		return resumed, nil
	}
	// A copy interrupted between phases leaves a migration row in status copying.
	// It has no parity proof, so nothing can act on it, and its copied target rows
	// would make the fresh copy below fail the empty-target gate for ever.
	// Disposing of it through the ledger-scoped rollback is what makes an
	// interrupted migration resumable instead of a permanently blocked target.
	if err := s.d1SessionMigrationDiscardInterruptedCopies(ctx, sourceID, targetID, mode); err != nil {
		return nil, err
	}

	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	execution, err := s.d1SessionMigrationCopy(ctx, SessionMigrationCompleteRequest{
		SourceSessionID:         sourceID,
		TargetSessionID:         targetID,
		Mode:                    mode,
		OperatorNote:            strings.TrimSpace(req.OperatorNote),
		RebuildPublicProjection: req.RebuildPublicProjection,
	})
	if err != nil {
		return nil, err
	}
	return &SessionMigrationCompleteResult{
		MigrationID:           execution.MigrationID,
		Status:                "copied",
		SourceSessionID:       sourceID,
		TargetSessionID:       targetID,
		Mode:                  mode,
		Counts:                execution.Counts,
		RowMapCount:           execution.RowMapCount,
		ChromaReindexedCount:  0,
		SourceLocked:          false,
		ChromaReindexRequired: execution.VectorExpectedCount > 0,
		ReadyForLive:          false,
		TargetStarterReplaced: execution.TargetStarterReplaced,
		EntityIDMap:           execution.EntityIDMap,
	}, nil
}

// d1SessionMigrationDiscardInterruptedCopies rolls back any migration of the
// same triple that never reached a status the resume path accepts.
func (s *d1Store) d1SessionMigrationDiscardInterruptedCopies(
	ctx context.Context, sourceID, targetID, mode string,
) error {
	rows, err := s.conn.Query(ctx, `
		SELECT id
		FROM session_migrations
		WHERE source_session_id = ? AND target_session_id = ? AND mode = ?
		  AND status = 'copying'
		ORDER BY id ASC
	`, sourceID, targetID, mode)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := s.d1SessionMigrationRollback(ctx, id, "interrupted copy discarded before retry"); err != nil {
			return err
		}
	}
	return nil
}

// d1SessionMigrationDeferred is one foreign key that can only be resolved after
// the copy, because both the referencing row and the referenced row carry
// engine-assigned ids.
type d1SessionMigrationDeferred struct {
	table      string
	primaryKey string
	column     string
	sourceKey  string
	targetKey  string
	reference  SessionMigrationForeignKeyPlan
	sourceVal  string
}

// d1SessionMigrationVectorCandidate is one copied row that may publish a vector
// document.
//
// The document id is tier + target session + the row's vector id, and for an
// auto-increment table that id is only known after the copy batch, so the
// candidate is completed from the row-map readback before the expected-ID ledger
// is written. Building it from the source id instead would name a target document
// that does not exist, and the parity check would then under-report forever.
type d1SessionMigrationVectorCandidate struct {
	entry  SessionMigrationManifestEntry
	plan   SessionMigrationExecutionPlan
	source sessionMigrationRow
	target sessionMigrationRow
}

// d1SessionMigrationCopy is the write phase.
func (s *d1Store) d1SessionMigrationCopy(
	ctx context.Context, req SessionMigrationCompleteRequest,
) (*sessionMigrationManifestExecution, error) {
	manifest := SessionMigrationManifest()
	sourceRows := make(map[string][]sessionMigrationRow, len(manifest))
	targetRowsBefore := make(map[string][]sessionMigrationRow, len(manifest))
	for _, entry := range manifest {
		if !entry.Implemented {
			return nil, fmt.Errorf("session migration manifest entry %s is not implemented", entry.Table)
		}
		plan, ok := SessionMigrationExecutionPlanFor(entry.Table)
		if !ok {
			return nil, fmt.Errorf("session migration manifest plan missing for %s", entry.Table)
		}
		rows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, req.SourceSessionID)
		if err != nil {
			return nil, fmt.Errorf("session migration source snapshot %s: %w", entry.Table, err)
		}
		sourceRows[entry.Table] = rows
		rows, err = s.d1SessionMigrationReadRows(ctx, entry, plan, req.TargetSessionID)
		if err != nil {
			return nil, fmt.Errorf("session migration target snapshot %s: %w", entry.Table, err)
		}
		targetRowsBefore[entry.Table] = rows
	}
	return s.d1SessionMigrationCopySnapshot(ctx, req, manifest, sourceRows, targetRowsBefore)
}

// d1SessionMigrationCopySnapshot performs the manifest copy against an already
// captured source and target snapshot. The stitch owner supplies its own ordered
// snapshot through the same path.
func (s *d1Store) d1SessionMigrationCopySnapshot(
	ctx context.Context,
	req SessionMigrationCompleteRequest,
	manifest []SessionMigrationManifestEntry,
	sourceRows, targetRowsBefore map[string][]sessionMigrationRow,
) (*sessionMigrationManifestExecution, error) {
	targetStarter, err := sessionMigrationValidateManifestSnapshots(manifest, sourceRows, targetRowsBefore)
	if err != nil {
		return nil, err
	}
	counts := sessionMigrationLegacyCountsFromManifest(sourceRows)
	counts.ReplaceableStarterOnly = targetStarter
	initialCountsJSON, _ := json.Marshal(counts)

	// ---- phase 1: the ledger row ----
	if err := s.conn.Batch(ctx, D1Statement{
		SQL: `
		INSERT INTO session_migrations (
			source_session_id, target_session_id, mode, status, operator_note,
			counts_json, chroma_reindexed_count, errors_json
		) VALUES (?, ?, ?, 'copying', ?, ?, 0, '[]')`,
		Args: []any{req.SourceSessionID, req.TargetSessionID, req.Mode,
			d1NullableString(strings.TrimSpace(req.OperatorNote)), string(initialCountsJSON)},
	}); err != nil {
		return nil, err
	}
	migrationID, err := s.d1SessionMigrationCopyingID(ctx, req)
	if err != nil {
		return nil, err
	}

	requestHash := sessionMigrationStringHash(
		SessionMigrationManifestVersion, req.SourceSessionID, req.TargetSessionID, req.Mode)
	statements := []D1Statement{
		d1SessionMigrationSagaStatement(migrationID, "manifest_validate", "completed",
			requestHash, `{"validated":true}`, ""),
		d1SessionMigrationSagaStatement(migrationID, "relational_copy", "running", requestHash, "", ""),
	}
	if targetStarter {
		statements = append(statements, D1Statement{
			SQL: `
			DELETE FROM chat_logs
			WHERE chat_session_id = ? AND turn_index = 0 AND LOWER(TRIM(role)) = 'assistant'`,
			Args: []any{req.TargetSessionID},
		})
	}

	keyMaps := newSessionMigrationKeyMaps()
	if err := sessionMigrationPrecomputeDeterministicKeys(manifest, sourceRows, req.TargetSessionID, keyMaps); err != nil {
		return nil, err
	}
	deferred := []d1SessionMigrationDeferred{}
	vectorCandidates := []d1SessionMigrationVectorCandidate{}
	memoryProjectionOps, err := sessionMigrationMemoryProjectionOperations(
		sourceRows["memory_vector_outbox"], sourceRows["memory_source_revisions"], req.SourceSessionID,
	)
	if err != nil {
		return nil, err
	}
	activeSourceRevisions := sessionMigrationActiveSourceRevisions(sourceRows["memory_source_revisions"])
	if req.RebuildPublicProjection != nil {
		repaired, err := s.d1SessionMigrationRepairPublicProjections(ctx, req, sourceRows, activeSourceRevisions)
		if err != nil {
			return nil, err
		}
		if repaired {
			// The repair wrote source receipts, so the outbox snapshot and the
			// projection decisions derived from it are stale. Re-read both, exactly
			// as the reference re-reads them inside its own transaction.
			entry, _ := sessionMigrationManifestEntryByTable("memory_vector_outbox")
			plan, _ := SessionMigrationExecutionPlanFor("memory_vector_outbox")
			rows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, req.SourceSessionID)
			if err != nil {
				return nil, err
			}
			sourceRows["memory_vector_outbox"] = rows
			memoryProjectionOps, err = sessionMigrationMemoryProjectionOperations(
				sourceRows["memory_vector_outbox"], sourceRows["memory_source_revisions"], req.SourceSessionID,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	// ---- phase 2a: the ordered copy ----
	rowMapCount := 0
	for _, entry := range manifest {
		if entry.Policy != SessionMigrationPolicyCopy {
			continue
		}
		plan, _ := SessionMigrationExecutionPlanFor(entry.Table)
		for _, sourceRow := range sourceRows[entry.Table] {
			targetRow, err := d1SessionMigrationBuildTargetRow(entry, plan, sourceRow, req.TargetSessionID, keyMaps)
			if err != nil {
				return nil, fmt.Errorf("session migration copy %s: %w", entry.Table, err)
			}
			deferred = append(deferred, d1SessionMigrationDeferredForeignKeys(entry, plan, sourceRow)...)

			copySQL, copyArgs, err := d1SessionMigrationCopyStatement(entry, plan, targetRow)
			if err != nil {
				return nil, fmt.Errorf("session migration copy %s: %w", entry.Table, err)
			}
			statements = append(statements, D1Statement{SQL: copySQL, Args: copyArgs})

			autoIncrement := len(plan.PrimaryKey) == 1 && plan.PrimaryKeyMode == SessionMigrationKeyAutoIncrement
			sourceKey := d1SessionMigrationCellText(sourceRow.Values[plan.PrimaryKey[0]])
			targetKey := d1SessionMigrationCellText(targetRow.Values[plan.PrimaryKey[0]])
			var targetArgs []any
			if autoIncrement {
				subquery, err := d1SessionMigrationNewRowIDSQL(entry, plan)
				if err != nil {
					return nil, fmt.Errorf("session migration copy %s: %w", entry.Table, err)
				}
				targetKey = subquery
				targetArgs = []any{req.TargetSessionID}
				// The assigned id is not the source id, so the target projection must
				// not keep the source value in that column: the vector expected
				// document id is built from it and would otherwise name a target
				// row that does not exist.
				delete(targetRow.Values, plan.PrimaryKey[0])
			} else if err := keyMaps.put(entry.Table, plan.PrimaryKey[0], sourceKey, targetKey); err != nil {
				return nil, err
			}
			keyMap, err := d1SessionMigrationKeyMapStatement(
				migrationID, entry.Table, plan.PrimaryKey[0], sourceKey, d1SessionMigrationLiteralOrSubquery(targetKey),
				"copied", targetArgs)
			if err != nil {
				return nil, err
			}
			statements = append(statements, keyMap)
			for _, generated := range plan.GeneratedKeys {
				sourceValue := sourceRow.Values[generated.Column]
				targetValue := targetRow.Values[generated.Column]
				if !sourceValue.Valid || !targetValue.Valid {
					continue
				}
				alternate, err := d1SessionMigrationKeyMapStatement(
					migrationID, entry.Table, generated.Column, sourceValue.Text,
					// The generated key's target value must go through the same
					// literal-or-subquery renderer the primary key map uses. It is a
					// plain copied value here and never a subquery, so it is always
					// quoted, but splicing it raw is a statement-injection-shaped bug —
					// and a generated key that happens to be a UUID
					// (memory_source_revisions.source_revision is exactly that) turns
					// it into a hard parse error rather than a subtle one.
					d1SessionMigrationLiteralOrSubquery(targetValue.Text),
					"alternate_key", nil)
				if err != nil {
					return nil, err
				}
				statements = append(statements, alternate)
			}
			if autoIncrement {
				if sourceRowID, parseErr := strconv.ParseInt(sourceKey, 10, 64); parseErr == nil {
					statements = append(statements, d1SessionMigrationRowMapStatement(
						migrationID, entry.Table, sourceRowID, targetKey, targetArgs))
				}
			}
			if entry.Table == "session_reference_bindings" {
				statements = append(statements, D1Statement{
					SQL: `
					INSERT INTO session_migration_reference_binding_map (
						migration_id, source_binding_id, target_binding_id, row_status
					) VALUES (?, ?, ?, 'copied')`,
					Args: []any{migrationID, sourceKey, targetKey},
				})
			}
			vectorCandidates = append(vectorCandidates, d1SessionMigrationVectorCandidate{
				entry: entry, plan: plan, source: sourceRow, target: targetRow,
			})
			rowMapCount++
		}
	}
	if err := s.d1SessionMigrationRun(ctx, statements); err != nil {
		return nil, err
	}

	// ---- phase 2b: the ids the engine assigned ----
	keyMaps, err = s.d1SessionMigrationLoadKeyMaps(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	tail := []D1Statement{}
	for _, item := range deferred {
		targetValue, ok := keyMaps.target(item.reference.ReferenceTable, item.reference.ReferenceColumn, item.sourceVal)
		if !ok {
			return nil, fmt.Errorf("session migration deferred FK %s.%s has no row map for %s.%s=%q",
				item.table, item.column, item.reference.ReferenceTable, item.reference.ReferenceColumn, item.sourceVal)
		}
		targetKey, ok := keyMaps.target(item.table, item.primaryKey, item.sourceKey)
		if !ok {
			return nil, fmt.Errorf("session migration deferred FK %s.%s has no row map for its own row %q",
				item.table, item.column, item.sourceKey)
		}
		// Binding the copied successor is part of the copy, not a new source
		// observation, so the stored timestamp is preserved explicitly. SQLite has
		// no ON UPDATE CURRENT_TIMESTAMP today; the clause keeps the intent visible
		// and keeps the statement honest if that ever changes.
		sql := "UPDATE " + d1QuoteIdent(item.table) + " SET " + d1QuoteIdent(item.column) + " = ?"
		if item.table == "memory_source_revisions" && item.column == "superseded_by_revision" {
			sql += ", updated_at = updated_at"
		}
		sql += " WHERE " + d1QuoteIdent(item.primaryKey) + " = ?"
		tail = append(tail, D1Statement{SQL: sql, Args: []any{targetValue, targetKey}})
	}
	projectionIDs := make([]string, 0, len(memoryProjectionOps))
	for id := range memoryProjectionOps {
		projectionIDs = append(projectionIDs, id)
	}
	sort.Strings(projectionIDs)
	for _, sourceDocumentID := range projectionIDs {
		projection := memoryProjectionOps[sourceDocumentID]
		sourceMemoryID := strings.TrimPrefix(sourceDocumentID, "memory:"+req.SourceSessionID+":")
		targetMemoryID, found := keyMaps.target("memories", "id", sourceMemoryID)
		if !found {
			// The decision's canonical memory no longer exists.
			continue
		}
		targetRevision, _ := keyMaps.target("memory_source_revisions", "source_revision", projection.SourceRevision)
		targetDocumentID := "memory:" + req.TargetSessionID + ":" + targetMemoryID
		item := &MemoryVectorOutboxItem{
			ContractVersion:     MemoryVectorOutboxContract,
			ChatSessionID:       req.TargetSessionID,
			SourceRevision:      targetRevision,
			DocumentID:          targetDocumentID,
			Operation:           projection.Operation,
			OperationKey:        sessionMigrationStringHash("migration_public_projection", req.TargetSessionID, targetRevision, targetDocumentID),
			RequiredSourceState: "active",
			Status:              "completed",
		}
		if projection.Operation == "upsert" {
			document, err := json.Marshal(map[string]any{
				"ID": targetDocumentID, "Tier": "memory", "ChatSessionID": req.TargetSessionID,
				"SourceTable": "memories", "SourceRowID": targetMemoryID, "SchemaVersion": "memory.v2",
				"DocumentText": projection.DocumentText,
				"Metadata": memoryVectorVerificationMetadata(
					targetRevision, MemorySourceRevisionContract, MemoryPublicProjectionIndex, projection.DocumentText),
			})
			if err != nil {
				return nil, err
			}
			item.DocumentJSON = string(document)
		}
		statement, err := d1SessionMigrationEnqueueProjection(ctx, s.conn, item)
		if err != nil {
			return nil, err
		}
		if statement.SQL == "" {
			continue
		}
		tail = append(tail, statement)
		// The receipt id is engine-assigned, so the outbox row map resolves it from
		// the row this very statement wrote, keyed by the operation key, which the
		// schema makes unique.
		outboxMap, err := d1SessionMigrationKeyMapStatement(
			migrationID, "memory_vector_outbox", "id", strconv.FormatInt(projection.OutboxID, 10),
			"(SELECT q.id FROM memory_vector_outbox q WHERE q.operation_key = ? ORDER BY q.id DESC LIMIT 1)",
			"alternate_key", []any{item.OperationKey})
		if err != nil {
			return nil, err
		}
		tail = append(tail, outboxMap)
	}
	if err := s.d1SessionMigrationRun(ctx, tail); err != nil {
		return nil, err
	}
	keyMaps, err = s.d1SessionMigrationLoadKeyMaps(ctx, migrationID)
	if err != nil {
		return nil, err
	}

	// ---- phase 3: parity, the vector ledger, and the commit point ----
	parityRows, err := s.d1SessionMigrationParity(ctx, manifest, sourceRows,
		req.SourceSessionID, req.TargetSessionID, keyMaps)
	if err != nil {
		return nil, err
	}
	vectorExpected, err := d1SessionMigrationExpectedVectors(
		vectorCandidates, keyMaps, migrationID, req.SourceSessionID, req.TargetSessionID,
		activeSourceRevisions, memoryProjectionOps)
	if err != nil {
		return nil, err
	}
	if err := s.d1SessionMigrationRun(ctx, d1SessionMigrationClosingStatements(
		migrationID, parityRows, vectorExpected, requestHash, rowMapCount, counts)); err != nil {
		return nil, err
	}

	return &sessionMigrationManifestExecution{
		MigrationID:           migrationID,
		Counts:                counts,
		RowMapCount:           rowMapCount,
		VectorExpectedCount:   len(vectorExpected),
		TargetStarterReplaced: targetStarter,
		EntityIDMap:           keyMaps.forward["entity_identities.stable_entity_id"],
	}, nil
}

// d1SessionMigrationBuildTargetRow produces the target projection of one source
// row without writing anything.
//
// It is the reference insert routine split at the point where LastInsertId is
// unavailable: everything the reference decides before the INSERT happens here,
// and the id-dependent rows are emitted as batch-internal subqueries instead.
func d1SessionMigrationBuildTargetRow(
	entry SessionMigrationManifestEntry,
	plan SessionMigrationExecutionPlan,
	source sessionMigrationRow,
	targetSessionID string,
	maps *sessionMigrationKeyMaps,
) (sessionMigrationRow, error) {
	if len(plan.PrimaryKey) != 1 {
		return sessionMigrationRow{}, fmt.Errorf("copy table %s has an unsupported composite primary key", entry.Table)
	}
	primaryKey := plan.PrimaryKey[0]
	sourceKey := source.Values[primaryKey]
	if !sourceKey.Valid || strings.TrimSpace(sourceKey.Text) == "" {
		return sessionMigrationRow{}, fmt.Errorf("source primary key %s is empty", primaryKey)
	}
	target := sessionMigrationRow{Values: make(map[string]sessionMigrationCell, len(source.Values))}
	for column, value := range source.Values {
		target.Values[column] = value
	}
	if entry.Direct {
		target.Values[entry.SessionColumn] = sessionMigrationCell{Valid: true, Text: targetSessionID}
	}
	if entry.Table == "lorebook_reference_scopes" {
		if err := sessionMigrationRemapLorebookScopeIdentity(&target, targetSessionID); err != nil {
			return sessionMigrationRow{}, err
		}
	}
	if sessionMigrationPrecomputedPrimaryKey(plan.PrimaryKeyMode) {
		value, ok := maps.target(entry.Table, primaryKey, sourceKey.Text)
		if !ok {
			return sessionMigrationRow{}, errors.New("precomputed primary key map missing")
		}
		target.Values[primaryKey] = sessionMigrationCell{Valid: true, Text: value}
	}
	for _, generated := range plan.GeneratedKeys {
		sourceValue := source.Values[generated.Column]
		if !sourceValue.Valid {
			continue
		}
		targetValue, ok := maps.target(entry.Table, generated.Column, sourceValue.Text)
		if !ok {
			return sessionMigrationRow{}, fmt.Errorf("generated key map missing for %s", generated.Column)
		}
		target.Values[generated.Column] = sessionMigrationCell{Valid: true, Text: targetValue}
	}
	for _, fk := range plan.ForeignKeys {
		sourceValue := source.Values[fk.Column]
		if sessionMigrationForeignKeyAbsent(plan.Table, fk, sourceValue) {
			continue
		}
		if fk.Deferred {
			// The referenced row is copied by a later statement, so this value can
			// only be bound once both row maps exist.
			target.Values[fk.Column] = sessionMigrationCell{}
			continue
		}
		targetValue, ok := maps.target(fk.ReferenceTable, fk.ReferenceColumn, sourceValue.Text)
		if !ok {
			return sessionMigrationRow{}, fmt.Errorf("FK %s has no row map for %s.%s=%q",
				fk.Column, fk.ReferenceTable, fk.ReferenceColumn, sourceValue.Text)
		}
		target.Values[fk.Column] = sessionMigrationCell{Valid: true, Text: targetValue}
	}
	if err := sessionMigrationRemapSemanticReferences(plan, source, &target, maps); err != nil {
		return sessionMigrationRow{}, err
	}
	if entry.Table == "memory_source_revisions" {
		if err := sessionMigrationValidateAdmissionResult(source); err != nil {
			return sessionMigrationRow{}, err
		}
		resultHash, _, err := sessionMigrationRemappedAdmissionResult(target)
		if err != nil {
			return sessionMigrationRow{}, err
		}
		if resultHash != "" {
			target.Values["derived_result_hash"] = sessionMigrationCell{Valid: true, Text: resultHash}
		}
	}
	return target, nil
}

// d1SessionMigrationDeferredForeignKeys lists the foreign keys of one row that
// must be bound after the whole copy has run.
func d1SessionMigrationDeferredForeignKeys(
	entry SessionMigrationManifestEntry,
	plan SessionMigrationExecutionPlan,
	source sessionMigrationRow,
) []d1SessionMigrationDeferred {
	out := []d1SessionMigrationDeferred{}
	for _, fk := range plan.ForeignKeys {
		if !fk.Deferred {
			continue
		}
		sourceValue := source.Values[fk.Column]
		if sessionMigrationForeignKeyAbsent(plan.Table, fk, sourceValue) {
			continue
		}
		out = append(out, d1SessionMigrationDeferred{
			table:      entry.Table,
			primaryKey: plan.PrimaryKey[0],
			column:     fk.Column,
			sourceKey:  d1SessionMigrationCellText(source.Values[plan.PrimaryKey[0]]),
			reference:  fk,
			sourceVal:  sourceValue.Text,
		})
	}
	return out
}

// d1SessionMigrationEnqueueProjection emits the outbox receipt for one copied
// public projection decision.
//
// The reference inserts and, on a duplicate key, compares the stored row with
// the incoming one. A batch cannot recover from a constraint failure, so the
// comparison is done first: an identical stored receipt means the work is already
// queued and nothing is written, and a different one under the same key means the
// key is lying about what it identifies, which is a conflict rather than a replay.
func d1SessionMigrationEnqueueProjection(
	ctx context.Context, conn D1Conn, item *MemoryVectorOutboxItem,
) (D1Statement, error) {
	var operation, sessionID, revision, documentID, existingJSON, requiredState string
	var embeddingReady bool
	err := conn.QueryRow(ctx, `
		SELECT operation, chat_session_id, source_revision, document_id,
		       COALESCE(document_json, ''), embedding_ready, required_source_state
		FROM memory_vector_outbox
		WHERE operation_key = ?
	`, item.OperationKey).Scan(&operation, &sessionID, &revision, &documentID,
		&existingJSON, &embeddingReady, &requiredState)
	if err == nil {
		if operation != item.Operation || sessionID != item.ChatSessionID ||
			revision != item.SourceRevision || documentID != item.DocumentID ||
			(operation == "upsert" && strings.TrimSpace(existingJSON) != strings.TrimSpace(item.DocumentJSON)) ||
			embeddingReady != item.EmbeddingReady || requiredState != item.RequiredSourceState {
			return D1Statement{}, errors.New("memory vector operation idempotency conflict")
		}
		return D1Statement{}, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return D1Statement{}, err
	}
	return d1EnqueueMemoryVectorStatement(item)
}

// d1SessionMigrationRepairPublicProjections rebuilds the source outbox receipts an
// older copy omitted, through the shared projection policy.
//
// It is the reference's optional repair pass and it is deliberately its own atomic
// batch rather than part of the copy batch: it writes SOURCE rows, it re-reads
// them immediately afterwards, and a partial repair would leave the source
// projection authority ambiguous. Keeping it atomic on its own is what makes the
// re-read honest.
func (s *d1Store) d1SessionMigrationRepairPublicProjections(
	ctx context.Context,
	req SessionMigrationCompleteRequest,
	sourceRows map[string][]sessionMigrationRow,
	activeSourceRevisions map[string]struct{},
) (bool, error) {
	currentByTurn := map[int]sessionMigrationRow{}
	for _, row := range sourceRows["memory_source_revisions"] {
		if _, active := activeSourceRevisions[d1SessionMigrationCellText(row.Values["source_revision"])]; active &&
			d1SessionMigrationCellText(row.Values["derived_admission_state"]) == "committed" &&
			strings.TrimSpace(d1SessionMigrationCellText(row.Values["derived_index_version"])) == MemoryPublicProjectionIndex {
			currentByTurn[sessionMigrationCellInt(row.Values["turn_index"])] = row
		}
	}
	existing, err := sessionMigrationMemoryProjectionOperations(
		sourceRows["memory_vector_outbox"], sourceRows["memory_source_revisions"], req.SourceSessionID)
	if err != nil {
		return false, err
	}
	statements := []D1Statement{}
	repaired := false
	for _, memory := range sourceRows["memories"] {
		memoryID := d1SessionMigrationCellText(memory.Values["id"])
		documentID := "memory:" + req.SourceSessionID + ":" + memoryID
		if _, present := existing[documentID]; present {
			continue
		}
		source, present := currentByTurn[sessionMigrationCellInt(memory.Values["turn_index"])]
		if !present {
			// Existing missing-authority handling still owns absent sources.
			continue
		}
		if err := sessionMigrationValidateAdmissionResult(source); err != nil {
			return false, err
		}
		revision := d1SessionMigrationCellText(source.Values["source_revision"])
		text := strings.TrimSpace(req.RebuildPublicProjection(
			d1SessionMigrationCellText(source.Values["derived_result_json"])))
		item := &MemoryVectorOutboxItem{
			ContractVersion:     MemoryVectorOutboxContract,
			ChatSessionID:       req.SourceSessionID,
			SourceRevision:      revision,
			DocumentID:          documentID,
			Operation:           "delete",
			OperationKey:        sessionMigrationStringHash("migration_projection_repair", req.SourceSessionID, revision, documentID),
			RequiredSourceState: "active",
			Status:              "completed",
		}
		if text != "" {
			item.Operation = "upsert"
			document, err := json.Marshal(map[string]any{
				"ID": documentID, "Tier": "memory", "ChatSessionID": req.SourceSessionID,
				"SourceTable": "memories", "SourceRowID": memoryID, "SchemaVersion": "memory.v2",
				"DocumentText": text,
				"Metadata": memoryVectorVerificationMetadata(
					revision, MemorySourceRevisionContract, MemoryPublicProjectionIndex, text),
			})
			if err != nil {
				return false, err
			}
			item.DocumentJSON = string(document)
		}
		statement, err := d1SessionMigrationEnqueueProjection(ctx, s.conn, item)
		if err != nil {
			return false, err
		}
		if statement.SQL == "" {
			continue
		}
		statements = append(statements, statement)
		repaired = true
	}
	if !repaired {
		return false, nil
	}
	if err := s.d1SessionMigrationRun(ctx, statements); err != nil {
		return false, err
	}
	return true, nil
}

// d1SessionMigrationLoadKeyMaps rebuilds the row maps from the durable ledger.
//
// The copy needs the assigned ids for the deferred foreign keys, the copied
// projection receipts, the vector expected documents, and the parity
// normalisation, and the ledger is the only place those ids were recorded.
func (s *d1Store) d1SessionMigrationLoadKeyMaps(
	ctx context.Context, migrationID int64,
) (*sessionMigrationKeyMaps, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT table_name, key_column_name, source_key, target_key
		FROM session_migration_artifact_row_map
		WHERE migration_id = ? AND row_status <> 'rolled_back'
		ORDER BY table_name, key_column_name, source_key
	`, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	maps := newSessionMigrationKeyMaps()
	for rows.Next() {
		var table, column, source, target string
		if err := rows.Scan(&table, &column, &source, &target); err != nil {
			return nil, err
		}
		if err := maps.put(table, column, source, target); err != nil {
			return nil, err
		}
	}
	return maps, rows.Err()
}

// d1SessionMigrationParityRow is one manifest table's proof.
type d1SessionMigrationParityRow struct {
	entry                                                  SessionMigrationManifestEntry
	sourceCount, targetCount                               int
	sourceHash, targetHash                                 string
	rowMapExpected, rowMapVerified, fkExpected, fkVerified int
	parityState                                            string
}

// d1SessionMigrationParity reads the target back and evaluates every manifest
// table against its stored proof.
//
// The evaluation itself is the shared sessionMigrationEvaluateArtifactParity, so
// the MariaDB and D1 providers apply the identical rule to the identical source
// and target rows; only the row transport differs.
func (s *d1Store) d1SessionMigrationParity(
	ctx context.Context,
	manifest []SessionMigrationManifestEntry,
	sourceRows map[string][]sessionMigrationRow,
	sourceSessionID, targetSessionID string,
	keyMaps *sessionMigrationKeyMaps,
) ([]d1SessionMigrationParityRow, error) {
	out := make([]d1SessionMigrationParityRow, 0, len(manifest))
	for _, entry := range manifest {
		plan, _ := SessionMigrationExecutionPlanFor(entry.Table)
		targetRows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, targetSessionID)
		if err != nil {
			return nil, fmt.Errorf("session migration target parity read %s: %w", entry.Table, err)
		}
		sourceHash := sessionMigrationCanonicalRowsHash(entry, plan, sourceRows[entry.Table], sourceSessionID, false, keyMaps)
		targetHash := sessionMigrationCanonicalRowsHash(entry, plan, targetRows, targetSessionID, true, keyMaps)
		evaluation, err := sessionMigrationEvaluateArtifactParity(
			entry, plan, sourceRows[entry.Table], targetRows, sourceHash, targetHash, keyMaps)
		if err != nil {
			return nil, err
		}
		out = append(out, d1SessionMigrationParityRow{
			entry:          entry,
			sourceCount:    len(sourceRows[entry.Table]),
			sourceHash:     sourceHash,
			targetCount:    len(targetRows),
			targetHash:     targetHash,
			rowMapExpected: evaluation.RowMapExpected,
			rowMapVerified: evaluation.RowMapVerified,
			fkExpected:     evaluation.FKExpected,
			fkVerified:     evaluation.FKVerified,
			parityState:    evaluation.ParityState,
		})
	}
	return out, nil
}

// d1SessionMigrationExpectedVectors builds the exact expected document set.
//
// The public projection decision is taken from the source outbox rather than from
// the canonical summary, exactly as the reference does: the latest causal outbox
// operation for a memory is either an exact public upsert or an explicit delete,
// and inferring visibility from the summary would resurrect memories the user
// retired.
func d1SessionMigrationExpectedVectors(
	candidates []d1SessionMigrationVectorCandidate,
	keyMaps *sessionMigrationKeyMaps,
	migrationID int64,
	sourceSessionID, targetSessionID string,
	activeSourceRevisions map[string]struct{},
	memoryProjectionOps map[string]sessionMigrationMemoryProjectionOperation,
) (map[string]SessionMigrationVectorDocument, error) {
	expected := map[string]SessionMigrationVectorDocument{}
	for _, candidate := range candidates {
		plan := candidate.plan
		if plan.Vector == nil {
			continue
		}
		target := candidate.target
		if mapped, ok := keyMaps.target(candidate.entry.Table, plan.Vector.IDColumn,
			d1SessionMigrationCellText(candidate.source.Values[plan.Vector.IDColumn])); ok {
			target.Values[plan.Vector.IDColumn] = sessionMigrationCell{Valid: true, Text: mapped}
		}
		document, ok := sessionMigrationExpectedVectorDocument(migrationID, candidate.entry.Table,
			plan, candidate.source, target, sourceSessionID, targetSessionID)
		if !ok {
			continue
		}
		if candidate.entry.Table == "precise_memory_units" &&
			!sessionMigrationPreciseVectorSourceActive(candidate.source, activeSourceRevisions) {
			continue
		}
		if candidate.entry.Table == "memories" {
			sourceMemoryID := d1SessionMigrationCellText(candidate.source.Values["id"])
			sourceDocumentID := "memory:" + sourceSessionID + ":" + sourceMemoryID
			projection, found := memoryProjectionOps[sourceDocumentID]
			if !found {
				return nil, fmt.Errorf(
					"session migration memory public projection authority is missing for %s; run canonical force reindex before retrying migration",
					sourceDocumentID)
			}
			if projection.SourceTurn != sessionMigrationCellInt(candidate.source.Values["turn_index"]) {
				return nil, fmt.Errorf(
					"session migration memory public projection turn mismatch for %s; run canonical force reindex before retrying migration",
					sourceDocumentID)
			}
			if projection.Operation == "delete" {
				continue
			}
			document.DocumentText = projection.DocumentText
		}
		if previous, duplicate := expected[document.ID]; duplicate && previous.SourceTable != document.SourceTable {
			return nil, fmt.Errorf("session migration vector expected ID collision %q", document.ID)
		}
		expected[document.ID] = document
	}
	return expected, nil
}

// d1SessionMigrationClosingStatements builds phase 3, whose last statement is the
// status flip. Nothing before it is visible to any other phase, and nothing after
// it needs to run, so a failure anywhere in the list leaves the migration in
// status copying, which is exactly the state a retry knows how to discard.
func d1SessionMigrationClosingStatements(
	migrationID int64,
	parityRows []d1SessionMigrationParityRow,
	vectorExpected map[string]SessionMigrationVectorDocument,
	requestHash string,
	rowMapCount int,
	counts SessionMigrationArtifactCounts,
) []D1Statement {
	statements := make([]D1Statement, 0, len(parityRows)*2+16)
	for _, row := range parityRows {
		statements = append(statements, D1Statement{
			SQL: `
			INSERT INTO session_migration_artifact_parity (
				migration_id, manifest_version, table_name, parent_table_name,
				session_column_name, migration_policy, source_row_count,
				source_content_hash, target_row_count, target_content_hash,
				row_map_expected_count, row_map_verified_count,
				fk_expected_count, fk_verified_count, parity_state,
				blocker_code, verified_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ` + d1NowExpression + `)`,
			Args: []any{migrationID, SessionMigrationManifestVersion, row.entry.Table,
				d1NullableString(row.entry.ParentTable), d1NullableString(row.entry.SessionColumn),
				row.entry.Policy, row.sourceCount, row.sourceHash, row.targetCount, row.targetHash,
				row.rowMapExpected, row.rowMapVerified, row.fkExpected, row.fkVerified, row.parityState},
		})
	}
	expectedIDs := make([]string, 0, len(vectorExpected))
	for id := range vectorExpected {
		expectedIDs = append(expectedIDs, id)
	}
	sort.Strings(expectedIDs)
	for _, id := range expectedIDs {
		document := vectorExpected[id]
		statements = append(statements, D1Statement{
			SQL: `
			INSERT INTO session_migration_vector_expected_ids (
				migration_id, document_id, source_table, source_row_id, observed
			) VALUES (?, ?, ?, ?, 0)`,
			Args: []any{migrationID, document.ID, document.SourceTable, document.SourceRowID},
		})
	}
	byTable := map[string][]string{}
	for _, id := range expectedIDs {
		byTable[vectorExpected[id].SourceTable] = append(byTable[vectorExpected[id].SourceTable], id)
	}
	tables := make([]string, 0, len(byTable))
	for table := range byTable {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		ids := byTable[table]
		sort.Strings(ids)
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE session_migration_artifact_parity
			SET vector_expected_count = ?,
			    vector_expected_id_hash = ?,
			    updated_at = ` + d1NowExpression + `
			WHERE migration_id = ? AND manifest_version = ? AND table_name = ?`,
			Args: []any{len(ids), sessionMigrationHashIDs(ids), migrationID,
				SessionMigrationManifestVersion, table},
		})
	}
	statements = append(statements,
		d1SessionMigrationSagaStatement(migrationID, "relational_copy", "completed", requestHash,
			fmt.Sprintf(`{"row_map_count":%d}`, rowMapCount), ""),
		d1SessionMigrationSagaStatement(migrationID, "vector_expected_ids", "completed", requestHash,
			fmt.Sprintf(`{"expected_count":%d}`, len(expectedIDs)), ""),
	)
	countsJSON, _ := json.Marshal(counts)
	statements = append(statements, D1Statement{
		SQL: `
		UPDATE session_migrations
		SET status = 'copied',
		    counts_json = ?,
		    errors_json = '["chroma_reindex_pending"]',
		    completed_at = ` + d1NowExpression + `,
		    updated_at = ` + d1NowExpression + `
		WHERE id = ?`,
		Args: []any{string(countsJSON), migrationID},
	})
	return statements
}

// ---------------------------------------------------------------------------
// schema and resume verification
// ---------------------------------------------------------------------------

// d1SessionMigrationValidateManifestSchema is the INFORMATION_SCHEMA check the
// reference runs before every destructive phase, restated for SQLite.
//
// It is not decoration. The manifest is an allowlist of column names, and a
// schema that drifted from it would make the copy silently drop or misplace a
// column; the reference refuses before touching the target for exactly that
// reason. PRAGMA table_xinfo is used rather than table_info because only the
// former reports generated columns, and the manifest names memory_source_revisions
// .active_logical_turn_slot as a database-generated column that must never be
// copied.
func (s *d1Store) d1SessionMigrationValidateManifestSchema(ctx context.Context) error {
	for _, entry := range SessionMigrationManifest() {
		plan, ok := SessionMigrationExecutionPlanFor(entry.Table)
		if !ok {
			return fmt.Errorf("session migration manifest plan missing for %s", entry.Table)
		}
		if err := s.d1SessionMigrationValidatePlanSchema(ctx, entry, plan); err != nil {
			return err
		}
	}
	return nil
}

func (s *d1Store) d1SessionMigrationValidatePlanSchema(
	ctx context.Context, entry SessionMigrationManifestEntry, plan SessionMigrationExecutionPlan,
) error {
	rows, err := s.conn.Query(ctx, "PRAGMA table_xinfo("+d1QuoteIdent(entry.Table)+")")
	if err != nil {
		return fmt.Errorf("session migration schema validation %s: %w", entry.Table, err)
	}
	actual := []string{}
	generated := []string{}
	primary := map[int]string{}
	for rows.Next() {
		cells := make([]any, 7)
		dest := make([]any, len(cells))
		for index := range cells {
			dest[index] = &cells[index]
		}
		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			return err
		}
		name, _ := cells[1].(string)
		actual = append(actual, name)
		if hidden := d1SessionMigrationAsInt(cells[6]); hidden == 2 || hidden == 3 {
			generated = append(generated, name)
		}
		if position := d1SessionMigrationAsInt(cells[5]); position > 0 {
			primary[position] = name
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if missing, extra, duplicates := sessionMigrationColumnSetDiff(actual, plan.Columns); len(missing) > 0 || len(extra) > 0 || len(duplicates) > 0 {
		return fmt.Errorf("session migration schema mismatch for %s: actual=%q manifest=%q missing=%q extra=%q duplicates=%q",
			entry.Table, strings.Join(actual, ","), strings.Join(plan.Columns, ","),
			strings.Join(missing, ","), strings.Join(extra, ","), strings.Join(duplicates, ","))
	}
	if missing, extra, duplicates := sessionMigrationColumnSetDiff(generated, plan.DatabaseGenerated); len(missing) > 0 || len(extra) > 0 || len(duplicates) > 0 {
		return fmt.Errorf(
			"session migration generated-column mismatch for %s: actual=%q manifest=%q missing=%q extra=%q duplicates=%q",
			entry.Table, strings.Join(generated, ","), strings.Join(plan.DatabaseGenerated, ","),
			strings.Join(missing, ","), strings.Join(extra, ","), strings.Join(duplicates, ","),
		)
	}
	ordered := make([]string, 0, len(primary))
	for position := 1; position <= len(primary); position++ {
		if name, ok := primary[position]; ok {
			ordered = append(ordered, name)
		}
	}
	if strings.Join(ordered, ",") != strings.Join(plan.PrimaryKey, ",") {
		return fmt.Errorf("session migration primary-key mismatch for %s: actual=%q manifest=%q",
			entry.Table, strings.Join(ordered, ","), strings.Join(plan.PrimaryKey, ","))
	}
	if entry.Direct {
		indexed, err := s.d1SessionMigrationHasLeadingIndex(ctx, entry.Table, entry.SessionColumn)
		if err != nil {
			return fmt.Errorf("session migration range-index validation %s: %w", entry.Table, err)
		}
		if !indexed {
			return fmt.Errorf("session migration range-lock index missing for %s.%s", entry.Table, entry.SessionColumn)
		}
	}
	return nil
}

// d1SessionMigrationHasLeadingIndex reports whether some index on the table
// starts with the column. The reference asks INFORMATION_SCHEMA.STATISTICS for the
// same thing: a session-scoped table without a leading session index would make
// every migration scan the whole canonical store.
//
// The answer is gathered in ONE query that joins the two PRAGMA table-valued
// functions. Asking for the index list and then, for each name, opening a second
// cursor would nest one cursor inside another on a pool that may hold a single
// connection, and the nested PRAGMA cursor can come back empty rather than
// failing loudly, which would read as "this table has no session index".
func (s *d1Store) d1SessionMigrationHasLeadingIndex(
	ctx context.Context, table, column string,
) (bool, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT index_list.name, index_info.name
		FROM pragma_index_list(?) AS index_list
		JOIN pragma_index_info(index_list.name) AS index_info ON index_info.seqno = 0
		ORDER BY index_list.seq
	`, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var indexName, leading string
		if err := rows.Scan(&indexName, &leading); err != nil {
			return false, err
		}
		if leading == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func d1SessionMigrationAsInt(value any) int {
	switch typed := value.(type) {
	case int64:
		return int(typed)
	case int:
		return typed
	case float64:
		return int(typed)
	default:
		return 0
	}
}

// d1SessionMigrationResume returns the recorded result of a migration that has
// already reached a status the complete endpoint can hand back.
//
// The parity re-validation is not optional here: a repeat call must not report
// success for a target that drifted after the copy, which is the entire reason
// this path exists separately from a fresh copy.
func (s *d1Store) d1SessionMigrationResume(
	ctx context.Context, sourceID, targetID, mode string,
) (*SessionMigrationCompleteResult, error) {
	var migrationID int64
	var status string
	var countsJSON *string
	var chromaCount int
	var lockedAt *time.Time
	err := s.conn.QueryRow(ctx, `
		SELECT sm.id, sm.status, sm.counts_json, sm.chroma_reindexed_count, sm.locked_at
		FROM session_migrations sm
		WHERE sm.source_session_id = ? AND sm.target_session_id = ? AND sm.mode = ?
		  AND sm.status IN ('copied', 'vector_reindexed', 'source_locked', 'cleanup_prepared', 'source_cleaned')
		ORDER BY sm.id DESC
		LIMIT 1
	`, sourceID, targetID, mode).Scan(&migrationID, &status, &countsJSON, &chromaCount, &lockedAt)
	if errors.Is(err, errD1NoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rowMapCount, err := s.d1SessionMigrationVerifyResumeParity(ctx, migrationID, status)
	if err != nil {
		return nil, err
	}
	counts := SessionMigrationArtifactCounts{}
	if countsJSON != nil && strings.TrimSpace(*countsJSON) != "" {
		if err := json.Unmarshal([]byte(*countsJSON), &counts); err != nil {
			return nil, fmt.Errorf("resume session migration counts: %w", err)
		}
	}
	return &SessionMigrationCompleteResult{
		MigrationID:           migrationID,
		Status:                status,
		SourceSessionID:       sourceID,
		TargetSessionID:       targetID,
		Mode:                  mode,
		Counts:                counts,
		RowMapCount:           rowMapCount,
		ChromaReindexedCount:  chromaCount,
		SourceLocked:          lockedAt != nil,
		ChromaReindexRequired: status == "copied",
		ReadyForLive: status == "source_locked" || status == "cleanup_prepared" || status == "source_cleaned" ||
			((mode == SessionMigrationModeCopyKeepSource || mode == SessionMigrationModeStitch) && status == "vector_reindexed"),
	}, nil
}

// d1SessionMigrationVerifyResumeParity re-proves the stored migration before a
// repeat call is allowed to report it as complete.
func (s *d1Store) d1SessionMigrationVerifyResumeParity(
	ctx context.Context, migrationID int64, status string,
) (int, error) {
	var total, relationalVerified, expectedRowMaps int
	if err := s.conn.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(
				CASE WHEN parity_state LIKE 'verified_%'
				 AND COALESCE(source_row_count, -1) >= 0
				 AND source_content_hash IS NOT NULL
				 AND COALESCE(target_row_count, -1) >= 0
				 AND target_content_hash IS NOT NULL
				 AND COALESCE(row_map_expected_count, 0) = COALESCE(row_map_verified_count, 0)
				 AND COALESCE(fk_expected_count, 0) = COALESCE(fk_verified_count, 0)
				THEN 1 ELSE 0 END
			), 0),
			COALESCE(SUM(row_map_expected_count), 0)
		FROM session_migration_artifact_parity
		WHERE migration_id = ? AND manifest_version = ?
	`, migrationID, SessionMigrationManifestVersion).Scan(&total, &relationalVerified, &expectedRowMaps); err != nil {
		return 0, err
	}
	expectedEntries := len(SessionMigrationManifest())
	if total != expectedEntries || relationalVerified != expectedEntries {
		return 0, fmt.Errorf(
			"session migration resume blocked: current manifest relational proof %d/%d (rows %d/%d)",
			relationalVerified, expectedEntries, total, expectedEntries,
		)
	}
	var actualRowMaps int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM session_migration_artifact_row_map
		WHERE migration_id = ? AND row_status = 'copied'
	`, migrationID).Scan(&actualRowMaps); err != nil {
		return 0, err
	}
	if actualRowMaps != expectedRowMaps {
		return 0, fmt.Errorf("session migration resume blocked: primary row-map proof %d/%d", actualRowMaps, expectedRowMaps)
	}
	if err := s.d1SessionMigrationVerifyExpectedVectorLedger(ctx, migrationID); err != nil {
		return 0, fmt.Errorf("session migration resume blocked: %w", err)
	}
	currentRelationalHash, err := s.d1SessionMigrationRevalidateCurrentRelationalState(
		ctx, migrationID, "resume", status != "copied")
	if err != nil {
		return 0, err
	}
	if status != "copied" {
		if err := s.d1SessionMigrationVerifyDurableParity(ctx, migrationID); err != nil {
			return 0, fmt.Errorf("session migration resume blocked: %w", err)
		}
		if err := s.d1SessionMigrationConsumeCurrentStateProof(
			ctx, migrationID, SessionMigrationProofOperationResume, currentRelationalHash); err != nil {
			return 0, err
		}
	}
	return actualRowMaps, nil
}

// d1SessionMigrationVerifyExpectedVectorLedger proves the expected-ID ledger
// still agrees with the per-table vector proof.
//
// It recomputes each table's hash from the ledger rows and compares it with the
// hash stored on the parity row. A drift here means the ledger was edited or
// partly lost, and every later gate would then be comparing against a set that
// no longer describes the copy, so it is reported rather than repaired.
func (s *d1Store) d1SessionMigrationVerifyExpectedVectorLedger(
	ctx context.Context, migrationID int64,
) error {
	ledgerRows, err := s.conn.Query(ctx, `
		SELECT source_table, document_id
		FROM session_migration_vector_expected_ids
		WHERE migration_id = ?
		ORDER BY source_table, document_id
	`, migrationID)
	if err != nil {
		return err
	}
	expectedByTable := map[string][]string{}
	for ledgerRows.Next() {
		var table, documentID string
		if err := ledgerRows.Scan(&table, &documentID); err != nil {
			ledgerRows.Close()
			return err
		}
		expectedByTable[table] = append(expectedByTable[table], documentID)
	}
	err = ledgerRows.Err()
	ledgerRows.Close()
	if err != nil {
		return err
	}

	parityRows, err := s.conn.Query(ctx, `
		SELECT table_name, COALESCE(vector_expected_count, 0), COALESCE(vector_expected_id_hash, '')
		FROM session_migration_artifact_parity
		WHERE migration_id = ? AND manifest_version = ?
		ORDER BY table_name
	`, migrationID, SessionMigrationManifestVersion)
	if err != nil {
		return err
	}
	defer parityRows.Close()
	seen := 0
	for parityRows.Next() {
		var table, expectedHash string
		var expectedCount int
		if err := parityRows.Scan(&table, &expectedCount, &expectedHash); err != nil {
			return err
		}
		seen++
		ids := expectedByTable[table]
		actualHash := ""
		if len(ids) > 0 {
			actualHash = sessionMigrationHashIDs(ids)
		}
		if len(ids) != expectedCount || actualHash != expectedHash {
			return fmt.Errorf("vector expected-ID ledger mismatch for %s: ids=%d/%s parity=%d/%s",
				table, len(ids), actualHash, expectedCount, expectedHash)
		}
		delete(expectedByTable, table)
	}
	if err := parityRows.Err(); err != nil {
		return err
	}
	if seen != len(SessionMigrationManifest()) || len(expectedByTable) != 0 {
		return fmt.Errorf("vector expected-ID ledger manifest coverage mismatch: parity=%d/%d orphan_tables=%d",
			seen, len(SessionMigrationManifest()), len(expectedByTable))
	}
	return nil
}

// d1SessionMigrationRevalidateCurrentRelationalState recomputes every manifest
// table's canonical hash and compares it with the stored proof.
//
// This is the gate that makes a destructive phase safe to run against a target
// that has been live since the copy. lockRanges asks the reference to hold the
// read ranges; D1 has no range locks, so the flag is accepted and the guarantee
// is instead obtained from the affected-row proof that each destructive phase
// performs afterwards. Dropping it silently would be a lie, so it is named here.
func (s *d1Store) d1SessionMigrationRevalidateCurrentRelationalState(
	ctx context.Context, migrationID int64, phase string, lockRanges bool,
) (string, error) {
	_ = lockRanges
	sourceID, targetID, status, mode, _, _, err := s.d1SessionMigrationReadMigration(ctx, migrationID)
	if err != nil {
		return "", err
	}
	rows, err := s.conn.Query(ctx, `
		SELECT table_name, source_row_count, source_content_hash,
		       target_row_count, target_content_hash
		FROM session_migration_artifact_parity
		WHERE migration_id = ? AND manifest_version = ?
		ORDER BY table_name
	`, migrationID, SessionMigrationManifestVersion)
	if err != nil {
		return "", err
	}
	stored := map[string]sessionMigrationStoredArtifactParity{}
	for rows.Next() {
		var table string
		var parity sessionMigrationStoredArtifactParity
		if err := rows.Scan(&table, &parity.SourceCount, &parity.SourceHash,
			&parity.TargetCount, &parity.TargetHash); err != nil {
			rows.Close()
			return "", err
		}
		stored[table] = parity
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}

	manifest := SessionMigrationManifest()
	if len(stored) != len(manifest) {
		return "", sessionMigrationBlocker("current_manifest_parity_incomplete", phase, "")
	}
	keyMaps, err := s.d1SessionMigrationLoadKeyMaps(ctx, migrationID)
	if err != nil {
		return "", err
	}
	fingerprint := []string{SessionMigrationManifestVersion, sourceID, targetID}
	sourceWasCleaned := status == "source_cleaned"
	allowLiveTargetAdditions := status == "source_locked" ||
		status == "cleanup_prepared" || status == "source_cleaned"
	for _, entry := range manifest {
		plan, ok := SessionMigrationExecutionPlanFor(entry.Table)
		if !ok {
			return "", sessionMigrationBlocker("current_manifest_plan_missing", phase, entry.Table)
		}
		if mode == SessionMigrationModeStitch {
			// A stitch's source is the ordered snapshot validated when it was
			// created, not the editable originals. Only the stored target snapshot
			// is re-proven here, including its remapped reference values.
			targetRows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, targetID)
			if err != nil {
				return "", err
			}
			hash := sessionMigrationCanonicalRowsHash(entry, plan, targetRows, targetID, true, keyMaps)
			want := stored[entry.Table]
			if len(targetRows) != want.TargetCount || hash != want.TargetHash {
				return "", sessionMigrationBlocker("current_target_snapshot_drift", phase, entry.Table)
			}
			fingerprint = append(fingerprint, entry.Table,
				strconv.Itoa(want.SourceCount), want.SourceHash, strconv.Itoa(len(targetRows)), hash)
			continue
		}
		sourceRows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, sourceID)
		if err != nil {
			return "", err
		}
		targetRows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, targetID)
		if err != nil {
			return "", err
		}
		if allowLiveTargetAdditions {
			targetRows = sessionMigrationOwnedTargetRows(entry, plan, targetRows, keyMaps)
		}
		sourceHash := sessionMigrationCanonicalRowsHash(entry, plan, sourceRows, sourceID, false, keyMaps)
		targetHash := sessionMigrationCanonicalRowsHash(entry, plan, targetRows, targetID, true, keyMaps)
		want := stored[entry.Table]
		if sourceWasCleaned {
			shouldRemain := entry.Policy == SessionMigrationPolicyRetainAudit
			if shouldRemain {
				if len(sourceRows) != want.SourceCount || sourceHash != want.SourceHash {
					return "", sessionMigrationBlocker("current_source_snapshot_drift", phase, entry.Table)
				}
			} else if len(sourceRows) != 0 {
				return "", sessionMigrationBlocker("current_source_cleanup_incomplete", phase, entry.Table)
			}
		} else if len(sourceRows) != want.SourceCount || sourceHash != want.SourceHash {
			return "", sessionMigrationBlocker("current_source_snapshot_drift", phase, entry.Table)
		}
		if len(targetRows) != want.TargetCount || targetHash != want.TargetHash {
			return "", sessionMigrationBlocker("current_target_snapshot_drift", phase, entry.Table)
		}
		if !sourceWasCleaned {
			if _, err := sessionMigrationEvaluateArtifactParity(
				entry, plan, sourceRows, targetRows, sourceHash, targetHash, keyMaps); err != nil {
				return "", sessionMigrationBlocker("current_relational_reference_drift", phase, entry.Table)
			}
		}
		fingerprint = append(fingerprint, entry.Table,
			strconv.Itoa(len(sourceRows)), sourceHash, strconv.Itoa(len(targetRows)), targetHash)
	}
	return sessionMigrationStringHash(fingerprint...), nil
}

// d1SessionMigrationVerifyDurableParity is the gate in front of every
// destructive phase: source lock and source cleanup both refuse to run unless the
// stored proof, the row map, the exact vector set, and the vector saga step all
// agree.
func (s *d1Store) d1SessionMigrationVerifyDurableParity(ctx context.Context, migrationID int64) error {
	var total, relationalVerified, vectorVerified int
	if err := s.conn.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(
				CASE
					WHEN parity_state IN (
						'verified_copy', 'verified_retain_audit',
						'verified_regenerate', 'verified_delete_pending'
					)
					 AND source_row_count IS NOT NULL
					 AND source_content_hash IS NOT NULL
					 AND target_row_count IS NOT NULL
					 AND target_content_hash IS NOT NULL
					 AND COALESCE(row_map_expected_count, 0) = COALESCE(row_map_verified_count, 0)
					 AND COALESCE(fk_expected_count, 0) = COALESCE(fk_verified_count, 0)
					THEN 1 ELSE 0
				END
			), 0),
			COALESCE(SUM(
				CASE
					WHEN COALESCE(vector_expected_count, 0) = COALESCE(vector_actual_count, 0)
					 AND (
						COALESCE(vector_expected_count, 0) = 0
						OR vector_expected_id_hash = vector_actual_id_hash
					 )
					THEN 1 ELSE 0
				END
			), 0)
		FROM session_migration_artifact_parity
		WHERE migration_id = ? AND manifest_version = ?
	`, migrationID, SessionMigrationManifestVersion).Scan(&total, &relationalVerified, &vectorVerified); err != nil {
		return err
	}
	direct, indirect, implemented := SessionMigrationManifestSummary()
	expectedEntries := direct + indirect
	if implemented != expectedEntries || total != expectedEntries {
		return fmt.Errorf("session migration source lock blocked: manifest parity rows %d/%d", total, expectedEntries)
	}
	if relationalVerified != expectedEntries {
		return fmt.Errorf("session migration source lock blocked: relational count/hash/row-map/FK parity %d/%d",
			relationalVerified, expectedEntries)
	}
	if vectorVerified != expectedEntries {
		return fmt.Errorf("session migration source lock blocked: vector artifact parity %d/%d", vectorVerified, expectedEntries)
	}
	var expectedVectors, observedVectors int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN observed = 1 THEN 1 ELSE 0 END), 0)
		FROM session_migration_vector_expected_ids
		WHERE migration_id = ?
	`, migrationID).Scan(&expectedVectors, &observedVectors); err != nil {
		return err
	}
	if expectedVectors != observedVectors {
		return fmt.Errorf("session migration source lock blocked: exact vector expected-ID parity %d/%d",
			observedVectors, expectedVectors)
	}
	var completedSaga int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM session_migration_saga_steps
		WHERE migration_id = ? AND phase = 'vector_exact_id_parity' AND phase_state = 'completed'
	`, migrationID).Scan(&completedSaga); err != nil {
		return err
	}
	if completedSaga != 1 {
		return errors.New("session migration source lock blocked: vector exact-ID saga is not completed")
	}
	return nil
}

// d1SessionMigrationValidateProofOperation refuses an exact-vector proof taken at
// a phase that has not been reached yet.
//
// A proof is only meaningful against the state it was measured against, so the
// status allowlist is the whole check: a caller cannot manufacture a source-lock
// proof for a migration whose vectors were never verified.
func (s *d1Store) d1SessionMigrationValidateProofOperation(
	ctx context.Context, migrationID int64, operation string,
) (string, error) {
	var status string
	if err := s.conn.QueryRow(ctx, `SELECT status FROM session_migrations WHERE id = ?`, migrationID).Scan(&status); err != nil {
		if errors.Is(err, errD1NoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	allowed := false
	switch operation {
	case SessionMigrationProofOperationSourceLock:
		allowed = status == "copied" || status == "vector_reindexed" || status == "source_locked"
	case SessionMigrationProofOperationCleanupPrepare:
		allowed = status == "source_locked"
	case SessionMigrationProofOperationCleanupFinalize:
		allowed = status == "cleanup_prepared"
	case SessionMigrationProofOperationResume:
		allowed = status == "vector_reindexed" || status == "source_locked" ||
			status == "cleanup_prepared" || status == "source_cleaned"
	}
	if !allowed {
		return status, sessionMigrationBlocker("current_vector_proof_phase_mismatch", operation, "")
	}
	return status, nil
}

// d1SessionMigrationConsumeCurrentStateProof spends the proof produced by the
// immediately preceding verification.
//
// The guard is the relational hash the proof was recorded against, so a proof
// measured against a state that has since changed cannot be spent. The affected
// count must be exactly one: zero means no proof was ever recorded, and more than
// one would mean the phase key is no longer unique.
func (s *d1Store) d1SessionMigrationConsumeCurrentStateProof(
	ctx context.Context, migrationID int64, operation, relationalHash string,
) error {
	phase, ok := sessionMigrationCurrentProofPhase(operation)
	if !ok {
		return sessionMigrationBlocker("current_vector_proof_operation_invalid", operation, "")
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE session_migration_saga_steps
		SET phase_state = 'consumed',
		    result_json = json_set(COALESCE(result_json, '{}'), '$.consumed', json('true')),
		    updated_at = `+d1NowExpression+`
		WHERE migration_id = ?
		  AND phase = ?
		  AND phase_state = 'completed'
		  AND json_extract(result_json, '$.relational_hash') = ?
	`, migrationID, phase, relationalHash)
	if err != nil {
		return err
	}
	if affected != 1 {
		return sessionMigrationBlocker("current_vector_snapshot_required", operation, "")
	}
	return nil
}

// ---------------------------------------------------------------------------
// SessionMigrationVectorStore
// ---------------------------------------------------------------------------

// ListSessionMigrationVectorDocuments returns the copied target rows that still
// need a vector index entry.
//
// The join is deliberately through the row map rather than through the document
// id: the id contains the target row id the engine assigned during the copy,
// which is only recoverable from the map. Rows whose mapping was rolled back are
// excluded, so a reindex after a rollback cannot resurrect a deleted document.
func (s *d1Store) ListSessionMigrationVectorDocuments(
	ctx context.Context, migrationID int64,
) ([]SessionMigrationVectorDocument, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	docs := []SessionMigrationVectorDocument{}
	for _, entry := range SessionMigrationManifest() {
		plan, ok := SessionMigrationExecutionPlanFor(entry.Table)
		if !ok || plan.Vector == nil || len(plan.PrimaryKey) != 1 {
			continue
		}
		tableDocs, err := s.d1SessionMigrationVectorDocumentsForPlan(ctx, migrationID, entry.Table, plan)
		if err != nil {
			return nil, err
		}
		docs = append(docs, tableDocs...)
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	return docs, nil
}

func (s *d1Store) d1SessionMigrationVectorDocumentsForPlan(
	ctx context.Context, migrationID int64, table string, plan SessionMigrationExecutionPlan,
) ([]SessionMigrationVectorDocument, error) {
	vectorPlan := plan.Vector
	textColumns := make([]string, 0, len(vectorPlan.TextColumns))
	for _, column := range vectorPlan.TextColumns {
		textColumns = append(textColumns, "t."+d1QuoteIdent(column))
	}
	contextColumns := make([]string, 0, len(vectorPlan.ContextTurnColumns))
	for _, column := range vectorPlan.ContextTurnColumns {
		contextColumns = append(contextColumns, "t."+d1QuoteIdent(column))
	}
	embeddingSelect := "''"
	if vectorPlan.EmbeddingColumn != "" {
		embeddingSelect = "COALESCE(t." + d1QuoteIdent(vectorPlan.EmbeddingColumn) + ", '')"
	}
	// The reference compares the row key through a CAST so a numeric key can meet
	// a TEXT ledger value. The same CAST is used here: without it SQLite would
	// compare an INTEGER column against a TEXT bound value and never match.
	query := `
		SELECT ve.document_id, sm.source_session_id, sm.target_session_id,
		       arm.target_key, ` + embeddingSelect
	if len(textColumns) > 0 {
		query += "," + strings.Join(textColumns, ",")
	}
	if len(contextColumns) > 0 {
		query += "," + strings.Join(contextColumns, ",")
	}
	query += `
		FROM session_migration_vector_expected_ids ve
		JOIN session_migrations sm ON sm.id = ve.migration_id
		JOIN session_migration_artifact_row_map arm
		  ON arm.migration_id = ve.migration_id
		 AND arm.table_name = ve.source_table
		 AND arm.key_column_name = ?
		 AND arm.source_key = ve.source_row_id
		 AND arm.row_status <> 'rolled_back'
		JOIN ` + d1QuoteIdent(table) + ` t
		  ON CAST(t.` + d1QuoteIdent(vectorPlan.IDColumn) + ` AS TEXT) = arm.target_key
		WHERE ve.migration_id = ? AND ve.source_table = ?
		ORDER BY ve.document_id`
	rows, err := s.conn.Query(ctx, query, vectorPlan.IDColumn, migrationID, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	docs := []SessionMigrationVectorDocument{}
	for rows.Next() {
		var id, sourceSessionID, targetSessionID, targetKey, embedding string
		// The text and context-turn columns are read as untyped values. The
		// context turn columns are integers in this schema and the text columns
		// are nullable, and a driver-specific rendering of either would change the
		// document text or the context turn a reindexer is handed. Normalising
		// through the shared helper keeps both providers producing the same
		// document from the same row.
		textValues := make([]any, len(vectorPlan.TextColumns))
		contextValues := make([]any, len(vectorPlan.ContextTurnColumns))
		dest := []any{&id, &sourceSessionID, &targetSessionID, &targetKey, &embedding}
		for index := range textValues {
			dest = append(dest, &textValues[index])
		}
		for index := range contextValues {
			dest = append(dest, &contextValues[index])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		vectorRow := sessionMigrationRow{Values: map[string]sessionMigrationCell{}}
		for index, value := range textValues {
			cell, err := sessionMigrationNormalizeDatabaseValue(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", table, vectorPlan.TextColumns[index], err)
			}
			vectorRow.Values[vectorPlan.TextColumns[index]] = cell
		}
		for index, value := range contextValues {
			cell, err := sessionMigrationNormalizeDatabaseValue(value)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", table, vectorPlan.ContextTurnColumns[index], err)
			}
			vectorRow.Values[vectorPlan.ContextTurnColumns[index]] = cell
		}
		contextTurnIndex, contextTurnKnown := sessionMigrationVectorContextTurn(vectorPlan, vectorRow)
		docs = append(docs, SessionMigrationVectorDocument{
			ID:                    id,
			MigrationID:           migrationID,
			Tier:                  vectorPlan.Tier,
			ChatSessionID:         targetSessionID,
			ContextTurnIndex:      contextTurnIndex,
			ContextTurnKnown:      contextTurnKnown,
			SourceTable:           table,
			SourceRowID:           targetKey,
			SchemaVersion:         vectorPlan.SchemaVersion,
			DocumentText:          sessionMigrationVectorDocumentText(vectorPlan, vectorRow),
			EmbeddingJSON:         embedding,
			MigratedFromSessionID: sourceSessionID,
		})
	}
	return docs, rows.Err()
}

// UpdateSessionMigrationVectorStatus records the vector reindex outcome in the
// migration ledger.
func (s *d1Store) UpdateSessionMigrationVectorStatus(
	ctx context.Context, migrationID int64, status string, reindexedCount int, errorsJSON string,
) error {
	if migrationID <= 0 {
		return ErrNotFound
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = "vector_reindexed"
	}
	if strings.TrimSpace(errorsJSON) == "" {
		errorsJSON = "[]"
	}
	_, err := s.conn.Exec(ctx, `
		UPDATE session_migrations
		SET status = ?,
		    chroma_reindexed_count = ?,
		    errors_json = ?,
		    updated_at = `+d1NowExpression+`
		WHERE id = ?
	`, status, reindexedCount, errorsJSON, migrationID)
	return err
}

// ---------------------------------------------------------------------------
// SessionMigrationVectorParityStore
// ---------------------------------------------------------------------------

// GetSessionMigrationVectorParityContext returns the exact document set the
// vector index is expected to hold.
//
// A rolled back migration has no expected set to compare against, so it is
// reported as absent rather than as an empty parity that would trivially verify.
func (s *d1Store) GetSessionMigrationVectorParityContext(
	ctx context.Context, migrationID int64,
) (*SessionMigrationVectorParityContext, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	var targetSessionID string
	err := s.conn.QueryRow(ctx, `
		SELECT target_session_id
		FROM session_migrations
		WHERE id = ? AND status NOT IN `+d1SessionMigrationRevertedStatuses,
		migrationID).Scan(&targetSessionID)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	expected, err := s.d1SessionMigrationExpectedDocumentIDs(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	return &SessionMigrationVectorParityContext{
		MigrationID:     migrationID,
		TargetSessionID: targetSessionID,
		ExpectedIDs:     expected,
	}, nil
}

// d1SessionMigrationExpectedDocumentIDs reads the ledger in document-id order.
func (s *d1Store) d1SessionMigrationExpectedDocumentIDs(
	ctx context.Context, migrationID int64,
) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT document_id
		FROM session_migration_vector_expected_ids
		WHERE migration_id = ?
		ORDER BY document_id
	`, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	expected := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		expected = append(expected, id)
	}
	return expected, rows.Err()
}

// VerifySessionMigrationVectorParity compares the vector index against the
// canonical copy and records the outcome.
//
// This is a count-sensitive operation and the counts must never be allowed to
// under-report. A missing vector that is not detected here becomes a memory that
// recall can never find, and the downstream gates (source lock, cleanup) consume
// the recorded result rather than recomputing it, so an under-reported count
// would let a destructive phase run against an incomplete index. Every side of
// the comparison therefore comes from the same ledger rows: the expected set is
// read from session_migration_vector_expected_ids, and the per-table actual
// counts written back into the parity rows are recomputed from those same ledger
// rows filtered by the observed document set, not from a separate query whose
// scope could differ.
func (s *d1Store) VerifySessionMigrationVectorParity(
	ctx context.Context, migrationID int64, operation string, actualIDs []string,
) (*SessionMigrationVectorParityResult, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	var targetSessionID string
	err := s.conn.QueryRow(ctx, `
		SELECT target_session_id
		FROM session_migrations
		WHERE id = ? AND status NOT IN `+d1SessionMigrationRevertedStatuses,
		migrationID).Scan(&targetSessionID)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	expectedIDs, err := s.d1SessionMigrationExpectedDocumentIDs(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	migrationStatus, err := s.d1SessionMigrationValidateProofOperation(ctx, migrationID, operation)
	if err != nil {
		return nil, err
	}
	allowUnexpected := operation == SessionMigrationProofOperationCleanupPrepare ||
		operation == SessionMigrationProofOperationCleanupFinalize ||
		(operation == SessionMigrationProofOperationResume &&
			(migrationStatus == "source_locked" || migrationStatus == "cleanup_prepared" ||
				migrationStatus == "source_cleaned"))
	result := sessionMigrationCompareVectorIDsWithPolicy(
		migrationID, targetSessionID, expectedIDs, actualIDs, allowUnexpected)
	expected := result.ExpectedIDs
	actual := result.ActualIDs
	missing := result.MissingIDs
	unexpected := result.UnexpectedIDs
	expectedSet := make(map[string]bool, len(expected))
	actualSet := make(map[string]bool, len(actual))
	for _, id := range expected {
		expectedSet[id] = true
	}
	for _, id := range actual {
		actualSet[id] = true
	}
	relationalHash, err := s.d1SessionMigrationRevalidateCurrentRelationalState(
		ctx, migrationID, "vector_verify", false)
	if err != nil {
		return result, err
	}
	if err := s.d1SessionMigrationVerifyExpectedVectorLedger(ctx, migrationID); err != nil {
		return result, sessionMigrationBlocker("current_vector_ledger_drift", "vector_verify", "")
	}
	if !result.Verified {
		// A failed verification still returns the full comparison so the caller
		// can see which documents are missing, but it writes no observation: a
		// partially observed set is what the source-lock gate consumes, and
		// recording one here would let a later gate mistake this attempt for a
		// completed proof.
		return result, sessionMigrationBlocker("current_vector_id_drift", "vector_verify", "")
	}

	statements := []D1Statement{{
		SQL: `
		UPDATE session_migration_vector_expected_ids
		SET observed = 0, observed_at = NULL
		WHERE migration_id = ?`,
		Args: []any{migrationID},
	}}
	for _, id := range actual {
		if !expectedSet[id] {
			continue
		}
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE session_migration_vector_expected_ids
			SET observed = 1, observed_at = ` + d1NowExpression + `
			WHERE migration_id = ? AND document_id = ?`,
			Args: []any{migrationID, id},
		})
	}
	// The per-table actual counts are recomputed from the same ledger rows the
	// expected ids came from, filtered by the observed set, so the two sides of
	// the parity row can never be computed over different rows.
	byTable, err := s.d1SessionMigrationObservedIDsByTable(ctx, migrationID, actualSet)
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0, len(byTable))
	for table := range byTable {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		ids := byTable[table]
		sort.Strings(ids)
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE session_migration_artifact_parity
			SET vector_actual_count = ?,
			    vector_actual_id_hash = ?,
			    updated_at = ` + d1NowExpression + `
			WHERE migration_id = ? AND manifest_version = ? AND table_name = ?`,
			Args: []any{len(ids), sessionMigrationHashIDs(ids), migrationID,
				SessionMigrationManifestVersion, table},
		})
	}
	status := "vector_reindexed"
	state := "completed"
	lastError := ""
	errorPayload, _ := json.Marshal(map[string]any{
		"missing_ids": missing, "unexpected_ids": unexpected,
	})
	statements = append(statements, D1Statement{
		SQL: `
		UPDATE session_migrations
		SET status = CASE
		        WHEN status IN ('copied', 'vector_reindex_failed', 'vector_reindex_unverified')
		        THEN ?
		        ELSE status
		    END,
		    chroma_reindexed_count = ?,
		    errors_json = ?,
		    updated_at = ` + d1NowExpression + `
		WHERE id = ?`,
		Args: []any{status, len(actual), string(errorPayload), migrationID},
	})
	statements = append(statements, d1SessionMigrationSagaStatement(
		migrationID, "vector_exact_id_parity", state, result.ExpectedIDHash, string(errorPayload), lastError))
	statements = append(statements, d1SessionMigrationPersistCurrentStateProof(
		migrationID, operation, relationalHash, result.ActualIDHash))
	if err := s.d1SessionMigrationRun(ctx, statements); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *d1Store) d1SessionMigrationObservedIDsByTable(
	ctx context.Context, migrationID int64, actualSet map[string]bool,
) (map[string][]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT source_table, document_id
		FROM session_migration_vector_expected_ids
		WHERE migration_id = ?
		ORDER BY source_table, document_id
	`, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byTable := map[string][]string{}
	for rows.Next() {
		var table, id string
		if err := rows.Scan(&table, &id); err != nil {
			return nil, err
		}
		if actualSet[id] {
			byTable[table] = append(byTable[table], id)
		}
	}
	return byTable, rows.Err()
}

// d1SessionMigrationPersistCurrentStateProof records the proof a destructive
// phase is about to spend, so that phase can prove it acted on a state it actually
// measured.
func d1SessionMigrationPersistCurrentStateProof(
	migrationID int64, operation, relationalHash, vectorHash string,
) D1Statement {
	phase, ok := sessionMigrationCurrentProofPhase(operation)
	if !ok {
		// The caller validated the operation against the status allowlist before
		// reaching here, so an unknown operation is unreachable; the phase is
		// still named so a bad value would be visible in the ledger rather than
		// silently folded into another phase.
		phase = operation
	}
	result := fmt.Sprintf(`{"relational_hash":%q,"vector_id_hash":%q}`, relationalHash, vectorHash)
	return d1SessionMigrationSagaStatement(
		migrationID, phase, "completed", sessionMigrationStringHash(relationalHash, vectorHash), result, "")
}

// ---------------------------------------------------------------------------
// source lock and its fence
// ---------------------------------------------------------------------------

// d1SessionMigrationActiveLock is the active lock on a source session.
//
// MariaDB adds FOR UPDATE to the in-transaction form so a concurrent Prepare
// cannot read a lock that is about to change. D1 has no row lock, so the same
// exclusion is obtained by making every writer prove its claim through a guarded
// UPDATE: the caller re-reads the lock afterwards and treats anything but its own
// claim as a blocker. A lock that vanished between the read and the claim is
// caught by the claim affecting zero rows.
const d1SessionMigrationActiveLockSelect = `
	SELECT migration_id, source_session_id, target_session_id, locked, lock_status,
	       COALESCE(reason, ''), locked_at
	FROM session_migration_locks
	WHERE source_session_id = ? AND locked = 1 AND unlocked_at IS NULL
	ORDER BY locked_at DESC, id DESC
	LIMIT 1`

func (s *d1Store) d1SessionMigrationActiveLock(
	ctx context.Context, sourceSessionID string,
) (*SessionMigrationLock, error) {
	lock := &SessionMigrationLock{}
	err := s.conn.QueryRow(ctx, d1SessionMigrationActiveLockSelect, sourceSessionID).Scan(
		&lock.MigrationID, &lock.SourceSessionID, &lock.TargetSessionID, &lock.Locked,
		&lock.LockStatus, &lock.Reason, &lock.LockedAt)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PrepareSessionMigrationSourceLock claims the durable fence that stops a live
// turn from writing into a session that is being drained.
//
// This is the claim the whole family turns on. MariaDB inserts the lock row and
// holds it with FOR UPDATE for the rest of the transaction; here the claim is a
// guarded INSERT that only succeeds when no other migration holds an active lock
// on the source, and a second claim for the same migration is idempotent rather
// than a duplicate. The status is written as lock_pending_verification rather than
// migrated_away on purpose: routing refuses to follow a pending lock, so a
// migration that dies here leaves the source closed but still reversible by
// ReleaseSessionMigrationSourceLockFence, instead of stranding the session.
func (s *d1Store) PrepareSessionMigrationSourceLock(
	ctx context.Context, migrationID int64, reason string,
) (*SessionMigrationLock, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	sourceID, targetID, mode, status, _, _, err := s.d1SessionMigrationReadMigration(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	if mode != SessionMigrationModeCopyThenLockSource {
		return nil, sessionMigrationBlocker("source_lock_mode_does_not_lock", "source_lock_prepare", "")
	}
	if status != "vector_reindexed" && status != "source_locked" {
		return nil, sessionMigrationBlocker("source_lock_phase_mismatch", "source_lock_prepare", "")
	}
	lock, err := s.d1SessionMigrationActiveLock(ctx, sourceID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if lock != nil && lock.MigrationID != migrationID {
		return nil, sessionMigrationBlocker("source_session_locked_by_other_migration", "source_lock_prepare", "")
	}
	trimmedReason := strings.TrimSpace(reason)
	if lock == nil {
		// The guard inside the statement is the whole compare-and-swap: a lock
		// that appeared between the read above and this write makes the INSERT
		// match nothing, and the claim is then reported as owned rather than
		// silently duplicated.
		affected, err := s.conn.Exec(ctx, `
			INSERT INTO session_migration_locks (
				migration_id, source_session_id, target_session_id, locked, lock_status, reason
			)
			SELECT ?, ?, ?, 1, 'lock_pending_verification', ?
			WHERE NOT EXISTS (
				SELECT 1 FROM session_migration_locks
				WHERE source_session_id = ? AND locked = 1 AND unlocked_at IS NULL
			)`, migrationID, sourceID, targetID, d1NullableString(trimmedReason), sourceID)
		if err != nil {
			return nil, err
		}
		if affected == 0 {
			return nil, sessionMigrationBlocker("source_session_locked_by_other_migration", "source_lock_prepare", "")
		}
		lock, err = s.d1SessionMigrationActiveLock(ctx, sourceID)
		if err != nil {
			return nil, err
		}
	}
	if lock.LockStatus != "migrated_away" {
		affected, err := s.conn.Exec(ctx, `
			UPDATE session_migration_locks
			SET lock_status = 'lock_pending_verification',
			    reason = ?,
			    updated_at = `+d1NowExpression+`
			WHERE migration_id = ? AND source_session_id = ?
			  AND locked = 1 AND unlocked_at IS NULL
		`, d1NullableString(trimmedReason), migrationID, sourceID)
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, sessionMigrationBlocker("source_lock_fence_claim_lost", "source_lock_prepare", "")
		}
		lock.LockStatus = "lock_pending_verification"
		lock.Reason = trimmedReason
	}
	if err := s.conn.Batch(ctx, d1SessionMigrationSagaStatement(
		migrationID, "source_lock_fence", "prepared",
		sessionMigrationStringHash(sourceID, targetID, reason),
		fmt.Sprintf(`{"source_session_id":%q,"lock_status":%q}`, sourceID, lock.LockStatus), "")); err != nil {
		return nil, err
	}
	return lock, nil
}

// ReleaseSessionMigrationSourceLockFence undoes a fence claim whose verification
// failed.
//
// The guard restricts the release to a row still in lock_pending_verification.
// Without that guard a late release could unlock a source that verification had
// already promoted to migrated_away, which is precisely the state a resumed live
// session must not find. A release that matches nothing is not an error: the fence
// was never claimed, or was already released, and both leave the source unlocked.
func (s *d1Store) ReleaseSessionMigrationSourceLockFence(
	ctx context.Context, migrationID int64, reason string,
) error {
	if migrationID <= 0 {
		return ErrNotFound
	}
	trimmedReason := strings.TrimSpace(reason)
	affected, err := s.conn.Exec(ctx, `
		UPDATE session_migration_locks
		SET locked = 0,
		    lock_status = 'lock_verification_failed',
		    reason = ?,
		    unlocked_at = `+d1NowExpression+`,
		    updated_at = `+d1NowExpression+`
		WHERE migration_id = ?
		  AND locked = 1
		  AND unlocked_at IS NULL
		  AND lock_status = 'lock_pending_verification'
	`, d1NullableString(trimmedReason), migrationID)
	if err != nil {
		return err
	}
	if affected > 1 {
		return sessionMigrationBlocker("source_lock_fence_not_unique", "source_lock_release", "")
	}
	return s.conn.Batch(ctx, d1SessionMigrationSagaStatement(
		migrationID, "source_lock_fence", "failed",
		sessionMigrationStringHash(strconv.FormatInt(migrationID, 10), reason),
		fmt.Sprintf(`{"released":%t}`, affected == 1), trimmedReason))
}

// LockSessionMigrationSource promotes a verified fence into the durable lock that
// retires the source session.
//
// The promotion is the second half of the compare-and-swap. It only runs from
// lock_pending_verification or migrated_away for THIS migration, and it must
// affect exactly one row. A zero count means another actor already moved the row,
// so the migration is reported as blocked rather than as locked: a caller that
// believed it owned the source lock while it did not would keep routing new turns
// onto a session that is being drained.
func (s *d1Store) LockSessionMigrationSource(
	ctx context.Context, migrationID int64, reason string,
) (*SessionMigrationSourceLockResult, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	if blockers := SessionMigrationManifestReleaseBlockers(); len(blockers) > 0 {
		return nil, fmt.Errorf("session migration source lock blocked: %s", SessionMigrationManifestParityUnverifiedReason)
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	sourceID, targetID, mode, status, _, _, err := s.d1SessionMigrationReadMigration(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(mode) != SessionMigrationModeCopyThenLockSource {
		return nil, fmt.Errorf("session migration source lock blocked: migration mode %q does not lock source", mode)
	}
	if status != "vector_reindexed" && status != "source_locked" {
		return nil, fmt.Errorf("session migration source lock blocked: migration status %q is not vector_reindexed", status)
	}
	lock, err := s.d1SessionMigrationActiveLock(ctx, sourceID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if lock != nil && lock.MigrationID != migrationID {
		return nil, fmt.Errorf("session migration source lock blocked: source session is already locked by migration %d", lock.MigrationID)
	}
	if lock == nil {
		return nil, sessionMigrationBlocker("source_lock_fence_required", "source_lock", "")
	}
	if lock.LockStatus != "lock_pending_verification" && lock.LockStatus != "migrated_away" {
		return nil, sessionMigrationBlocker("source_lock_fence_phase_mismatch", "source_lock", "")
	}
	leasePhase, err := s.d1SessionMigrationActiveDerivationLeasePhase(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	if leasePhase != "" {
		return nil, sessionMigrationBlocker("source_derivation_lease_active", leasePhase, "")
	}
	if err := s.d1SessionMigrationVerifyDurableParity(ctx, migrationID); err != nil {
		return nil, err
	}
	currentRelationalHash, err := s.d1SessionMigrationRevalidateCurrentRelationalState(
		ctx, migrationID, "source_lock", true)
	if err != nil {
		return nil, err
	}
	trimmedReason := strings.TrimSpace(reason)
	// The promotion and the proof spend are one atomic unit. If the guard matched
	// no row the whole batch rolls back, so a migration can never record a spent
	// proof against a lock it did not actually take.
	if err := s.conn.Batch(ctx,
		D1Statement{
			SQL: `
			UPDATE session_migration_locks
			SET lock_status = 'migrated_away',
			    reason = ?,
			    updated_at = ` + d1NowExpression + `
			WHERE migration_id = ? AND source_session_id = ?
			  AND locked = 1 AND unlocked_at IS NULL
			  AND lock_status IN ('lock_pending_verification', 'migrated_away')`,
			Args: []any{d1NullableString(trimmedReason), migrationID, sourceID},
		},
		D1Statement{
			SQL: `
			UPDATE session_migration_saga_steps
			SET phase_state = 'consumed',
			    result_json = json_set(COALESCE(result_json, '{}'), '$.consumed', json('true')),
			    updated_at = ` + d1NowExpression + `
			WHERE migration_id = ?
			  AND phase = 'current_state_revalidation_' || ?
			  AND phase_state = 'completed'
			  AND json_extract(result_json, '$.relational_hash') = ?`,
			Args: []any{migrationID, SessionMigrationProofOperationSourceLock, currentRelationalHash},
		},
		D1Statement{
			SQL: `
			UPDATE session_migrations
			SET status = 'source_locked',
			    locked_at = COALESCE(locked_at, ` + d1NowExpression + `),
			    errors_json = '[]',
			    updated_at = ` + d1NowExpression + `
			WHERE id = ?`,
			Args: []any{migrationID},
		},
		d1SessionMigrationSagaStatement(migrationID, "source_lock_fence", "completed",
			sessionMigrationStringHash(sourceID, targetID, reason), `{"lock_status":"migrated_away"}`, ""),
	); err != nil {
		return nil, err
	}
	// The promotion is the claim. A lock that was concurrently released or
	// promoted by another actor would have matched no row, and the affected
	// count is checked here rather than being assumed from a successful Exec.
	claimed := 0
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM session_migration_locks
		WHERE migration_id = ? AND source_session_id = ?
		  AND locked = 1 AND unlocked_at IS NULL AND lock_status = 'migrated_away'
	`, migrationID, sourceID).Scan(&claimed); err != nil {
		return nil, err
	}
	if claimed != 1 {
		return nil, sessionMigrationBlocker("source_lock_fence_claim_lost", "source_lock", "")
	}
	lock.LockStatus = "migrated_away"
	lock.Reason = trimmedReason
	return &SessionMigrationSourceLockResult{
		MigrationID:     migrationID,
		SourceSessionID: sourceID,
		TargetSessionID: targetID,
		Status:          "source_locked",
		Lock:            *lock,
		ReadyForLive:    true,
	}, nil
}

// d1SessionMigrationActiveDerivationLeasePhase reports which durable worker lane
// still holds a lease on the source session.
//
// The reference locks the lease row with FOR UPDATE and blocks the source lock
// while any lease is live. A live derivation lease means a worker is mid-write
// into the session, and locking it away underneath that write is what would
// produce a half-written turn in a session that is no longer a memory owner. The
// lease comparison is TEXT because the canonical schema stores timestamps as
// fixed-width RFC3339 text and orders it chronologically.
func (s *d1Store) d1SessionMigrationActiveDerivationLeasePhase(
	ctx context.Context, sourceSessionID string,
) (string, error) {
	checks := []struct{ phase, table string }{
		{"memory_reprocessing_drain", "memory_reprocessing_jobs"},
		{"memory_vector_drain", "memory_vector_outbox"},
	}
	for _, check := range checks {
		var id int64
		err := s.conn.QueryRow(ctx, `
			SELECT id
			FROM `+d1QuoteIdent(check.table)+`
			WHERE chat_session_id = ?
			  AND status = 'leased'
			  AND lease_until >= `+d1NowExpression+`
			ORDER BY id
			LIMIT 1
		`, sourceSessionID).Scan(&id)
		if errors.Is(err, errD1NoRows) {
			continue
		}
		if err != nil {
			return "", err
		}
		return check.phase, nil
	}
	return "", nil
}

// GetSessionMigrationSourceLock returns the active lock on a source session, or
// ErrNotFound when the session is still a live memory owner.
func (s *d1Store) GetSessionMigrationSourceLock(
	ctx context.Context, sourceSessionID string,
) (*SessionMigrationLock, error) {
	sourceID := strings.TrimSpace(sourceSessionID)
	if sourceID == "" {
		return nil, ErrNotFound
	}
	return s.d1SessionMigrationActiveLock(ctx, sourceID)
}

// ---------------------------------------------------------------------------
// SessionMigrationRecoveryStore
// ---------------------------------------------------------------------------

// RollbackSessionMigration removes the rows one migration copied and reopens the
// source.
//
// It is ledger-scoped on purpose: only target rows the row map records are
// deleted, so an operator who has since added their own rows to the target keeps
// them. It is also idempotent, because an interrupted migration is exactly the
// case this exists for: a migration already in status rolled_back returns the same
// result without touching anything a second time, and a second rollback of a
// migration whose rows are already gone deletes nothing and still reports
// success.
func (s *d1Store) RollbackSessionMigration(
	ctx context.Context, migrationID int64, reason string,
) (*SessionMigrationRollbackResult, error) {
	return s.d1SessionMigrationRollback(ctx, migrationID, reason)
}

func (s *d1Store) d1SessionMigrationRollback(
	ctx context.Context, migrationID int64, reason string,
) (*SessionMigrationRollbackResult, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	sourceID, targetID, status, _, _, _, err := s.d1SessionMigrationReadMigration(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	if status == "rolled_back" {
		return &SessionMigrationRollbackResult{
			MigrationID:     migrationID,
			SourceSessionID: sourceID,
			TargetSessionID: targetID,
			Status:          "rolled_back",
			ReadyForLive:    false,
		}, nil
	}
	if status == "source_cleaned" || status == "cleanup_completed" {
		return nil, fmt.Errorf("session migration rollback blocked: migration status %q has already cleaned the source", status)
	}

	deletedByTable, rowMapCount, statements, err := s.d1SessionMigrationRollbackStatements(
		ctx, migrationID, d1SessionMigrationReverseManifest())
	if err != nil {
		return nil, err
	}
	counts := d1SessionMigrationCountsFromDeleted(deletedByTable)
	trimmedReason := strings.TrimSpace(reason)
	if status == "source_locked" {
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE session_migration_locks
			SET locked = 0,
			    lock_status = 'rolled_back',
			    reason = TRIM(COALESCE(reason, '') || CASE WHEN COALESCE(reason, '') = '' THEN '' ELSE char(10) END || ?),
			    unlocked_at = ` + d1NowExpression + `,
			    updated_at = ` + d1NowExpression + `
			WHERE migration_id = ? AND locked = 1 AND unlocked_at IS NULL`,
			Args: []any{trimmedReason, migrationID},
		})
	}
	statements = append(statements,
		D1Statement{
			SQL: `
			UPDATE session_migration_row_map
			SET row_status = 'rolled_back'
			WHERE migration_id = ? AND row_status <> 'rolled_back'`,
			Args: []any{migrationID},
		},
		D1Statement{
			SQL: `
			UPDATE session_migration_artifact_row_map
			SET row_status = 'rolled_back',
			    updated_at = ` + d1NowExpression + `
			WHERE migration_id = ? AND row_status <> 'rolled_back'`,
			Args: []any{migrationID},
		},
		D1Statement{
			SQL: `
			UPDATE session_migration_reference_binding_map
			SET row_status = 'rolled_back'
			WHERE migration_id = ? AND row_status <> 'rolled_back'`,
			Args: []any{migrationID},
		},
		D1Statement{
			SQL: `
			UPDATE session_migration_vector_expected_ids
			SET observed = 0, observed_at = NULL
			WHERE migration_id = ?`,
			Args: []any{migrationID},
		},
	)
	errorPayload, _ := json.Marshal([]string{"rolled_back", trimmedReason})
	statements = append(statements,
		D1Statement{
			SQL: `
			UPDATE session_migrations
			SET status = 'rolled_back',
			    errors_json = ?,
			    updated_at = ` + d1NowExpression + `
			WHERE id = ?`,
			Args: []any{string(errorPayload), migrationID},
		},
		d1SessionMigrationSagaStatement(migrationID, "rollback", "completed",
			sessionMigrationStringHash(strconv.FormatInt(migrationID, 10), reason),
			fmt.Sprintf(`{"deleted_rows":%d}`, rowMapCount), ""),
	)
	if err := s.d1SessionMigrationRun(ctx, statements); err != nil {
		return nil, err
	}
	return &SessionMigrationRollbackResult{
		MigrationID:     migrationID,
		SourceSessionID: sourceID,
		TargetSessionID: targetID,
		Status:          "rolled_back",
		Counts:          counts,
		RowMapCount:     rowMapCount,
		// The unlock statement is guarded to a live lock, so its effect is read
		// back rather than assumed from the batch succeeding.
		SourceUnlocked: s.d1SessionMigrationLockReleased(ctx, migrationID),
		ReadyForLive:   false,
	}, nil
}

func (s *d1Store) d1SessionMigrationLockReleased(ctx context.Context, migrationID int64) bool {
	var count int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM session_migration_locks
		WHERE migration_id = ? AND (locked = 0 OR unlocked_at IS NOT NULL)
	`, migrationID).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

// d1SessionMigrationReverseManifest returns the manifest in reverse order, which
// is the order the reference deletes and rolls back in: children before parents,
// so a foreign key never dangles mid-sweep.
func d1SessionMigrationReverseManifest() []SessionMigrationManifestEntry {
	manifest := SessionMigrationManifest()
	reversed := make([]SessionMigrationManifestEntry, 0, len(manifest))
	for index := len(manifest) - 1; index >= 0; index-- {
		reversed = append(reversed, manifest[index])
	}
	return reversed
}

// d1SessionMigrationCountsFromDeleted renders the deleted-row map as the legacy
// artifact counts the rollback and cleanup results report.
func d1SessionMigrationCountsFromDeleted(deleted map[string]int) SessionMigrationArtifactCounts {
	counts := SessionMigrationArtifactCounts{
		ChatLogs:                 deleted["chat_logs"],
		EffectiveInputs:          deleted["effective_input_logs"],
		Memories:                 deleted["memories"],
		DirectEvidence:           deleted["direct_evidence_records"],
		KGTriples:                deleted["kg_triples"],
		Episodes:                 deleted["episode_summaries"],
		SubjectiveEntityMemories: deleted["protagonist_entity_memories"],
		ReferenceBindings:        deleted["session_reference_bindings"],
	}
	sessionMigrationFinalizeCounts(&counts)
	return counts
}

// d1SessionMigrationRollbackStatements builds the delete sweep.
//
// The reference deletes in 500-key chunks inside its transaction and compares the
// affected count with the key count. A batch cannot report per-statement counts,
// so the same equality is proved before the delete: every mapped key is counted
// against the live table first, and a shortfall is reported instead of deleting a
// partial set. Deleting fewer rows than the ledger claims is what would make a
// rollback leave orphan target rows behind, so the check is done up front rather
// than inferred afterwards.
func (s *d1Store) d1SessionMigrationRollbackStatements(
	ctx context.Context, migrationID int64, manifest []SessionMigrationManifestEntry,
) (map[string]int, int, []D1Statement, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT DISTINCT table_name
		FROM session_migration_artifact_row_map
		WHERE migration_id = ? AND row_status <> 'rolled_back'
		ORDER BY table_name
	`, migrationID)
	if err != nil {
		return nil, 0, nil, err
	}
	mappedTables := map[string]bool{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			rows.Close()
			return nil, 0, nil, err
		}
		entry, ok := sessionMigrationManifestEntryByTable(table)
		if !ok || (entry.Policy != SessionMigrationPolicyCopy && entry.Table != "memory_vector_outbox") {
			rows.Close()
			return nil, 0, nil, fmt.Errorf("session migration rollback blocked: unsupported mapped table %q", table)
		}
		mappedTables[table] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, 0, nil, err
	}

	deletedByTable := map[string]int{}
	statements := []D1Statement{}
	total := 0
	for _, entry := range manifest {
		if !mappedTables[entry.Table] {
			continue
		}
		plan, _ := SessionMigrationExecutionPlanFor(entry.Table)
		if len(plan.PrimaryKey) != 1 {
			return nil, total, nil, fmt.Errorf("session migration rollback blocked: composite key table %q", entry.Table)
		}
		keys, err := s.d1SessionMigrationMappedTargetKeys(ctx, migrationID, entry.Table, plan.PrimaryKey[0])
		if err != nil {
			return nil, total, nil, err
		}
		if len(keys) == 0 {
			continue
		}
		live, err := s.d1SessionMigrationCountKeys(ctx, entry.Table, plan.PrimaryKey[0], keys)
		if err != nil {
			return nil, total, nil, err
		}
		if live != len(keys) {
			return nil, total, nil, fmt.Errorf("session migration rollback %s deleted %d/%d mapped rows",
				entry.Table, live, len(keys))
		}
		for _, fk := range plan.ForeignKeys {
			if !fk.Deferred || fk.ReferenceTable != entry.Table {
				continue
			}
			statements = append(statements, D1Statement{
				SQL: "UPDATE " + d1QuoteIdent(entry.Table) + " SET " + d1QuoteIdent(fk.Column) +
					" = NULL WHERE " + d1QuoteIdent(fk.Column) + " IN (" +
					d1SessionMigrationPlaceholders(len(keys)) + ")",
				Args: d1SessionMigrationAnyArgs(keys),
			})
		}
		statements = append(statements, D1Statement{
			SQL: "DELETE FROM " + d1QuoteIdent(entry.Table) + " WHERE " +
				d1QuoteIdent(plan.PrimaryKey[0]) + " IN (" +
				d1SessionMigrationPlaceholders(len(keys)) + ")",
			Args: d1SessionMigrationAnyArgs(keys),
		})
		deletedByTable[entry.Table] = live
		total += live
	}
	return deletedByTable, total, statements, nil
}

func (s *d1Store) d1SessionMigrationMappedTargetKeys(
	ctx context.Context, migrationID int64, table, primaryKey string,
) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT target_key
		FROM session_migration_artifact_row_map
		WHERE migration_id = ? AND table_name = ? AND key_column_name = ?
		  AND row_status <> 'rolled_back'
		ORDER BY target_key
	`, migrationID, table, primaryKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// d1SessionMigrationCountKeys counts how many of the mapped keys are still live.
//
// The rollback compares this against the ledger's key count. A shortfall means a
// mapped row was removed by something other than this rollback, and deleting the
// rest would report a cleanup that never covered what the ledger claims, so the
// mismatch is reported instead of deleted around.
func (s *d1Store) d1SessionMigrationCountKeys(
	ctx context.Context, table, primaryKey string, keys []string,
) (int, error) {
	count := 0
	for start := 0; start < len(keys); start += d1SessionMigrationInClauseChunk {
		end := start + d1SessionMigrationInClauseChunk
		if end > len(keys) {
			end = len(keys)
		}
		clause, args := d1SessionMigrationInClause(primaryKey, keys[start:end])
		var chunk int
		if err := s.conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM "+d1QuoteIdent(table)+" WHERE "+clause, args...).Scan(&chunk); err != nil {
			return 0, err
		}
		count += chunk
	}
	return count, nil
}

// PreviewSessionMigrationSourceCleanup reports whether the source may be deleted.
//
// It is a preview, so the durable gates are reported as blocked reasons rather
// than raised: an operator asking what is missing must be told the list, not
// handed the first error.
func (s *d1Store) PreviewSessionMigrationSourceCleanup(
	ctx context.Context, migrationID int64,
) (*SessionMigrationCleanupPreview, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	preview, err := s.d1SessionMigrationPreviewSourceCleanup(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	if err := s.d1SessionMigrationVerifyDurableParity(ctx, migrationID); err != nil {
		preview.BlockedReasons = append(preview.BlockedReasons, err.Error())
		preview.ReadyForCleanup = false
	}
	if _, err := s.d1SessionMigrationRevalidateCurrentRelationalState(
		ctx, migrationID, "cleanup_preview", false); err != nil {
		preview.BlockedReasons = append(preview.BlockedReasons, err.Error())
		preview.ReadyForCleanup = false
	}
	return preview, nil
}

func (s *d1Store) d1SessionMigrationPreviewSourceCleanup(
	ctx context.Context, migrationID int64,
) (*SessionMigrationCleanupPreview, error) {
	sourceID, targetID, status, _, _, _, err := s.d1SessionMigrationReadMigration(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	counts, err := s.d1SessionMigrationCountArtifacts(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	lock, err := s.d1SessionMigrationActiveLock(ctx, sourceID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	sourceLocked := lock != nil && lock.MigrationID == migrationID && lock.Locked
	blocked := []string{}
	if status != "source_locked" && status != "cleanup_prepared" {
		blocked = append(blocked, "migration_status_not_source_locked")
	}
	if !sourceLocked {
		blocked = append(blocked, "active_source_lock_not_found_for_migration")
	}
	return &SessionMigrationCleanupPreview{
		MigrationID:     migrationID,
		SourceSessionID: sourceID,
		TargetSessionID: targetID,
		Status:          status,
		SourceLocked:    sourceLocked,
		Counts:          counts,
		BlockedReasons:  blocked,
		ReadyForCleanup: len(blocked) == 0,
	}, nil
}

// d1SessionMigrationCountArtifacts is the source-side inventory the cleanup
// preview reports.
//
// The starter check counts only when the whole session is a single assistant turn
// at turn zero, because that is the only shape a cleanup may describe as
// replaceable; reporting it for a session that also holds real turns would tell an
// operator their archive is disposable when it is not.
func (s *d1Store) d1SessionMigrationCountArtifacts(
	ctx context.Context, sessionID string,
) (SessionMigrationArtifactCounts, error) {
	counts := SessionMigrationArtifactCounts{}
	tables := []struct {
		name string
		dst  *int
	}{
		{"chat_logs", &counts.ChatLogs},
		{"effective_input_logs", &counts.EffectiveInputs},
		{"memories", &counts.Memories},
		{"direct_evidence_records", &counts.DirectEvidence},
		{"kg_triples", &counts.KGTriples},
		{"episode_summaries", &counts.Episodes},
		{"protagonist_entity_memories", &counts.SubjectiveEntityMemories},
		{"session_reference_bindings", &counts.ReferenceBindings},
	}
	for _, item := range tables {
		query := "SELECT COUNT(*) FROM " + d1QuoteIdent(item.name) + " WHERE chat_session_id = ?"
		if item.name == "protagonist_entity_memories" {
			query = "SELECT COUNT(*) FROM protagonist_entity_memories WHERE source_chat_session_id = ?"
		}
		if err := s.conn.QueryRow(ctx, query, sessionID).Scan(item.dst); err != nil {
			return counts, err
		}
	}
	sessionMigrationFinalizeCounts(&counts)
	if counts.CanonicalAndSubjectiveTotal == 1 && counts.ChatLogs == 1 {
		var matching int
		if err := s.conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM chat_logs
			WHERE chat_session_id = ? AND turn_index = 0 AND LOWER(TRIM(role)) = 'assistant'
		`, sessionID).Scan(&matching); err != nil {
			return counts, err
		}
		counts.ReplaceableStarterOnly = matching == 1
	}
	return counts, nil
}

// CleanupSessionMigrationSource deletes the source session after a verified copy,
// a verified vector index, and a durable source lock.
//
// This is the only irreversible operation in the family, so it is fail-closed at
// every step: the preview must be clean, the durable parity must hold, the current
// relational state must match the stored proof, the proof produced by that check
// must be spendable, and the source vector deletion must have been recorded as
// durably complete. The counts are taken before the delete batch because a batch
// cannot report per-statement affected rows, and each table is deleted in a
// single statement scoped to the source session.
func (s *d1Store) CleanupSessionMigrationSource(
	ctx context.Context, migrationID int64, reason string,
) (*SessionMigrationCleanupResult, error) {
	if migrationID <= 0 {
		return nil, ErrNotFound
	}
	preview, err := s.d1SessionMigrationPreviewSourceCleanup(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	if len(preview.BlockedReasons) > 0 || !preview.ReadyForCleanup {
		return nil, fmt.Errorf("session migration source cleanup blocked: %s", strings.Join(preview.BlockedReasons, ","))
	}
	if err := s.d1SessionMigrationVerifyDurableParity(ctx, migrationID); err != nil {
		return nil, err
	}
	currentRelationalHash, err := s.d1SessionMigrationRevalidateCurrentRelationalState(
		ctx, migrationID, "cleanup_finalize", true)
	if err != nil {
		return nil, err
	}
	if err := s.d1SessionMigrationConsumeCurrentStateProof(
		ctx, migrationID, SessionMigrationProofOperationCleanupFinalize, currentRelationalHash); err != nil {
		return nil, err
	}
	var vectorCleanupCompleted int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM session_migration_saga_steps
		WHERE migration_id = ? AND phase = 'source_vector_cleanup' AND phase_state = 'completed'
	`, migrationID).Scan(&vectorCleanupCompleted); err != nil {
		return nil, err
	}
	if vectorCleanupCompleted != 1 {
		return nil, errors.New("session migration source cleanup blocked: source vector cleanup is not durably completed")
	}
	deletedByTable, err := s.d1SessionMigrationDeleteSourceManifestRows(ctx, preview.SourceSessionID)
	if err != nil {
		return nil, err
	}
	counts := d1SessionMigrationCountsFromDeleted(deletedByTable)
	if err := s.d1SessionMigrationRun(ctx, []D1Statement{
		{
			SQL: `
			UPDATE session_migrations
			SET status = 'source_cleaned',
			    cleanup_at = ` + d1NowExpression + `,
			    errors_json = '[]',
			    updated_at = ` + d1NowExpression + `
			WHERE id = ?`,
			Args: []any{migrationID},
		},
		d1SessionMigrationSagaStatement(migrationID, "source_cleanup", "completed",
			sessionMigrationStringHash(strconv.FormatInt(migrationID, 10), reason),
			fmt.Sprintf(`{"deleted_rows":%d}`, sessionMigrationDeletedTableTotal(deletedByTable)), ""),
	}); err != nil {
		return nil, err
	}
	return &SessionMigrationCleanupResult{
		MigrationID:     migrationID,
		SourceSessionID: preview.SourceSessionID,
		TargetSessionID: preview.TargetSessionID,
		Status:          "source_cleaned",
		Counts:          counts,
		SourceCleaned:   true,
		ReadyForLive:    true,
	}, nil
}

// d1SessionMigrationDeleteSourceManifestRows deletes the copy-owned and
// cleanup-owned rows of a source session.
//
// Retain-audit tables are excluded on purpose: audit history and feedback outlive
// the session they were recorded in, and the reference leaves them alone. The
// count is read first so the reported figures are the rows actually removed rather
// than a post-hoc total of a batch this provider cannot measure.
func (s *d1Store) d1SessionMigrationDeleteSourceManifestRows(
	ctx context.Context, sourceSessionID string,
) (map[string]int, error) {
	deleted := map[string]int{}
	statements := []D1Statement{}
	for _, entry := range SessionMigrationManifest() {
		if !entry.Direct {
			continue
		}
		if entry.Policy != SessionMigrationPolicyCopy && entry.Policy != SessionMigrationPolicyDeleteAfterVerified {
			continue
		}
		var count int
		if err := s.conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM "+d1QuoteIdent(entry.Table)+" WHERE "+d1QuoteIdent(entry.SessionColumn)+" = ?",
			sourceSessionID).Scan(&count); err != nil {
			return nil, fmt.Errorf("session migration source cleanup %s: %w", entry.Table, err)
		}
		deleted[entry.Table] = count
		statements = append(statements, D1Statement{
			SQL: "DELETE FROM " + d1QuoteIdent(entry.Table) + " WHERE " +
				d1QuoteIdent(entry.SessionColumn) + " = ?",
			Args: []any{sourceSessionID},
		})
	}
	if err := s.d1SessionMigrationRun(ctx, statements); err != nil {
		return nil, err
	}
	return deleted, nil
}
