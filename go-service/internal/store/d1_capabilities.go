package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// D1 optional-capability implementations.
//
// HTTP routes gate individual features on optional interfaces, so a capability
// the D1 provider does not implement silently disables the route that needs it.
// CapabilityCoverage measures that gap; this file closes part of it, starting
// with the operator path the Cloudflare profile cannot ship without.

var _ AdminResetStore = (*d1Store)(nil)
var _ TimelineTurnIndexStore = (*d1Store)(nil)
var _ AuditLogCounter = (*d1Store)(nil)
var _ EffectiveInputListStore = (*d1Store)(nil)

// adminResetConfirmation records the operator confirmation token that authorises
// a destructive reset, matching the HTTP route's requirement.
const adminResetConfirmation = "RESET_ARCHIVE_CENTER_DB"

// d1ResetEpochRowID is the single row of the epoch table.
const d1ResetEpochRowID = 1

// ---------------------------------------------------------------------------
// administrative reset
// ---------------------------------------------------------------------------

// ResetAll clears every application row while preserving schema, migration
// metadata, and the reset control plane.
//
// It is the D1 counterpart to the MariaDB reset, which disables foreign keys and
// deletes its whole registry in one transaction. D1 enforces foreign keys and
// bounds a single modification, so this walks the same child-first allowlist in
// committed chunks and checkpoints its cursor, which makes the reset resumable:
//
//  1. the global maintenance lease advances its fencing token, so a resumed or
//     concurrent worker cannot act on a stale claim;
//  2. an interrupted run is resumed from its persisted cursor rather than
//     restarted, and the partial unique index permits only one active run;
//  3. each chunk commits the delete and its cursor together in one D1 batch, and
//     finishing a table clears the cursor in the same step, so a resume can never
//     skip rows by carrying a previous table's rowid into the next table;
//  4. the reset epoch advances while vector_purged_epoch stays behind, so stale
//     Vectorize results stay fenced until Stage 4 purges them;
//  5. sqlite_sequence is never touched, preserving AUTOINCREMENT monotonicity
//     exactly as the MariaDB DELETE-based reset does.
func (s *d1Store) ResetAll(ctx context.Context) (AdminResetResult, error) {
	return s.ResetAllAs(ctx, "")
}

// ResetAllAs is ResetAll plus the actor recorded in the reset run.
//
// The row already had a requested_by column that nothing wrote, so a completed
// reset could report what it deleted and never who asked for it. The plan calls
// for the reset's confirmation/authorization/audit semantics to be preserved, and
// a column that is declared but never filled is the audit half of that left
// undone: it reads as if the actor were recorded somewhere.
//
// An empty actor is stored as empty. Writing a placeholder would produce an audit
// entry that cannot be told apart from a real identity later, which is worse than
// an absent one.
func (s *d1Store) ResetAllAs(ctx context.Context, actor string) (AdminResetResult, error) {
	var result AdminResetResult

	generatedRunID, err := d1NewResetRunID()
	if err != nil {
		return result, fmt.Errorf("store: reset run id: %w", err)
	}
	now := d1TimeValue(time.Time{})
	normalizedActor := NormalizeAdminResetActor(actor)

	if _, err := s.conn.Exec(ctx, `
		UPDATE d1_maintenance_lease
		SET fencing_token = fencing_token + 1, holder = ?, acquired_at = ?, note = ?
		WHERE lease_name = ?`, generatedRunID, now, "administrative reset", d1ResetMaintenanceLeaseName); err != nil {
		return result, err
	}
	var fencingToken int64
	if err := s.conn.QueryRow(ctx,
		`SELECT fencing_token FROM d1_maintenance_lease WHERE lease_name = ?`,
		d1ResetMaintenanceLeaseName).Scan(&fencingToken); err != nil {
		return result, err
	}

	runID, tableIndex, cursor, rowsDeleted, tablesCleared, err := s.d1ResumeOrStartReset(ctx, generatedRunID, fencingToken, now, normalizedActor)
	if err != nil {
		return result, err
	}

	allowlist := d1AdminResetTableAllowlist()
	for ; tableIndex < len(allowlist); tableIndex++ {
		table := allowlist[tableIndex]
		if d1ResetReservedName(table) {
			return result, fmt.Errorf("store: reset allowlist must not target %q", table)
		}
		for {
			deleted, nextCursor, chunkErr := s.d1ResetChunk(ctx, runID, table, tableIndex, cursor, rowsDeleted, tablesCleared, now)
			if chunkErr != nil {
				_, _ = s.conn.Exec(ctx, `
					UPDATE d1_reset_runs SET status = 'failed', last_error = ?, updated_at = ?
					WHERE reset_run_id = ?`, chunkErr.Error(), d1TimeValue(time.Time{}), runID)
				return result, chunkErr
			}
			rowsDeleted += deleted
			if deleted == 0 {
				break
			}
			cursor = nextCursor
		}
		cursor = 0
		tablesCleared++
		// Persist the cleared cursor so a resume starts the next table from its
		// own beginning instead of inheriting the previous table's rowid.
		if _, err := s.conn.Exec(ctx, `
			UPDATE d1_reset_runs
			SET table_index = ?, table_name = ?, last_key = NULL, rows_deleted = ?, tables_cleared = ?, updated_at = ?
			WHERE reset_run_id = ?`, tableIndex+1, table, rowsDeleted, tablesCleared, now, runID); err != nil {
			return result, err
		}
	}

	// The D1 deletion is complete. Vectorize purge for this epoch lands in
	// Stage 4, so the run completes while vector_purged_epoch deliberately stays
	// behind: the epoch fence, not this status, keeps stale vectors untrusted.
	completedAt := d1TimeValue(time.Time{})
	if _, err := s.conn.Exec(ctx, `
		UPDATE d1_reset_runs
		SET status = 'completed', completed_at = ?, updated_at = ?, table_index = ?, table_name = NULL,
		    last_key = NULL, rows_deleted = ?, tables_cleared = ?
		WHERE reset_run_id = ?`,
		completedAt, completedAt, len(allowlist), rowsDeleted, tablesCleared, runID); err != nil {
		return result, err
	}
	if _, err := s.conn.Exec(ctx, `
		UPDATE d1_maintenance_lease
		SET holder = NULL, acquired_at = NULL, note = 'reset completed'
		WHERE lease_name = ?`, d1ResetMaintenanceLeaseName); err != nil {
		return result, err
	}

	result.TablesCleared = tablesCleared
	result.RowsDeleted = rowsDeleted
	return result, nil
}

// d1ResumeOrStartReset continues an interrupted run or opens a new one, returning
// the run id and its persisted cursor.
func (s *d1Store) d1ResumeOrStartReset(ctx context.Context, runID string, fencingToken int64, now, actor string) (
	activeRun string, tableIndex int, cursor int64, rowsDeleted int64, tablesCleared int, err error,
) {
	var existingRunID, lastKey string
	var existingIndex, existingCleared int
	var existingRows int64
	queryErr := s.conn.QueryRow(ctx, `
		SELECT reset_run_id, table_index, COALESCE(last_key, ''), rows_deleted, tables_cleared
		FROM d1_reset_runs
		WHERE status IN ('running', 'purging_vectors')
		ORDER BY started_at ASC
		LIMIT 1`).Scan(&existingRunID, &existingIndex, &lastKey, &existingRows, &existingCleared)

	if queryErr == nil {
		resumedCursor := int64(0)
		if trimmed := strings.TrimSpace(lastKey); trimmed != "" {
			parsed, parseErr := strconv.ParseInt(trimmed, 10, 64)
			if parseErr != nil {
				return "", 0, 0, 0, 0, fmt.Errorf("store: reset cursor %q is not a rowid: %w", lastKey, parseErr)
			}
			resumedCursor = parsed
		}
		if _, updateErr := s.conn.Exec(ctx, `
			UPDATE d1_reset_runs
			SET status = 'running', fencing_token = ?, updated_at = ?,
			    requested_by = COALESCE(NULLIF(requested_by, ''), NULLIF(?, ''))
			WHERE reset_run_id = ?`, fencingToken, now, actor, existingRunID); updateErr != nil {
			return "", 0, 0, 0, 0, updateErr
		}
		return existingRunID, existingIndex, resumedCursor, existingRows, existingCleared, nil
	}
	if !errors.Is(queryErr, errD1NoRows) {
		return "", 0, 0, 0, 0, queryErr
	}

	var epoch int64
	if err := s.conn.QueryRow(ctx,
		`SELECT current_epoch FROM d1_reset_epoch WHERE epoch_id = ?`, d1ResetEpochRowID).Scan(&epoch); err != nil {
		return "", 0, 0, 0, 0, err
	}
	if _, err := s.conn.Exec(ctx, `
		UPDATE d1_reset_epoch SET current_epoch = current_epoch + 1, updated_at = ?
		WHERE epoch_id = ?`, now, d1ResetEpochRowID); err != nil {
		return "", 0, 0, 0, 0, err
	}
	if _, err := s.conn.Exec(ctx, `
		INSERT INTO d1_reset_runs
			(reset_run_id, epoch, status, table_index, rows_deleted, tables_cleared, started_at, updated_at, fencing_token, confirmation, requested_by)
		VALUES (?, ?, 'running', 0, 0, 0, ?, ?, ?, ?, ?)`,
		runID, epoch+1, now, now, fencingToken, adminResetConfirmation, actor); err != nil {
		return "", 0, 0, 0, 0, err
	}
	return runID, 0, 0, 0, 0, nil
}

// d1ResetChunk deletes one bounded chunk and advances the cursor in the same D1
// batch, so the checkpoint and the deletion commit atomically. It returns the
// number of rows deleted and the new cursor.
func (s *d1Store) d1ResetChunk(
	ctx context.Context, runID, table string, tableIndex int, cursor, rowsDeleted int64, tablesCleared int, now string,
) (int64, int64, error) {
	rows, err := s.conn.Query(ctx, d1ResetChunkSelectSQL(table), cursor, d1ResetChunkSize)
	if err != nil {
		return 0, cursor, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, cursor, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, cursor, err
	}
	rows.Close()

	if len(ids) == 0 {
		return 0, cursor, nil
	}
	nextCursor := ids[len(ids)-1]

	if err := s.conn.Batch(ctx,
		D1Statement{SQL: d1ResetChunkDeleteSQL(table), Args: []any{cursor, d1ResetChunkSize}},
		D1Statement{
			SQL: `UPDATE d1_reset_runs
				SET table_index = ?, table_name = ?, last_key = ?, rows_deleted = ?, tables_cleared = ?, updated_at = ?
				WHERE reset_run_id = ?`,
			Args: []any{tableIndex, table, strconv.FormatInt(nextCursor, 10), rowsDeleted + int64(len(ids)), tablesCleared, now, runID},
		},
	); err != nil {
		return 0, cursor, err
	}
	return int64(len(ids)), nextCursor, nil
}

// d1NewResetRunID returns a unique reset run identifier.
func d1NewResetRunID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// ---------------------------------------------------------------------------
// timeline turn index
// ---------------------------------------------------------------------------

// LatestTimelineTurnIndex reports the newest turn across every ledger the
// timeline renders, matching the MariaDB query's UNION ALL maxima.
func (s *d1Store) LatestTimelineTurnIndex(ctx context.Context, chatSessionID string) (int, error) {
	// SQLite has no GREATEST(); the result is clamped in Go so the aggregate
	// MAX(...) form stays unambiguous.
	var latest *int64
	if err := s.conn.QueryRow(ctx, `
		SELECT MAX(latest) FROM (
			SELECT MAX(turn_index) AS latest FROM chat_logs WHERE chat_session_id = ?
			UNION ALL SELECT MAX(turn_index) FROM memories WHERE chat_session_id = ?
			UNION ALL SELECT MAX(turn_anchor) FROM direct_evidence_records WHERE chat_session_id = ?
			UNION ALL SELECT MAX(source_turn) FROM kg_triples WHERE chat_session_id = ?
			UNION ALL SELECT MAX(to_turn) FROM episode_summaries WHERE chat_session_id = ?
		) AS timeline_turns`,
		chatSessionID, chatSessionID, chatSessionID, chatSessionID, chatSessionID).Scan(&latest); err != nil {
		return 0, err
	}
	if latest == nil || *latest < 0 {
		return 0, nil
	}
	return int(*latest), nil
}

// ---------------------------------------------------------------------------
// audit log counter
// ---------------------------------------------------------------------------

// CountAuditLogs counts the entries ListAuditLogs would page through, using the
// same filters so a paginated total agrees with the returned rows.
func (s *d1Store) CountAuditLogs(ctx context.Context, chatSessionID string, eventType string) (int, error) {
	var total int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM audit_logs
		WHERE (? = '' OR chat_session_id = ?) AND (? = '' OR event_type = ?)`,
		strings.TrimSpace(chatSessionID), chatSessionID,
		strings.TrimSpace(eventType), eventType).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// effective input listing
// ---------------------------------------------------------------------------

// ListEffectiveInputs returns the processed intents in a turn range.
func (s *d1Store) ListEffectiveInputs(ctx context.Context, chatSessionID string, fromTurn, toTurn int) ([]EffectiveInput, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, turn_index, effective_input, created_at
		FROM effective_input_logs
		WHERE chat_session_id = ? AND (? <= 0 OR turn_index >= ?) AND (? <= 0 OR turn_index <= ?)
		ORDER BY turn_index ASC, id ASC
	`, chatSessionID, fromTurn, fromTurn, toTurn, toTurn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []EffectiveInput{}
	for rows.Next() {
		var item EffectiveInput
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.TurnIndex, &item.EffectiveInput, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
