package store

import (
	"context"
	"errors"
	"strings"
)

// D1 session fork lineage capability.
//
// The automatic path keeps an idempotency ledger, and its merge rules protect a
// confirmed worldline lineage from being rewritten by a replayed or older import:
//
//   - an existing confirmed record wins over everything except the sanctioned
//     v1-confirmed -> v2-confirmed upgrade,
//   - an existing newer import wins over an older replay,
//   - a confirmed winner may still receive the first observed message-origin map
//     (inherited_items_json) without any other field changing.
//
// MariaDB enforces this with a SERIALIZABLE transaction and SELECT FOR UPDATE.
// D1 has no row locks, so the decision is encoded as the WHERE guard of the
// ON CONFLICT DO UPDATE clause: the write and the dominance decision are one
// statement, so a concurrent confirmed record cannot be clobbered by a decision
// that was made against stale data. The readback comparison then verifies the
// outcome and fails loudly instead of returning a merged surprise.

var _ ForkLineageStore = (*d1Store)(nil)

const d1ForkLineageSelect = `
		SELECT id, contract_version, lineage_state, chat_session_id,
		       scope_id, parent_scope_id, copied_from_scope_id, copied_from_session_id,
		       fork_turn, fork_source_message_id, fork_source_role, idempotency_key,
		       imported_at, divergence_marker, provenance_source, inheritance_mode,
		       inherited_items_json, created_at, updated_at
		FROM session_fork_lineage`

// d1Scanner is scan-only so a single record reader serves both the single-row
// and the multi-row transport.
type d1Scanner interface {
	Scan(dest ...any) error
}

func d1ScanForkLineageRecord(row d1Scanner) (ForkLineageRecord, error) {
	var record ForkLineageRecord
	var scopeID, parentScopeID, copiedFromScopeID, copiedFromSessionID *string
	var forkTurn *int64
	var forkSourceMessageID, forkSourceRole, idempotencyKey *string
	var divergenceMarker, inheritedItemsJSON *string
	if err := row.Scan(
		&record.ID, &record.ContractVersion, &record.LineageState, &record.ChatSessionID,
		&scopeID, &parentScopeID, &copiedFromScopeID, &copiedFromSessionID,
		&forkTurn, &forkSourceMessageID, &forkSourceRole, &idempotencyKey,
		&record.ImportedAt, &divergenceMarker, &record.ProvenanceSource, &record.InheritanceMode,
		&inheritedItemsJSON, &record.CreatedAt, &record.UpdatedAt,
	); err != nil {
		return ForkLineageRecord{}, err
	}
	record.ScopeID = d1DerefString(scopeID)
	record.ParentScopeID = d1DerefString(parentScopeID)
	record.CopiedFromScopeID = d1DerefString(copiedFromScopeID)
	record.CopiedFromSessionID = d1DerefString(copiedFromSessionID)
	record.ForkTurn = int(d1DerefInt64(forkTurn))
	record.ForkSourceMessageID = d1DerefString(forkSourceMessageID)
	record.ForkSourceRole = d1DerefString(forkSourceRole)
	record.IdempotencyKey = d1DerefString(idempotencyKey)
	record.DivergenceMarker = d1DerefString(divergenceMarker)
	record.InheritedItemsJSON = d1DerefString(inheritedItemsJSON)
	return record, nil
}

// ListForkLineageRecords returns newest imports first. imported_at is a TEXT
// RFC3339 column, so ordering uses julianday: RFC3339Nano trims trailing
// fractional zeros, which makes a raw string order disagree with the clock for
// '.1Z' versus '.05Z' style pairs.
func (s *d1Store) ListForkLineageRecords(ctx context.Context, chatSessionID, scopeID string, limit int) ([]ForkLineageRecord, error) {
	rows, err := s.conn.Query(ctx, d1ForkLineageSelect+`
		WHERE chat_session_id = ? AND (? = '' OR scope_id = ?)
		ORDER BY julianday(imported_at) DESC, id DESC
		LIMIT ?`, chatSessionID, scopeID, scopeID, d1ClampListLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ForkLineageRecord
	for rows.Next() {
		record, err := d1ScanForkLineageRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

// SaveForkLineageRecord validates and routes to the idempotent automatic path or
// the plain manual insert, mirroring the MariaDB entry point exactly.
func (s *d1Store) SaveForkLineageRecord(ctx context.Context, record ForkLineageRecord) (ForkLineageRecord, error) {
	record.ContractVersion = firstNonEmptyString(record.ContractVersion, ForkLineageContractVersion)
	record.LineageState = firstNonEmptyString(record.LineageState, "manual")
	record.ChatSessionID = strings.TrimSpace(record.ChatSessionID)
	record.ForkSourceRole = strings.TrimSpace(record.ForkSourceRole)
	record.IdempotencyKey = strings.TrimSpace(record.IdempotencyKey)
	if record.ChatSessionID == "" {
		return record, errors.New("chat_session_id is required")
	}
	if record.ForkSourceRole != "" && record.ForkSourceRole != "user" && record.ForkSourceRole != "char" {
		return record, errors.New("fork_source_role must be user or char")
	}
	if record.IdempotencyKey != "" &&
		record.ContractVersion == RisuWorldlineForkLineageContractVersion &&
		record.LineageState == "confirmed" && record.ForkSourceRole == "" {
		return record, errors.New("fork_source_role is required for confirmed Risu worldline lineage")
	}
	record.ImportedAt = parseD1TimeOrZero(d1TimeValue(record.ImportedAt))
	record.CreatedAt = parseD1TimeOrZero(d1TimeValue(record.CreatedAt))
	if record.IdempotencyKey != "" {
		return s.saveAutomaticForkLineageRecord(ctx, record)
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO session_fork_lineage (
			contract_version, lineage_state, chat_session_id,
			scope_id, parent_scope_id, copied_from_scope_id, copied_from_session_id,
			fork_turn, fork_source_message_id, fork_source_role, idempotency_key,
			imported_at, divergence_marker, provenance_source, inheritance_mode, inherited_items_json,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		record.ContractVersion, record.LineageState, record.ChatSessionID,
		d1NullableString(record.ScopeID), d1NullableString(record.ParentScopeID),
		d1NullableString(record.CopiedFromScopeID), d1NullableString(record.CopiedFromSessionID),
		nullableForkTurn(record.ForkTurn), d1NullableString(record.ForkSourceMessageID),
		d1NullableString(record.ForkSourceRole), d1TimeValue(record.ImportedAt),
		d1NullableString(record.DivergenceMarker),
		firstNonEmptyString(record.ProvenanceSource, "manual"),
		firstNonEmptyString(record.InheritanceMode, "conservative_import"),
		d1NullableString(record.InheritedItemsJSON), d1TimeValue(record.CreatedAt)).Scan(&id); err != nil {
		return record, err
	}
	record.ID = id
	record.UpdatedAt = record.CreatedAt
	return record, nil
}

// d1ForkLineageUpsertSQL is the guarded upsert behind the idempotent path. The
// WHERE guard restates the MariaDB dominance decision:
//
//	new wins  <=>  (v1-confirmed -> v2-confirmed upgrade)
//	           OR (stored row is not confirmed AND stored import is not newer)
//
// The v1 and v2 contract versions are bound as parameters so the guard stays in
// sync with the Go constants.
const d1ForkLineageUpsertSQL = `
		INSERT INTO session_fork_lineage (
			contract_version, lineage_state, chat_session_id,
			scope_id, parent_scope_id, copied_from_scope_id, copied_from_session_id,
			fork_turn, fork_source_message_id, fork_source_role, idempotency_key,
			imported_at, divergence_marker, provenance_source, inheritance_mode,
			inherited_items_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (chat_session_id, idempotency_key) DO UPDATE SET
			contract_version = excluded.contract_version,
			lineage_state = excluded.lineage_state,
			chat_session_id = excluded.chat_session_id,
			scope_id = excluded.scope_id,
			parent_scope_id = excluded.parent_scope_id,
			copied_from_scope_id = excluded.copied_from_scope_id,
			copied_from_session_id = excluded.copied_from_session_id,
			fork_turn = excluded.fork_turn,
			fork_source_message_id = excluded.fork_source_message_id,
			fork_source_role = excluded.fork_source_role,
			divergence_marker = excluded.divergence_marker,
			provenance_source = excluded.provenance_source,
			inheritance_mode = excluded.inheritance_mode,
			inherited_items_json = excluded.inherited_items_json,
			imported_at = excluded.imported_at
		WHERE (
			(
				session_fork_lineage.contract_version = ?
				AND session_fork_lineage.lineage_state = 'confirmed'
				AND excluded.contract_version = ?
				AND excluded.lineage_state = 'confirmed'
			)
			OR (
				session_fork_lineage.lineage_state <> 'confirmed'
				AND julianday(session_fork_lineage.imported_at) <= julianday(excluded.imported_at)
			)
		)`

// d1ForkLineageAttachItemsSQL re-guards the one write the dominant path may
// perform: attaching the first observed message-origin map to a confirmed
// lineage. Every row-side condition from the MariaDB branch is restated so the
// attach cannot land on a row that stopped qualifying.
const d1ForkLineageAttachItemsSQL = `
		UPDATE session_fork_lineage
		SET inherited_items_json = ?
		WHERE chat_session_id = ? AND idempotency_key = ?
			AND lineage_state = 'confirmed'
			AND (inherited_items_json IS NULL OR TRIM(inherited_items_json) IN ('', '[]'))`

func (s *d1Store) d1ForkLineageByIdempotencyKey(ctx context.Context, chatSessionID, key string) (*ForkLineageRecord, error) {
	record, err := d1ScanForkLineageRecord(s.conn.QueryRow(ctx,
		d1ForkLineageSelect+` WHERE chat_session_id = ? AND idempotency_key = ?`, chatSessionID, key))
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *d1Store) saveAutomaticForkLineageRecord(ctx context.Context, record ForkLineageRecord) (ForkLineageRecord, error) {
	existing, err := s.d1ForkLineageByIdempotencyKey(ctx, record.ChatSessionID, record.IdempotencyKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return record, err
	}

	expected := record
	upgradesConfirmedV1ToV2 := existing != nil &&
		existing.ContractVersion == ForkLineageContractVersion && existing.LineageState == "confirmed" &&
		record.ContractVersion == RisuWorldlineForkLineageContractVersion && record.LineageState == "confirmed"
	if existing != nil && !upgradesConfirmedV1ToV2 &&
		(existing.LineageState == "confirmed" || existing.ImportedAt.After(record.ImportedAt)) {
		expected = *existing
		// Attach the first observed message-origin map without changing any
		// confirmed parent, fork point, source identity or earlier provenance.
		if existing.LineageState == "confirmed" && record.LineageState == "confirmed" &&
			(strings.TrimSpace(existing.InheritedItemsJSON) == "" || strings.TrimSpace(existing.InheritedItemsJSON) == "[]") &&
			record.InheritedItemsJSON != "" {
			if _, err := s.conn.Exec(ctx, d1ForkLineageAttachItemsSQL,
				record.InheritedItemsJSON, record.ChatSessionID, record.IdempotencyKey); err != nil {
				return record, err
			}
			expected.InheritedItemsJSON = record.InheritedItemsJSON
		}
	} else {
		if _, err := s.conn.Exec(ctx, d1ForkLineageUpsertSQL,
			record.ContractVersion, record.LineageState, record.ChatSessionID,
			d1NullableString(record.ScopeID), d1NullableString(record.ParentScopeID),
			d1NullableString(record.CopiedFromScopeID), d1NullableString(record.CopiedFromSessionID),
			nullableForkTurn(record.ForkTurn), d1NullableString(record.ForkSourceMessageID),
			d1NullableString(record.ForkSourceRole), record.IdempotencyKey,
			d1TimeValue(record.ImportedAt), d1NullableString(record.DivergenceMarker),
			firstNonEmptyString(record.ProvenanceSource, "automatic_hook"),
			firstNonEmptyString(record.InheritanceMode, "none"),
			d1NullableString(record.InheritedItemsJSON), d1TimeValue(record.CreatedAt),
			ForkLineageContractVersion, RisuWorldlineForkLineageContractVersion); err != nil {
			return record, err
		}
	}

	readback, err := s.d1ForkLineageByIdempotencyKey(ctx, record.ChatSessionID, record.IdempotencyKey)
	if err != nil {
		return record, err
	}
	if !forkLineageReadbackMatches(*readback, expected) {
		return record, errors.New("session fork lineage readback mismatch")
	}
	return *readback, nil
}
