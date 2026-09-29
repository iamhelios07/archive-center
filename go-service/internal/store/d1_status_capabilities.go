package store

import (
	"context"
	"strings"
	"time"
)

// D1 status current-value capability.
//
// This is the first D1 capability that exercises the project's central domain
// rule rather than a plain row read: "current projection" is not one JSON flag.
// A row is eligible only when its materialised write_state is 'current' AND its
// evidence names an active source revision, or the evidence names no revision at
// all (an explicit state_repair correction).
//
// The MariaDB query expresses the revision match with
// JSON_UNQUOTE(JSON_EXTRACT(...)). SQLite's json_extract already dequotes JSON
// strings, so the extraction is equivalent for text revisions, and a non-string
// revision fails to match the TEXT revision column exactly as it does on
// MariaDB instead of being coerced.

var _ StatusCurrentValueStore = (*d1Store)(nil)

// d1StatusCurrentValueSelect is the shared projection for current-value reads.
const d1StatusCurrentValueSelect = `
		SELECT current_value.id, current_value.chat_session_id, current_value.registry_id,
		       current_value.status_key, current_value.owner_scope, current_value.owner_id,
		       current_value.owner_label, current_value.value_kind, current_value.value_json,
		       current_value.evidence_json, current_value.source_turn,
		       current_value.write_state, current_value.created_at, current_value.updated_at
		FROM status_current_values current_value
		LEFT JOIN memory_source_revisions source_revision
		  ON source_revision.chat_session_id = current_value.chat_session_id
		 AND source_revision.source_revision = json_extract(current_value.evidence_json, '$."source_revision"')
		 AND source_revision.lifecycle_state = 'active'`

// d1StatusCurrentValueEligible mirrors the MariaDB eligibility predicate: the
// materialised row must be current, and either it names no source revision or the
// named revision resolves to an active one.
const d1StatusCurrentValueEligible = `
		WHERE current_value.chat_session_id = ? AND current_value.write_state = 'current'
		  AND (
			NULLIF(json_extract(current_value.evidence_json, '$."source_revision"'), '') IS NULL
			OR source_revision.source_revision IS NOT NULL
		  )`

func (s *d1Store) ListStatusCurrentValues(ctx context.Context, chatSessionID, ownerScope, ownerID, statusKey string, limit int) ([]StatusCurrentValue, error) {
	// -1 requests the complete projection, any other non-positive limit falls back
	// to the bounded default, and an oversized one is capped. Only a positive
	// limit emits a LIMIT clause, which is how the MariaDB path expresses the
	// unbounded -1 case.
	if limit <= 0 && limit != -1 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	sql := d1StatusCurrentValueSelect + d1StatusCurrentValueEligible
	args := []any{chatSessionID}
	if trimmed := strings.TrimSpace(ownerScope); trimmed != "" {
		sql += ` AND current_value.owner_scope = ?`
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(ownerID); trimmed != "" {
		sql += ` AND current_value.owner_id = ?`
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(statusKey); trimmed != "" {
		sql += ` AND current_value.status_key = ?`
		args = append(args, trimmed)
	}
	sql += ` ORDER BY current_value.owner_scope ASC, current_value.owner_id ASC, current_value.status_key ASC, current_value.updated_at DESC`
	if limit > 0 {
		sql += ` LIMIT ?`
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StatusCurrentValue
	for rows.Next() {
		var item StatusCurrentValue
		var ownerLabel *string
		var sourceTurn *int64
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.RegistryID, &item.StatusKey, &item.OwnerScope, &item.OwnerID,
			&ownerLabel, &item.ValueKind, &item.ValueJSON, &item.EvidenceJSON, &sourceTurn,
			&item.WriteState, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.OwnerLabel = d1DerefString(ownerLabel)
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SaveStatusCurrentValue(ctx context.Context, value StatusCurrentValue) (StatusCurrentValue, error) {
	now := d1TimeValue(value.CreatedAt)
	state := strings.TrimSpace(value.WriteState)
	if state == "" {
		state = "current"
	}

	// MariaDB's ON DUPLICATE KEY UPDATE with id = LAST_INSERT_ID(id) becomes a
	// SQLite upsert on the same unique key, and RETURNING id reports the existing
	// row's id on the update path just as LAST_INSERT_ID did.
	//
	// created_at is deliberately not in the update list, matching MariaDB: an
	// existing row keeps its original creation time while updated_at advances.
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO status_current_values (
			chat_session_id, registry_id, status_key, owner_scope, owner_id, owner_label,
			value_kind, value_json, evidence_json, source_turn, write_state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?, ?)
		ON CONFLICT (chat_session_id, registry_id, owner_scope, owner_id) DO UPDATE SET
			status_key = excluded.status_key,
			owner_label = excluded.owner_label,
			value_kind = excluded.value_kind,
			value_json = excluded.value_json,
			evidence_json = excluded.evidence_json,
			source_turn = excluded.source_turn,
			write_state = excluded.write_state,
			updated_at = excluded.updated_at
		RETURNING id`,
		value.ChatSessionID, value.RegistryID, value.StatusKey, value.OwnerScope, value.OwnerID,
		d1NullableString(value.OwnerLabel), value.ValueKind, value.ValueJSON, value.EvidenceJSON,
		value.SourceTurn, state, now, now).Scan(&id); err != nil {
		return value, err
	}

	value.ID = id
	// The returned struct mirrors the MariaDB return values, which report the
	// attempted timestamp rather than re-reading the persisted row.
	value.CreatedAt = parseD1TimeOrZero(now)
	value.UpdatedAt = value.CreatedAt
	value.WriteState = state
	return value, nil
}

// parseD1TimeOrZero parses a timestamp the store just rendered. The value is
// known-valid, so a parse failure yields the zero time rather than an error that
// would mask a successful write.
func parseD1TimeOrZero(text string) time.Time {
	parsed, err := parseD1Time(text)
	if err != nil {
		return time.Time{}
	}
	return parsed
}
