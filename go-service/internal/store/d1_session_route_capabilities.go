package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// D1 session routing capability: the durable official-host route binding and the
// imported-turn baseline.
//
// The two capabilities answer two different routing questions and neither shares
// the other's write path, so both live here:
//
//   - SessionRouteBindingStore decides which canonical Archive Center SID belongs
//     to an official RisuAI (stable character, host chat) pair, and refuses to
//     route a host onto a session that has been locked away by a migration.
//   - SessionRoutingBaselineStore resolves how far into a copied or stitched
//     target session the imported archive reaches, so a locally deleted new turn
//     cannot take imported rows with it.
//
// MariaDB reaches both through a transaction: SERIALIZABLE with SELECT ... FOR
// UPDATE for the binding, and an aggregate read for the baseline. D1 has no row
// locks, so the decision MariaDB makes while holding the row is restated here as
// a guard inside the statement itself, and the readback comparison stays the
// thing that fails loudly rather than returning a merged surprise. The mode
// vocabulary and the redirect-hop bound are shared with the MariaDB path instead
// of being restated, so the two providers cannot drift apart.
//
// MariaDB-only idioms are deliberately absent: no FOR UPDATE, no transaction
// isolation hints, and no MySQL upsert. The binding write is a single SQLite
// upsert whose conflict branch carries the guard.

var _ SessionRouteBindingStore = (*d1Store)(nil)
var _ SessionRoutingBaselineStore = (*d1Store)(nil)

// ---------------------------------------------------------------------------
// session route binding
// ---------------------------------------------------------------------------

// d1SessionRouteBindingSelect mirrors the MariaDB projection. The two COALESCEs
// reproduce the nullable columns the MariaDB read normalises in Go, so the two
// providers read the same representation for an absent redirect.
const d1SessionRouteBindingSelect = `
		SELECT contract_version, stable_character_id, host_chat_id,
		       canonical_session_id, binding_state, binding_reason,
		       COALESCE(redirected_from_session_id, ''),
		       COALESCE(redirect_migration_id, 0), revision, created_at, updated_at
		FROM session_route_bindings`

// d1SessionRouteBindingUpsertSQL is the binding write.
//
// MariaDB decides between INSERT, UPDATE, and no write at all while it holds the
// row lock. SQLite has no such lock, so the same decision is encoded here:
//
//   - no conflicting row          -> the INSERT branch runs, revision 1,
//     exactly as MariaDB inserts a new binding;
//   - conflicting row, guard true -> the DO UPDATE branch runs, revision + 1,
//     exactly as MariaDB's forced or redirected
//     UPDATE;
//   - conflicting row, guard false-> nothing is written, exactly as MariaDB's
//     idempotent replay branch, which must not bump
//     the revision of a binding that already carries
//     the resolved route.
//
// The guard is a bound parameter rather than a literal so the statement text
// stays constant and the decision travels with the call that made it. A
// concurrent writer that inserts the same identity between the read and this
// statement therefore loses the conflict branch and is caught by the readback
// comparison below rather than being silently overwritten.
const d1SessionRouteBindingUpsertSQL = `
		INSERT INTO session_route_bindings (
			contract_version, stable_character_id, host_chat_id,
			canonical_session_id, binding_state, binding_reason,
			redirected_from_session_id, redirect_migration_id, revision
		) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, 1)
		ON CONFLICT (stable_character_id, host_chat_id) DO UPDATE SET
			contract_version = excluded.contract_version,
			canonical_session_id = excluded.canonical_session_id,
			binding_state = excluded.binding_state,
			binding_reason = excluded.binding_reason,
			redirected_from_session_id = excluded.redirected_from_session_id,
			redirect_migration_id = excluded.redirect_migration_id,
			revision = session_route_bindings.revision + 1,
			updated_at = ` + d1NowExpression + `
		WHERE ? = 1`

// d1SessionRouteLockSelect resolves one redirect hop.
//
//   - `locked = 1` is the SQLite form of MariaDB's `locked = TRUE`: the D1 schema
//     stores the flag as an INTEGER, so the boolean literal is not portable here.
//   - locked_at is a TEXT RFC3339 column, so the ordering uses julianday. RFC3339
//     trims trailing fractional zeros, which makes a raw string order disagree
//     with the clock for a '.1Z' versus '.05Z' pair.
const d1SessionRouteLockSelect = `
		SELECT target_session_id, migration_id, lock_status
		FROM session_migration_locks
		WHERE source_session_id = ? AND locked = 1 AND unlocked_at IS NULL
		ORDER BY julianday(locked_at) DESC, id DESC
		LIMIT 1`

// d1SessionRouteLockHopLimit mirrors the eight-hop bound of the MariaDB redirect
// walk. A longer chain means the lock ledger is corrupt, not that the route should
// be followed further.
const d1SessionRouteLockHopLimit = 8

// d1UnsignedRevision maps the SQLite INTEGER revision onto the uint64 field.
// MariaDB stores revision as BIGINT UNSIGNED, so a negative value is
// unrepresentable there; SQLite has no unsigned type, and clamping keeps a
// corrupt value from wrapping into a huge revision.
func d1UnsignedRevision(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func d1ScanSessionRouteBinding(row d1Scanner) (SessionRouteBinding, error) {
	var binding SessionRouteBinding
	var revision int64
	if err := row.Scan(
		&binding.ContractVersion,
		&binding.StableCharacterID,
		&binding.HostChatID,
		&binding.CanonicalSessionID,
		&binding.BindingState,
		&binding.BindingReason,
		&binding.RedirectedFromSessionID,
		&binding.RedirectMigrationID,
		&revision,
		&binding.CreatedAt,
		&binding.UpdatedAt,
	); err != nil {
		return SessionRouteBinding{}, err
	}
	binding.Revision = d1UnsignedRevision(revision)
	return binding, nil
}

func (s *d1Store) d1SessionRouteBinding(ctx context.Context, stableCharacterID, hostChatID string) (*SessionRouteBinding, error) {
	binding, err := d1ScanSessionRouteBinding(s.conn.QueryRow(ctx,
		d1SessionRouteBindingSelect+` WHERE stable_character_id = ? AND host_chat_id = ?`,
		stableCharacterID, hostChatID))
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

// d1SessionRouteBindingEnvelopeOK restates the MariaDB readback identity check.
// A blank expectedCanonical asks only for a non-blank canonical session, which is
// the resolve_existing form; a concrete value is compared exactly, which is the
// form the write path uses against the route it just resolved.
func d1SessionRouteBindingEnvelopeOK(binding SessionRouteBinding, stableCharacterID, hostChatID, expectedCanonical string) bool {
	if binding.ContractVersion != SessionRouteBindingContractVersion ||
		binding.StableCharacterID != stableCharacterID ||
		binding.HostChatID != hostChatID ||
		binding.BindingState != "active" {
		return false
	}
	if expectedCanonical == "" {
		return strings.TrimSpace(binding.CanonicalSessionID) != ""
	}
	return binding.CanonicalSessionID == expectedCanonical
}

// BindSessionRoute resolves or creates the durable route binding for one official
// host identity, following the migration source lock so a chat that was migrated
// away lands on its current owner.
//
// The mode vocabulary is the shared sessionRouteBindingModeSupported helper, so an
// unsupported mode is rejected identically on both providers.
func (s *d1Store) BindSessionRoute(ctx context.Context, req SessionRouteBindingRequest) (*SessionRouteBindingResult, error) {
	stableCharacterID := strings.TrimSpace(req.StableCharacterID)
	hostChatID := strings.TrimSpace(req.HostChatID)
	requestedSessionID := strings.TrimSpace(req.RequestedSessionID)
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = SessionRouteBindingModeResolveOrCreate
	}
	if stableCharacterID == "" || hostChatID == "" {
		return nil, errors.New("stable_character_id and host_chat_id are required")
	}
	if !sessionRouteBindingModeSupported(mode) {
		return nil, fmt.Errorf("unsupported session route binding mode %q", mode)
	}

	existing, err := s.d1SessionRouteBinding(ctx, stableCharacterID, hostChatID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if mode == SessionRouteBindingModeResolveExisting {
		if errors.Is(err, ErrNotFound) || existing == nil {
			return nil, ErrNotFound
		}
		// Read-only mode: a binding that does not already prove the route is not
		// created here, and a binding that is not in the expected contract or
		// state is reported rather than repaired.
		if !d1SessionRouteBindingEnvelopeOK(*existing, stableCharacterID, hostChatID, "") {
			return nil, errors.New("session route binding readback mismatch")
		}
		return &SessionRouteBindingResult{
			Binding:          *existing,
			ReadbackVerified: true,
		}, nil
	}

	forceRequested := mode == SessionRouteBindingModeManualAttach || mode == SessionRouteBindingModeMigrationCommit
	canonicalSessionID := requestedSessionID
	if existing != nil && !forceRequested {
		canonicalSessionID = existing.CanonicalSessionID
	}
	if canonicalSessionID == "" {
		return nil, errors.New("requested_session_id is required when no durable route binding exists")
	}

	redirectedSessionID, redirectMigrationID, err := s.d1ResolveLockedSessionRoute(ctx, canonicalSessionID)
	if err != nil {
		return nil, err
	}
	lockedRedirect := redirectedSessionID != canonicalSessionID
	redirectedFrom := ""
	if lockedRedirect {
		redirectedFrom = canonicalSessionID
		canonicalSessionID = redirectedSessionID
	}
	bindingReason := mode
	if existing != nil && !forceRequested && !lockedRedirect {
		bindingReason = "existing_readback"
	}
	if lockedRedirect {
		bindingReason = "locked_source_redirect"
	}

	// The write is skipped entirely when the existing row already carries the
	// resolved route and the caller did not force an attach, matching the branch
	// structure of the MariaDB path.
	writeUpdate := forceRequested || lockedRedirect
	if _, err := s.conn.Exec(ctx, d1SessionRouteBindingUpsertSQL,
		SessionRouteBindingContractVersion, stableCharacterID, hostChatID,
		canonicalSessionID, bindingReason,
		d1NullableString(redirectedFrom), nullablePositiveInt64(redirectMigrationID),
		d1BoolValue(writeUpdate)); err != nil {
		return nil, err
	}

	result := &SessionRouteBindingResult{
		Created:              existing == nil,
		Updated:              existing != nil && writeUpdate,
		LockedSourceRedirect: lockedRedirect,
	}
	readback, err := s.d1SessionRouteBinding(ctx, stableCharacterID, hostChatID)
	if err != nil {
		return nil, fmt.Errorf("session route binding readback: %w", err)
	}
	if !d1SessionRouteBindingEnvelopeOK(*readback, stableCharacterID, hostChatID, canonicalSessionID) {
		return nil, errors.New("session route binding readback mismatch")
	}
	result.Binding = *readback
	result.ReadbackVerified = true
	return result, nil
}

// d1ResolveLockedSessionRoute walks the migration source lock chain and returns the
// session that currently owns the requested one. A source that is still being
// verified, or whose lock is in a state that may not be routed through, is a
// blocker rather than a redirect: routing a host onto a session that has not been
// proven would move memory to the wrong owner.
func (s *d1Store) d1ResolveLockedSessionRoute(ctx context.Context, sessionID string) (string, int64, error) {
	current := strings.TrimSpace(sessionID)
	seen := map[string]bool{}
	var lastMigrationID int64
	for hop := 0; hop < d1SessionRouteLockHopLimit; hop++ {
		if current == "" {
			return "", 0, errors.New("empty canonical session route")
		}
		if seen[current] {
			return "", 0, errors.New("session route lock redirect cycle")
		}
		seen[current] = true
		var target string
		var migrationID int64
		var lockStatus string
		err := s.conn.QueryRow(ctx, d1SessionRouteLockSelect, current).Scan(&target, &migrationID, &lockStatus)
		if errors.Is(err, errD1NoRows) {
			return current, lastMigrationID, nil
		}
		if err != nil {
			return "", 0, err
		}
		if lockStatus == "lock_pending_verification" {
			return "", 0, sessionMigrationBlocker(
				"source_lock_verification_in_progress", "session_route", "",
			)
		}
		if lockStatus != "migrated_away" {
			return "", 0, sessionMigrationBlocker(
				"source_lock_state_not_routable", "session_route", "",
			)
		}
		target = strings.TrimSpace(target)
		if target == "" || target == current {
			return "", 0, errors.New("invalid session route lock redirect")
		}
		current = target
		lastMigrationID = migrationID
	}
	return "", 0, errors.New("session route lock redirect depth exceeded")
}

// ---------------------------------------------------------------------------
// session routing baseline
// ---------------------------------------------------------------------------

// d1SessionRoutingBaselineSelect is the MariaDB baseline aggregate, restated for
// SQLite. The two guards that decide which copied turn counts are load bearing and
// must not be relaxed:
//
//   - a row-map entry counts only when it maps a chat_logs row that is not
//     rolled back and whose target_row_id really belongs to this migration's
//     target session. Without the second guard a migrated row that was later
//     re-pointed at another session would raise the baseline.
//   - a migration with no imported turn above zero contributes nothing, so a
//     target session that only holds its starter row reports no baseline. The
//     HAVING gate therefore runs before the newest-migration ordering.
const d1SessionRoutingBaselineSelect = `
		SELECT sm.id, sm.source_session_id, sm.target_session_id, sm.mode,
		       COALESCE(MAX(cl.turn_index), 0) AS imported_through_turn
		FROM session_migrations sm
		LEFT JOIN session_migration_row_map rm
		  ON rm.migration_id = sm.id
		 AND rm.table_name = 'chat_logs'
		 AND rm.target_row_id IS NOT NULL
		 AND rm.row_status <> 'rolled_back'
		LEFT JOIN chat_logs cl
		  ON cl.id = rm.target_row_id
		 AND cl.chat_session_id = sm.target_session_id
		WHERE sm.target_session_id = ?
		  AND sm.status NOT IN ('rolled_back', 'rollback_partial')
		GROUP BY sm.id, sm.source_session_id, sm.target_session_id, sm.mode
		HAVING imported_through_turn > 0 OR sm.mode = '` + SessionMigrationModeStitch + `'
		ORDER BY sm.id DESC
		LIMIT 1`

// d1SessionStitchNoteSelect reads the stitch operation note. A stitch records the
// authoritative imported boundary inside the note rather than in the row map, so
// the note is the only place the current offset and the current source set live.
const d1SessionStitchNoteSelect = `SELECT COALESCE(operator_note, '') FROM session_migrations WHERE id = ?`

// GetSessionRoutingBaseline resolves the imported-turn boundary for a copied or
// stitched target session. The caller uses it to clamp deletions so a locally
// removed new turn cannot take the imported archive with it.
func (s *d1Store) GetSessionRoutingBaseline(ctx context.Context, targetSessionID string) (*SessionRoutingBaseline, error) {
	targetID := strings.TrimSpace(targetSessionID)
	if targetID == "" {
		return nil, ErrNotFound
	}
	row := &SessionRoutingBaseline{}
	if err := s.conn.QueryRow(ctx, d1SessionRoutingBaselineSelect, targetID).Scan(
		&row.MigrationID,
		&row.SourceSessionID,
		&row.TargetSessionID,
		&row.Mode,
		&row.ImportedThroughTurn,
	); err != nil {
		if errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if row.Mode == SessionMigrationModeStitch {
		var note string
		if err := s.conn.QueryRow(ctx, d1SessionStitchNoteSelect, row.MigrationID).Scan(&note); err != nil {
			return nil, err
		}
		// A stitch baseline is the note's current offset, not the row-map maximum,
		// and only a note that actually carries a current source rewrites the
		// source identity.
		if offset, ok := sessionStitchCurrentOffset(note); ok {
			row.ImportedThroughTurn = offset
			var result SessionStitchResult
			if json.Unmarshal([]byte(note), &result) == nil && result.CurrentSourceSessionID != "" {
				row.SourceSessionID = result.CurrentSourceSessionID
				row.SourceSessionIDs = result.CurrentSourceSessionIDs
				row.InputGroupAliases = result.CurrentInputGroupAliases
			}
		}
	}
	return row, nil
}
