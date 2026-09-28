package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

func d1Exec(t *testing.T, conn D1Conn, query string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("seed %q: %v", strings.TrimSpace(strings.SplitN(query, "\n", 2)[0]), err)
	}
}

// d1SeedSessionMigration writes one migration ledger row. The explicit id keeps
// the "newest migration wins" assertions readable.
func d1SeedSessionMigration(t *testing.T, conn D1Conn, id int64, sourceSessionID, targetSessionID, mode, status, operatorNote string) {
	t.Helper()
	d1Exec(t, conn, `
		INSERT INTO session_migrations (id, source_session_id, target_session_id, mode, status, operator_note)
		VALUES (?, ?, ?, ?, ?, NULLIF(?, ''))`,
		id, sourceSessionID, targetSessionID, mode, status, operatorNote)
}

func d1SeedSessionMigrationLock(t *testing.T, conn D1Conn, migrationID int64, sourceSessionID, targetSessionID, lockStatus, lockedAt string, locked int, unlockedAt any) {
	t.Helper()
	d1Exec(t, conn, `
		INSERT INTO session_migration_locks (migration_id, source_session_id, target_session_id, lock_status, locked, locked_at, unlocked_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		migrationID, sourceSessionID, targetSessionID, lockStatus, locked, lockedAt, unlockedAt)
}

func d1SeedRouteBinding(t *testing.T, conn D1Conn, contractVersion, stableCharacterID, hostChatID, canonicalSessionID, state, reason string, revision int64) {
	t.Helper()
	d1Exec(t, conn, `
		INSERT INTO session_route_bindings (
			contract_version, stable_character_id, host_chat_id,
			canonical_session_id, binding_state, binding_reason, revision
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		contractVersion, stableCharacterID, hostChatID, canonicalSessionID, state, reason, revision)
}

func d1SeedChatLog(t *testing.T, conn D1Conn, chatSessionID string, turnIndex int, role, content string) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES (?, ?, ?, ?) RETURNING id`,
		chatSessionID, turnIndex, role, content).Scan(&id); err != nil {
		t.Fatalf("seed chat log %s/%d: %v", chatSessionID, turnIndex, err)
	}
	return id
}

func d1SeedChatRowMap(t *testing.T, conn D1Conn, migrationID int64, sourceRowID, targetRowID int64, rowStatus string) {
	t.Helper()
	d1Exec(t, conn, `
		INSERT INTO session_migration_row_map (migration_id, table_name, source_row_id, target_row_id, row_status)
		VALUES (?, 'chat_logs', ?, ?, ?)`,
		migrationID, sourceRowID, targetRowID, rowStatus)
}

// d1ReadBinding reads the stored binding row directly, so a test can assert the
// durable state rather than the value the store chose to hand back.
func d1ReadBinding(t *testing.T, conn D1Conn, stableCharacterID, hostChatID string) SessionRouteBinding {
	t.Helper()
	binding, err := d1ScanSessionRouteBinding(conn.QueryRow(context.Background(),
		d1SessionRouteBindingSelect+` WHERE stable_character_id = ? AND host_chat_id = ?`,
		stableCharacterID, hostChatID))
	if err != nil {
		t.Fatalf("read stored binding: %v", err)
	}
	return binding
}

// ---------------------------------------------------------------------------
// capability advertisement
// ---------------------------------------------------------------------------

// TestD1SessionRouteCapabilitiesAreAdvertised pins that both routing capabilities
// are reachable by the type assertion the HTTP routes perform. A capability that
// is implemented but not reachable would leave the route silently disabled.
func TestD1SessionRouteCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var _ SessionRouteBindingStore = st
	var _ SessionRoutingBaselineStore = st

	implemented := map[string]bool{}
	for _, status := range CapabilityReport(st) {
		implemented[status.Name] = status.Implemented
	}
	for _, name := range []string{"SessionRouteBindingStore", "SessionRoutingBaselineStore"} {
		if !implemented[name] {
			t.Errorf("D1 provider must advertise %s", name)
		}
	}
}

// ---------------------------------------------------------------------------
// BindSessionRoute: creation and readback
// ---------------------------------------------------------------------------

func TestD1BindSessionRouteCreatesAndReadbacksOfficialIdentity(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID:  "character-stable",
		HostChatID:         "chat-opaque",
		RequestedSessionID: "session-canonical",
		Mode:               SessionRouteBindingModeResolveOrCreate,
	})
	if err != nil {
		t.Fatalf("BindSessionRoute: %v", err)
	}
	if !result.Created || result.Updated || !result.ReadbackVerified || result.LockedSourceRedirect {
		t.Fatalf("binding result = %+v", result)
	}
	if result.Binding.ContractVersion != SessionRouteBindingContractVersion ||
		result.Binding.CanonicalSessionID != "session-canonical" ||
		result.Binding.BindingState != "active" ||
		result.Binding.BindingReason != SessionRouteBindingModeResolveOrCreate ||
		result.Binding.Revision != 1 {
		t.Fatalf("binding = %+v", result.Binding)
	}
	if result.Binding.RedirectedFromSessionID != "" || result.Binding.RedirectMigrationID != 0 {
		t.Errorf("a fresh binding must carry no redirect: %+v", result.Binding)
	}
	if result.Binding.CreatedAt.IsZero() || result.Binding.UpdatedAt.IsZero() {
		t.Errorf("schema default timestamps must be readable: %+v", result.Binding)
	}

	stored := d1ReadBinding(t, conn, "character-stable", "chat-opaque")
	if stored.CanonicalSessionID != "session-canonical" || stored.Revision != 1 {
		t.Errorf("stored row = %+v", stored)
	}
}

func TestD1BindSessionRouteKeepsSameHostChatSeparateAcrossStableCharacters(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, tc := range []struct{ character, session string }{
		{"different-character", "different-session"},
		{"other-character", "other-session"},
	} {
		result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
			StableCharacterID:  tc.character,
			HostChatID:         "same-chat",
			RequestedSessionID: tc.session,
		})
		if err != nil || result.Binding.CanonicalSessionID != tc.session {
			t.Fatalf("character %s: result=%+v err=%v", tc.character, result, err)
		}
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_route_bindings WHERE host_chat_id = 'same-chat'`); got != 2 {
		t.Errorf("bindings for one host chat = %d, want 2 (the stable character is part of the identity)", got)
	}
}

// TestD1BindSessionRouteReplayIsIdempotentAndDoesNotBumpRevision pins the replay
// branch: an existing binding already carries the resolved route, so a second
// resolve_or_create must not rewrite it and must not advance the revision.
func TestD1BindSessionRouteReplayIsIdempotentAndDoesNotBumpRevision(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	first, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "session-a",
	})
	if err != nil {
		t.Fatalf("first bind: %v", err)
	}

	replay, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "session-b",
	})
	if err != nil {
		t.Fatalf("replayed bind: %v", err)
	}
	if replay.Created || replay.Updated || !replay.ReadbackVerified {
		t.Fatalf("replay must be a no-op readback: %+v", replay)
	}
	if replay.Binding.CanonicalSessionID != "session-a" {
		t.Errorf("canonical session = %q, want the durable session-a", replay.Binding.CanonicalSessionID)
	}
	if replay.Binding.Revision != first.Binding.Revision {
		t.Errorf("replay bumped the revision from %d to %d", first.Binding.Revision, replay.Binding.Revision)
	}
	stored := d1ReadBinding(t, conn, "character-stable", "chat-opaque")
	if stored.Revision != 1 || stored.BindingReason != SessionRouteBindingModeResolveOrCreate {
		t.Errorf("replay must leave the stored reason untouched: %+v", stored)
	}
}

// TestD1BindSessionRouteForcedAttachOverridesExistingRoute covers the two modes
// that deliberately move an existing binding onto a requested session.
func TestD1BindSessionRouteForcedAttachOverridesExistingRoute(t *testing.T) {
	for _, mode := range []string{SessionRouteBindingModeManualAttach, SessionRouteBindingModeMigrationCommit} {
		t.Run(mode, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
				"character-stable", "chat-opaque", "session-old", "active", "existing_readback", 7)

			result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
				StableCharacterID: "character-stable", HostChatID: "chat-opaque",
				RequestedSessionID: "session-new", Mode: mode,
			})
			if err != nil {
				t.Fatalf("forced bind: %v", err)
			}
			if result.Created || !result.Updated || !result.ReadbackVerified {
				t.Fatalf("forced attach result = %+v", result)
			}
			if result.Binding.CanonicalSessionID != "session-new" || result.Binding.BindingReason != mode {
				t.Errorf("binding = %+v, want the requested session and the mode as reason", result.Binding)
			}
			if result.Binding.Revision != 8 {
				t.Errorf("revision = %d, want 8", result.Binding.Revision)
			}
			if result.Binding.UpdatedAt.Before(result.Binding.CreatedAt) {
				t.Errorf("a forced update must advance updated_at: %+v", result.Binding)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BindSessionRoute: migration source lock
// ---------------------------------------------------------------------------

func TestD1BindSessionRouteRedirectsLockedSourceAndPersistsTarget(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 41, "locked-source", "canonical-target", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigrationLock(t, conn, 41, "locked-source", "canonical-target", "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)
	d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
		"character-stable", "chat-opaque", "locked-source", "active", "existing_readback", 2)

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque",
	})
	if err != nil {
		t.Fatalf("redirecting bind: %v", err)
	}
	if !result.Updated || !result.LockedSourceRedirect || result.Created {
		t.Fatalf("redirect result = %+v", result)
	}
	if result.Binding.CanonicalSessionID != "canonical-target" ||
		result.Binding.BindingReason != "locked_source_redirect" ||
		result.Binding.RedirectedFromSessionID != "locked-source" ||
		result.Binding.RedirectMigrationID != 41 ||
		result.Binding.Revision != 3 {
		t.Fatalf("redirect binding = %+v", result.Binding)
	}
}

// TestD1BindSessionRouteFollowsRedirectChainToCurrentOwner pins that the whole
// lock chain is followed and that the recorded migration id is the last hop, not
// the first.
func TestD1BindSessionRouteFollowsRedirectChainToCurrentOwner(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		source := "hop-" + string(rune('a'+i))
		target := "hop-" + string(rune('b'+i))
		id := int64(100 + i)
		d1SeedSessionMigration(t, conn, id, source, target, "copy_then_lock_source", "copied", "")
		d1SeedSessionMigrationLock(t, conn, id, source, target, "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)
	}

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "hop-a",
	})
	if err != nil {
		t.Fatalf("chained bind: %v", err)
	}
	if result.Binding.CanonicalSessionID != "hop-d" || result.Binding.RedirectedFromSessionID != "hop-a" {
		t.Fatalf("chain resolution = %+v", result.Binding)
	}
	if result.Binding.RedirectMigrationID != 102 {
		t.Errorf("redirect_migration_id = %d, want the last hop 102", result.Binding.RedirectMigrationID)
	}
}

// TestD1BindSessionRouteIgnoresReleasedSourceLocks proves the lock predicate is
// not just "a lock row exists": a released or never-locked row must not redirect a
// host away from a session it still owns.
func TestD1BindSessionRouteIgnoresReleasedSourceLocks(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 41, "source", "stale-target", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigration(t, conn, 42, "source", "unlocked-target", "copy_then_lock_source", "copied", "")
	// An unlocked lock, and a lock that was explicitly released, are both absent
	// from the routing view.
	d1SeedSessionMigrationLock(t, conn, 41, "source", "stale-target", "migrated_away", "2026-01-01T00:00:00.000Z", 0, nil)
	d1SeedSessionMigrationLock(t, conn, 42, "source", "unlocked-target", "migrated_away", "2026-01-02T00:00:00.000Z", 1, "2026-01-03T00:00:00.000Z")

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "source",
	})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if result.LockedSourceRedirect || result.Binding.CanonicalSessionID != "source" {
		t.Fatalf("a released lock must not redirect: %+v", result.Binding)
	}
	if result.Binding.RedirectMigrationID != 0 || result.Binding.RedirectedFromSessionID != "" {
		t.Errorf("no redirect provenance may be recorded: %+v", result.Binding)
	}
}

// TestD1BindSessionRoutePrefersNewestLockByClockNotText pins the lock ordering.
// locked_at is RFC3339 text whose fractional part is not zero padded, so a raw
// string order would rank '.05Z' above '.1Z'; the julianday order used here ranks
// them by the clock.
func TestD1BindSessionRoutePrefersNewestLockByClockNotText(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 41, "source", "late-target", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigration(t, conn, 42, "source", "text-later-target", "copy_then_lock_source", "copied", "")
	// The second row is inserted later (higher id) and sorts later as text, but it
	// is the earlier lock by the clock and must lose.
	d1SeedSessionMigrationLock(t, conn, 41, "source", "late-target", "migrated_away", "2026-01-01T00:00:00.1Z", 1, nil)
	d1SeedSessionMigrationLock(t, conn, 42, "source", "text-later-target", "migrated_away", "2026-01-01T00:00:00.05Z", 1, nil)

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "source",
	})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if result.Binding.CanonicalSessionID != "late-target" || result.Binding.RedirectMigrationID != 41 {
		t.Fatalf("newest lock by clock must win: %+v", result.Binding)
	}
}

func TestD1BindSessionRouteBlocksUnroutableSourceLockState(t *testing.T) {
	for _, tc := range []struct {
		lockStatus string
		wantCode   string
	}{
		{"lock_pending_verification", "source_lock_verification_in_progress"},
		{"lock_released", "source_lock_state_not_routable"},
		{"", "source_lock_state_not_routable"},
	} {
		t.Run("lock_status="+tc.lockStatus, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			d1SeedSessionMigration(t, conn, 41, "locked-source", "canonical-target", "copy_then_lock_source", "copied", "")
			d1SeedSessionMigrationLock(t, conn, 41, "locked-source", "canonical-target", tc.lockStatus, "2026-01-01T00:00:00.000Z", 1, nil)

			result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
				StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "locked-source",
			})
			if result != nil {
				t.Fatalf("a blocked source must not be routed: %+v", result)
			}
			var blocker *SessionMigrationBlockerError
			if !errors.As(err, &blocker) || blocker.Code != tc.wantCode || blocker.Phase != "session_route" {
				t.Fatalf("error = %v, want blocker %s", err, tc.wantCode)
			}
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_route_bindings`); got != 0 {
				t.Errorf("a blocked route must not persist a binding, rows = %d", got)
			}
		})
	}
}

func TestD1BindSessionRouteRejectsUnusableRedirectTargets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		target  string
		wantMsg string
	}{
		{"self redirect", "source", "invalid session route lock redirect"},
		{"blank redirect", "   ", "invalid session route lock redirect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			d1SeedSessionMigration(t, conn, 41, "source", tc.target, "copy_then_lock_source", "copied", "")
			d1SeedSessionMigrationLock(t, conn, 41, "source", tc.target, "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)

			result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
				StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "source",
			})
			if result != nil || err == nil || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("result=%+v err=%v, want %q", result, err, tc.wantMsg)
			}
		})
	}
}

func TestD1BindSessionRouteRejectsRedirectCycle(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 41, "hop-a", "hop-b", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigration(t, conn, 42, "hop-b", "hop-a", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigrationLock(t, conn, 41, "hop-a", "hop-b", "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)
	d1SeedSessionMigrationLock(t, conn, 42, "hop-b", "hop-a", "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "hop-a",
	})
	if result != nil || err == nil || !strings.Contains(err.Error(), "session route lock redirect cycle") {
		t.Fatalf("result=%+v err=%v, want a redirect cycle rejection", result, err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_route_bindings`); got != 0 {
		t.Errorf("a cyclic lock chain must not persist a binding, rows = %d", got)
	}
}

// TestD1BindSessionRouteRejectsRedirectBeyondDepthBound pins the walk bound: a
// chain within the bound resolves, and one hop past it is reported as a corrupt
// ledger rather than followed further.
func TestD1BindSessionRouteRejectsRedirectBeyondDepthBound(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	chain := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		chain = append(chain, "hop-"+string(rune('a'+i)))
	}
	// Seven hops stay inside the bound.
	for i := 0; i < 7; i++ {
		d1SeedSessionMigration(t, conn, int64(200+i), chain[i], chain[i+1], "copy_then_lock_source", "copied", "")
		d1SeedSessionMigrationLock(t, conn, int64(200+i), chain[i], chain[i+1], "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)
	}
	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: chain[0],
	})
	if err != nil {
		t.Fatalf("seven-hop bind: %v", err)
	}
	if result.Binding.CanonicalSessionID != chain[7] {
		t.Errorf("seven hops must resolve to %q, got %q", chain[7], result.Binding.CanonicalSessionID)
	}

	// The eighth hop exceeds the bound and must be reported.
	d1SeedSessionMigration(t, conn, 299, chain[7], "hop-i", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigrationLock(t, conn, 299, chain[7], "hop-i", "migrated_away", "2026-01-01T00:00:00.000Z", 1, nil)
	over, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "other-character", HostChatID: "chat-opaque", RequestedSessionID: chain[0],
	})
	if over != nil || err == nil || !strings.Contains(err.Error(), "session route lock redirect depth exceeded") {
		t.Fatalf("result=%+v err=%v, want a depth rejection", over, err)
	}
}

// ---------------------------------------------------------------------------
// BindSessionRoute: validation and read-only mode
// ---------------------------------------------------------------------------

func TestD1BindSessionRouteValidationRejectsIncompleteRequests(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		req     SessionRouteBindingRequest
		wantMsg string
	}{
		{"no identity", SessionRouteBindingRequest{HostChatID: "chat", RequestedSessionID: "s"},
			"stable_character_id and host_chat_id are required"},
		{"no host chat", SessionRouteBindingRequest{StableCharacterID: "char", RequestedSessionID: "s"},
			"stable_character_id and host_chat_id are required"},
		{"blank identity is not trimmed away", SessionRouteBindingRequest{StableCharacterID: "   ", HostChatID: "  ", RequestedSessionID: "s"},
			"stable_character_id and host_chat_id are required"},
		{"unsupported mode", SessionRouteBindingRequest{StableCharacterID: "char", HostChatID: "chat", RequestedSessionID: "s", Mode: "rewrite"},
			`unsupported session route binding mode "rewrite"`},
		{"no requested session", SessionRouteBindingRequest{StableCharacterID: "char", HostChatID: "chat"},
			"requested_session_id is required when no durable route binding exists"},
		{"blank requested session", SessionRouteBindingRequest{StableCharacterID: "char", HostChatID: "chat", RequestedSessionID: "   "},
			"requested_session_id is required when no durable route binding exists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := st.BindSessionRoute(ctx, tc.req)
			if result != nil || err == nil || err.Error() != tc.wantMsg {
				t.Fatalf("result=%+v err=%v, want %q", result, err, tc.wantMsg)
			}
		})
	}
}

// TestD1BindSessionRouteAcceptsEveryDeclaredMode guards against the shared mode
// vocabulary and the D1 validator drifting apart.
func TestD1BindSessionRouteAcceptsEveryDeclaredMode(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	for _, mode := range []string{
		SessionRouteBindingModeResolveOrCreate,
		SessionRouteBindingModeManualAttach,
		SessionRouteBindingModeMigrationCommit,
		SessionRouteBindingModeLegacyPromotion,
	} {
		if _, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
			StableCharacterID: "char-" + mode, HostChatID: "chat", RequestedSessionID: "session", Mode: mode,
		}); err != nil {
			t.Fatalf("mode %s: %v", mode, err)
		}
	}
}

func TestD1BindSessionRouteResolveExistingReturnsExactReadOnlyBinding(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
		"character-stable", "parent-chat", "parent-session", "active", "existing_readback", 4)

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "parent-chat",
		RequestedSessionID: "ignored-session",
		Mode:               SessionRouteBindingModeResolveExisting,
	})
	if err != nil {
		t.Fatalf("resolve_existing: %v", err)
	}
	if result.Created || result.Updated || result.LockedSourceRedirect || !result.ReadbackVerified {
		t.Fatalf("resolve_existing result = %+v", result)
	}
	if result.Binding.CanonicalSessionID != "parent-session" || result.Binding.Revision != 4 {
		t.Fatalf("binding = %+v", result.Binding)
	}
	// The requested session must not have been adopted by a read-only resolve.
	if stored := d1ReadBinding(t, conn, "character-stable", "parent-chat"); stored.Revision != 4 {
		t.Errorf("resolve_existing must not write: %+v", stored)
	}
}

func TestD1BindSessionRouteResolveExistingNeverCreatesMissingParent(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "missing-parent-chat",
		RequestedSessionID: "requested-session",
		Mode:               SessionRouteBindingModeResolveExisting,
	})
	if result != nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("result=%+v err=%v, want ErrNotFound", result, err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_route_bindings`); got != 0 {
		t.Errorf("resolve_existing must not create a binding, rows = %d", got)
	}
}

func TestD1BindSessionRouteResolveExistingRejectsUnverifiedBindings(t *testing.T) {
	for _, tc := range []struct {
		name            string
		contractVersion string
		state           string
		canonical       string
	}{
		{"stale contract", "session-route-binding.v0", "active", "parent-session"},
		{"released binding", SessionRouteBindingContractVersion, "released", "parent-session"},
		{"blank canonical session", SessionRouteBindingContractVersion, "active", "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			d1SeedRouteBinding(t, conn, tc.contractVersion,
				"character-stable", "parent-chat", tc.canonical, tc.state, "existing_readback", 4)

			result, err := st.BindSessionRoute(ctx, SessionRouteBindingRequest{
				StableCharacterID: "character-stable", HostChatID: "parent-chat",
				Mode: SessionRouteBindingModeResolveExisting,
			})
			if result != nil || err == nil || !strings.Contains(err.Error(), "session route binding readback mismatch") {
				t.Fatalf("result=%+v err=%v, want a readback mismatch", result, err)
			}
		})
	}
}

// racingD1Conn applies a mutation right after a statement commits, modelling a
// concurrent writer that lands between this store's decision and its readback.
// D1 has no row lock, so the readback comparison is the only thing standing
// between a concurrent decision and a silently merged route.
type racingD1Conn struct {
	D1Conn
	afterExec func(ctx context.Context)
}

func (c *racingD1Conn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	n, err := c.D1Conn.Exec(ctx, query, args...)
	if err == nil && c.afterExec != nil {
		c.afterExec(ctx)
	}
	return n, err
}

// TestD1BindSessionRouteReadbackMismatchFailsLoudly proves the guard is real: when
// another writer re-points the identity between the decision and the readback, the
// call reports the divergence instead of returning a binding it did not write.
func TestD1BindSessionRouteReadbackMismatchFailsLoudly(t *testing.T) {
	base, conn := newD1TestStore(t)
	ctx := context.Background()
	racing := &racingD1Conn{
		D1Conn: conn,
		afterExec: func(ctx context.Context) {
			d1Exec(t, base.conn, `UPDATE session_route_bindings SET canonical_session_id = 'concurrent-session'
				WHERE stable_character_id = 'character-stable' AND host_chat_id = 'chat-opaque'`)
		},
	}

	result, err := (&d1Store{conn: racing}).BindSessionRoute(ctx, SessionRouteBindingRequest{
		StableCharacterID: "character-stable", HostChatID: "chat-opaque", RequestedSessionID: "session-canonical",
	})
	if result != nil || err == nil || !strings.Contains(err.Error(), "session route binding readback mismatch") {
		t.Fatalf("result=%+v err=%v, want a readback mismatch", result, err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_route_bindings`); got != 1 {
		t.Errorf("rows = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// GetSessionRoutingBaseline
// ---------------------------------------------------------------------------

func TestD1GetSessionRoutingBaselineRejectsBlankTarget(t *testing.T) {
	st, _ := newD1TestStore(t)
	for _, target := range []string{"", "   "} {
		if baseline, err := st.GetSessionRoutingBaseline(context.Background(), target); baseline != nil || !errors.Is(err, ErrNotFound) {
			t.Fatalf("target %q: baseline=%+v err=%v, want ErrNotFound", target, baseline, err)
		}
	}
}

func TestD1GetSessionRoutingBaselineResolvesCopiedTurns(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 7, "source-session", "target-session", "copy_then_lock_source", "copied", "")
	for _, turn := range []int{0, 1, 2, 5} {
		target := d1SeedChatLog(t, conn, "target-session", turn, "user", "copied")
		d1SeedChatRowMap(t, conn, 7, int64(turn+1), target, "copied")
	}

	baseline, err := st.GetSessionRoutingBaseline(ctx, "target-session")
	if err != nil {
		t.Fatalf("GetSessionRoutingBaseline: %v", err)
	}
	if baseline.MigrationID != 7 || baseline.SourceSessionID != "source-session" ||
		baseline.TargetSessionID != "target-session" || baseline.Mode != "copy_then_lock_source" ||
		baseline.ImportedThroughTurn != 5 {
		t.Fatalf("baseline = %+v", baseline)
	}
}

// TestD1GetSessionRoutingBaselineIgnoresRolledBackRowsAndMigrations pins both
// exclusions: a rolled-back row map entry does not extend the boundary, and a
// rolled-back migration contributes no baseline at all.
func TestD1GetSessionRoutingBaselineIgnoresRolledBackRowsAndMigrations(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 7, "live-source", "target-session", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigration(t, conn, 8, "dead-source", "target-session", "copy_then_lock_source", "rolled_back", "")
	d1SeedSessionMigration(t, conn, 9, "partial-source", "target-session", "copy_then_lock_source", "rollback_partial", "")
	live := d1SeedChatLog(t, conn, "target-session", 3, "user", "live")
	d1SeedChatRowMap(t, conn, 7, 1, live, "copied")
	for i, id := range []int64{8, 9} {
		rolled := d1SeedChatLog(t, conn, "target-session", 9+i, "user", "rolled")
		d1SeedChatRowMap(t, conn, id, 1, rolled, "rolled_back")
	}

	baseline, err := st.GetSessionRoutingBaseline(ctx, "target-session")
	if err != nil {
		t.Fatalf("GetSessionRoutingBaseline: %v", err)
	}
	if baseline.MigrationID != 7 || baseline.ImportedThroughTurn != 3 {
		t.Fatalf("baseline = %+v, want the live migration and its copied turns", baseline)
	}

	// A rolled-back row map entry on the live migration must not extend it.
	d1SeedChatRowMap(t, conn, 7, 2, d1SeedChatLog(t, conn, "target-session", 11, "user", "undone"), "rolled_back")
	if baseline, err = st.GetSessionRoutingBaseline(ctx, "target-session"); err != nil || baseline.ImportedThroughTurn != 3 {
		t.Fatalf("rolled_back row map entries must not extend the boundary: %+v err=%v", baseline, err)
	}
}

// TestD1GetSessionRoutingBaselineIgnoresRowsOutsideTargetSession pins the join
// guard: a row map entry pointing at a chat log in a different session must not
// raise the imported boundary of the target.
func TestD1GetSessionRoutingBaselineIgnoresRowsOutsideTargetSession(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 7, "source-session", "target-session", "copy_then_lock_source", "copied", "")
	mine := d1SeedChatLog(t, conn, "target-session", 2, "user", "mine")
	elsewhere := d1SeedChatLog(t, conn, "other-session", 40, "user", "elsewhere")
	d1SeedChatRowMap(t, conn, 7, 1, mine, "copied")
	d1SeedChatRowMap(t, conn, 7, 2, elsewhere, "copied")

	baseline, err := st.GetSessionRoutingBaseline(ctx, "target-session")
	if err != nil {
		t.Fatalf("GetSessionRoutingBaseline: %v", err)
	}
	if baseline.ImportedThroughTurn != 2 {
		t.Fatalf("imported_through_turn = %d, want 2 (the other session's turn must not count)", baseline.ImportedThroughTurn)
	}
}

// TestD1GetSessionRoutingBaselinePrefersNewestImportingMigration pins that the
// newest migration wins and that the "must have imported a turn" gate is applied
// before that ordering: a newer migration that imported nothing must not mask the
// migration that actually holds the archive.
func TestD1GetSessionRoutingBaselinePrefersNewestImportingMigration(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 3, "older-source", "target-session", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigration(t, conn, 4, "newer-source", "target-session", "copy_then_lock_source", "copied", "")
	d1SeedSessionMigration(t, conn, 5, "starter-source", "target-session", "copy_then_lock_source", "copied", "")
	d1SeedChatRowMap(t, conn, 3, 1, d1SeedChatLog(t, conn, "target-session", 4, "user", "older"), "copied")
	d1SeedChatRowMap(t, conn, 4, 1, d1SeedChatLog(t, conn, "target-session", 9, "user", "newer"), "copied")
	// Migration 5 only copied the starter row, so it contributes no boundary.
	d1SeedChatRowMap(t, conn, 5, 1, d1SeedChatLog(t, conn, "target-session", 0, "assistant", "starter"), "copied")

	baseline, err := st.GetSessionRoutingBaseline(ctx, "target-session")
	if err != nil {
		t.Fatalf("GetSessionRoutingBaseline: %v", err)
	}
	if baseline.MigrationID != 4 || baseline.SourceSessionID != "newer-source" || baseline.ImportedThroughTurn != 9 {
		t.Fatalf("baseline = %+v, want the newest importing migration", baseline)
	}
}

func TestD1GetSessionRoutingBaselineRequiresImportedTurnAboveZero(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 7, "source-session", "target-session", "copy_then_lock_source", "copied", "")
	d1SeedChatRowMap(t, conn, 7, 1, d1SeedChatLog(t, conn, "target-session", 0, "assistant", "starter"), "copied")

	baseline, err := st.GetSessionRoutingBaseline(ctx, "target-session")
	if baseline != nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("baseline=%+v err=%v, want ErrNotFound for a starter-only migration", baseline, err)
	}
}

func TestD1GetSessionRoutingBaselineStitchUsesCurrentOffsetAndSources(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	note, err := json.Marshal(SessionStitchResult{
		TargetSessionID:         "stitch-target",
		CurrentOffset:           17,
		CurrentSourceSessionID:  "current-source",
		CurrentSourceSessionIDs: []string{"source-a", "source-b"},
		CurrentInputGroupAliases: map[string][]string{
			"group-1": {"source-a"},
		},
		Segments: []SessionStitchSegment{
			{SessionID: "source-a", Ordinal: 0, Offset: 0, ThroughTurn: 6},
			{SessionID: "current-source", Ordinal: 1, Offset: 10, ThroughTurn: 17},
		},
	})
	if err != nil {
		t.Fatalf("marshal stitch note: %v", err)
	}
	d1SeedSessionMigration(t, conn, 11, "source-a", "stitch-target", SessionMigrationModeStitch, "copied", string(note))
	// Only the starter row is mapped, so the row-map boundary is 0 and the row is
	// admitted by the stitch disjunct alone. The note is then the authority.
	d1SeedChatRowMap(t, conn, 11, 1, d1SeedChatLog(t, conn, "stitch-target", 0, "assistant", "starter"), "copied")

	baseline, err := st.GetSessionRoutingBaseline(ctx, "stitch-target")
	if err != nil {
		t.Fatalf("GetSessionRoutingBaseline: %v", err)
	}
	if baseline.MigrationID != 11 || baseline.Mode != SessionMigrationModeStitch {
		t.Fatalf("baseline = %+v", baseline)
	}
	if baseline.ImportedThroughTurn != 17 {
		t.Errorf("imported_through_turn = %d, want the note's current offset 17", baseline.ImportedThroughTurn)
	}
	if baseline.SourceSessionID != "current-source" {
		t.Errorf("source_session_id = %q, want current-source", baseline.SourceSessionID)
	}
	if strings.Join(baseline.SourceSessionIDs, ",") != "source-a,source-b" {
		t.Errorf("source_session_ids = %v", baseline.SourceSessionIDs)
	}
	if got := baseline.InputGroupAliases["group-1"]; len(got) != 1 || got[0] != "source-a" {
		t.Errorf("input_group_aliases = %+v", baseline.InputGroupAliases)
	}
}

// TestD1GetSessionRoutingBaselineStitchWithoutSegmentsKeepsRowMapBoundary pins the
// guard on the note: a note that carries no segments is not a stitch boundary, so
// the row-map maximum stands and the source identity is not rewritten.
func TestD1GetSessionRoutingBaselineStitchWithoutSegmentsKeepsRowMapBoundary(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionMigration(t, conn, 11, "source-a", "stitch-target", SessionMigrationModeStitch, "copied",
		`{"current_offset":42,"current_source_session_id":"claimed-source"}`)
	d1SeedChatRowMap(t, conn, 11, 1, d1SeedChatLog(t, conn, "stitch-target", 6, "user", "copied"), "copied")

	baseline, err := st.GetSessionRoutingBaseline(ctx, "stitch-target")
	if err != nil {
		t.Fatalf("GetSessionRoutingBaseline: %v", err)
	}
	if baseline.ImportedThroughTurn != 6 {
		t.Errorf("imported_through_turn = %d, want the row-map maximum 6", baseline.ImportedThroughTurn)
	}
	if baseline.SourceSessionID != "source-a" {
		t.Errorf("source_session_id = %q, want the migration's own source", baseline.SourceSessionID)
	}
	if len(baseline.SourceSessionIDs) != 0 || len(baseline.InputGroupAliases) != 0 {
		t.Errorf("a note without segments must not populate stitch sources: %+v", baseline)
	}
}
