package store

import (
	"context"
	"strings"
	"testing"
)

// newD1TestStore builds a store on the D1 simulator, which is modernc.org/sqlite:
// the same engine D1 runs. These tests therefore exercise the real SQL, including
// the ON CONFLICT upsert and the composite primary key.

func d1SettingsDoc(enabled bool) string {
	if enabled {
		return `{"contract_version":"multi_agent_settings.v1","enabled":true,"candidate_chars":64000}`
	}
	return `{"contract_version":"multi_agent_settings.v1","enabled":false,"candidate_chars":64000}`
}

// TestD1TurnPreparationSettingsRoundTrip covers the plain save/load/delete cycle.
//
// The delete is asserted to succeed when the document is already absent, because
// restoring a default is a user action that happens more than once and must not
// fail the second time.
func TestD1TurnPreparationSettingsRoundTrip(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	const scope = TurnPreparationScopeMultiAgent
	const key = TurnPreparationDefaultDocumentKey

	payload, found, err := st.LoadTurnPreparationSettings(ctx, scope, key)
	if err != nil {
		t.Fatalf("load before any save: %v", err)
	}
	if found {
		t.Errorf("load before any save reported found=true with payload %q; an unwritten setting must be distinguishable from a written one", payload)
	}

	if err := st.SaveTurnPreparationSettings(ctx, scope, key, []byte(d1SettingsDoc(true))); err != nil {
		t.Fatalf("save: %v", err)
	}
	payload, found, err = st.LoadTurnPreparationSettings(ctx, scope, key)
	if err != nil {
		t.Fatalf("load after save: %v", err)
	}
	if !found {
		t.Fatal("load after save reported found=false")
	}
	if string(payload) != d1SettingsDoc(true) {
		t.Errorf("payload = %s, want it stored verbatim", payload)
	}

	// A second save replaces rather than appending or failing. The local file
	// backend renames over the target, so this must match or a Cloudflare
	// deployment would accumulate documents that MariaDB never had.
	if err := st.SaveTurnPreparationSettings(ctx, scope, key, []byte(d1SettingsDoc(false))); err != nil {
		t.Fatalf("second save: %v", err)
	}
	payload, _, err = st.LoadTurnPreparationSettings(ctx, scope, key)
	if err != nil {
		t.Fatalf("load after second save: %v", err)
	}
	if string(payload) != d1SettingsDoc(false) {
		t.Errorf("payload = %s, want the replacement document, not both", payload)
	}

	if err := st.DeleteTurnPreparationSettings(ctx, scope, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, _ := st.LoadTurnPreparationSettings(ctx, scope, key); found {
		t.Error("document still present after delete")
	}
	// Idempotent: deleting an absent document is a no-op, not a failure.
	if err := st.DeleteTurnPreparationSettings(ctx, scope, key); err != nil {
		t.Errorf("delete of an absent document returned %v; restoring a default must be safe to repeat", err)
	}
}

// TestD1TurnPreparationSettingsScopesAndKeysAreIndependent is the property the
// composite key exists for: the deployment-wide multi-agent document must not
// collide with a per-session body-tracking row, and sessions must not see each
// other's configuration.
func TestD1TurnPreparationSettingsScopesAndKeysAreIndependent(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveTurnPreparationSettings(ctx, TurnPreparationScopeMultiAgent,
		TurnPreparationDefaultDocumentKey, []byte(d1SettingsDoc(true))); err != nil {
		t.Fatalf("save multi_agent: %v", err)
	}
	for _, sessionID := range []string{"sess-a", "sess-b"} {
		if err := st.SaveTurnPreparationSettings(ctx, TurnPreparationScopeBodyTracking,
			sessionID, []byte(`{"contract_version":"body_tracking_settings.v1","enabled":true,"session":"`+sessionID+`"}`)); err != nil {
			t.Fatalf("save body_tracking %s: %v", sessionID, err)
		}
	}

	keys, err := st.ListTurnPreparationSettingsKeys(ctx, TurnPreparationScopeBodyTracking)
	if err != nil {
		t.Fatalf("list body_tracking keys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "sess-a" || keys[1] != "sess-b" {
		t.Errorf("body_tracking keys = %v, want [sess-a sess-b]", keys)
	}
	// The multi-agent document must not appear in a body_tracking listing, and vice
	// versa. Sharing a table is only safe while the scope actually separates them.
	multiKeys, err := st.ListTurnPreparationSettingsKeys(ctx, TurnPreparationScopeMultiAgent)
	if err != nil {
		t.Fatalf("list multi_agent keys: %v", err)
	}
	if len(multiKeys) != 1 || multiKeys[0] != TurnPreparationDefaultDocumentKey {
		t.Errorf("multi_agent keys = %v, want just [%s]", multiKeys, TurnPreparationDefaultDocumentKey)
	}

	payload, found, err := st.LoadTurnPreparationSettings(ctx, TurnPreparationScopeBodyTracking, "sess-a")
	if err != nil {
		t.Fatalf("load sess-a: %v", err)
	}
	if !found || !strings.Contains(string(payload), "sess-a") {
		t.Errorf("sess-a payload = %q, want its own document", payload)
	}
	// The identical document_key in a different scope is a different document.
	payload, found, err = st.LoadTurnPreparationSettings(ctx, TurnPreparationScopeBodyTracking, TurnPreparationDefaultDocumentKey)
	if err != nil {
		t.Fatalf("load default key in body_tracking scope: %v", err)
	}
	if found {
		t.Errorf("body_tracking/%s reported found with %q; the scope must isolate the deployment-wide document", TurnPreparationDefaultDocumentKey, payload)
	}
}

// TestD1TurnPreparationSettingsRejectsInvalidJSON guards the one way this store
// could destroy a user's configuration permanently.
//
// A document that does not parse can never be loaded back, and the settings
// handler's merge step would then have nothing to work from. Refusing the write
// turns a permanent silent loss into a visible error at the moment of the save.
func TestD1TurnPreparationSettingsRejectsInvalidJSON(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	const scope = TurnPreparationScopeMultiAgent
	const key = TurnPreparationDefaultDocumentKey

	if err := st.SaveTurnPreparationSettings(ctx, scope, key, []byte(d1SettingsDoc(true))); err != nil {
		t.Fatalf("save valid: %v", err)
	}
	if err := st.SaveTurnPreparationSettings(ctx, scope, key, []byte(`{"enabled":`)); err == nil {
		t.Error("save accepted a truncated JSON document; the value it replaced would then be unrecoverable")
	}
	// The rejected write must not have disturbed the stored document.
	payload, found, err := st.LoadTurnPreparationSettings(ctx, scope, key)
	if err != nil {
		t.Fatalf("load after rejected save: %v", err)
	}
	if !found || string(payload) != d1SettingsDoc(true) {
		t.Errorf("payload = %q (found=%t), want the previous document intact", payload, found)
	}
}

// TestD1TurnPreparationSettingsAreExcludedFromReset pins the parity decision that
// is easiest to get wrong later.
//
// The reset allowlist must match MariaDB's exactly, and MariaDB has no row for
// this data because the local runtime keeps it in a file that the reset never
// touched. So a user keeps their agent configuration across a reset on both
// providers. If this table were ever added to the allowlist, Cloudflare would
// delete settings the local runtime keeps, which is the silent divergence the
// parity work exists to remove.
func TestD1TurnPreparationSettingsAreExcludedFromReset(t *testing.T) {
	st, conn := newD1TestStore(t)

	for _, table := range d1AdminResetTables {
		if table == "d1_turn_preparation_settings" {
			t.Fatal("d1_turn_preparation_settings is in the reset allowlist; MariaDB has no such row and its reset never deleted the settings file, so a reset would now erase settings the local runtime keeps")
		}
	}
	if d1ResetReservedName("d1_turn_preparation_settings") {
		t.Error("d1_turn_preparation_settings is treated as reset control plane; it is application data that is merely excluded from the allowlist, and mislabelling it would let a future change delete it as control plane state")
	}
	// The table must actually exist, or the capability is advertised and then
	// fails on its first write against a real deployment.
	var name string
	if err := conn.QueryRow(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'd1_turn_preparation_settings'`).Scan(&name); err != nil {
		t.Fatalf("d1_turn_preparation_settings is absent from the D1 schema: %v; apply deploy/cloudflare/migrations/003_turn_preparation_settings.sql", err)
	}
	// And the capability must be present on the store, or the settings handler
	// would silently fall back to the filesystem on a Cloudflare deployment.
	if _, ok := interface{}(st).(TurnPreparationSettingsStore); !ok {
		t.Error("the D1 store does not implement TurnPreparationSettingsStore, so the settings handler would use the filesystem on Cloudflare")
	}
}
