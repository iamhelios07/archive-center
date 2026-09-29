package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TurnPreparationSettingsStore persists the settings that the local runtime keeps
// in files next to its data directory: the multi-agent document
// (memory-preprocessing.json) and per-session body tracking configuration
// (body-tracking.json).
//
// It exists because a Cloudflare Container is stateless. There is no managed
// launcher to supply a persistent directory, so a file-backed setting is discarded
// on every restart and a Container that scales to zero loses it while idle. The
// failure is silent and looks like the user's setting is being ignored, which is
// strictly worse than the setting not existing.
//
// The documents are stored verbatim rather than decomposed into columns. The local
// files are versioned JSON with a contract_version, and the runtime already treats
// an unrecognised field as a newer version rather than as corruption. Keeping the
// document whole preserves that, and lets a new setting ship without a migration.
//
// The local runtime keeps using its files. A managed local installation has a real
// data directory and a working backup story, and rewriting a working path to reach
// parity on a platform that has the filesystem would be a regression.
type TurnPreparationSettingsStore interface {
	// LoadTurnPreparationSettings returns the document for a scope and key, and
	// reports whether one was stored. A missing document is not an error: it is
	// the state before anyone has changed a setting, and every caller has a
	// default to fall back to.
	LoadTurnPreparationSettings(ctx context.Context, scope, documentKey string) (payload []byte, found bool, err error)

	// SaveTurnPreparationSettings writes the whole document, replacing any
	// previous value. Documents are small and written whole by the local runtime
	// too, so partial field updates are not offered: a merge here would be a
	// second document format to keep compatible.
	SaveTurnPreparationSettings(ctx context.Context, scope, documentKey string, payload []byte) error

	// DeleteTurnPreparationSettings removes one document. A missing document is not
	// an error, so restoring a default is idempotent.
	DeleteTurnPreparationSettings(ctx context.Context, scope, documentKey string) error

	// ListTurnPreparationSettingsKeys returns the document keys stored for a scope.
	// It exists so the per-session scope can be enumerated for migration and export
	// rather than only read by a key the caller happens to already know.
	ListTurnPreparationSettingsKeys(ctx context.Context, scope string) ([]string, error)
}

// Settings scopes. These are storage keys, not user-facing labels, and they are
// named for the files the local runtime writes so the correspondence is obvious
// to anyone comparing the two.
const (
	TurnPreparationScopeMultiAgent   = "multi_agent"
	TurnPreparationScopeBodyTracking = "body_tracking"
)

// TurnPreparationDefaultDocumentKey addresses the single document of a scope that
// holds one deployment-wide value rather than one per session.
const TurnPreparationDefaultDocumentKey = "default"

var _ TurnPreparationSettingsStore = (*d1Store)(nil)

func (s *d1Store) LoadTurnPreparationSettings(ctx context.Context, scope, documentKey string) ([]byte, bool, error) {
	var payload string
	err := s.conn.QueryRow(ctx,
		`SELECT payload_json FROM d1_turn_preparation_settings WHERE scope = ? AND document_key = ?`,
		scope, documentKey).Scan(&payload)
	if errors.Is(err, errD1NoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: load %s/%s: %w", scope, documentKey, err)
	}
	return []byte(payload), true, nil
}

func (s *d1Store) SaveTurnPreparationSettings(ctx context.Context, scope, documentKey string, payload []byte) error {
	// A settings document must be valid JSON before it is stored, not after. An
	// unreadable document is a permanent loss of the user's configuration with no
	// way to tell how it happened, whereas a rejected write is a visible failure at
	// the moment of the save.
	if !json.Valid(payload) {
		return fmt.Errorf("store: refusing to save %s/%s: payload is not valid JSON", scope, documentKey)
	}
	if _, err := s.conn.Exec(ctx, `
		INSERT INTO d1_turn_preparation_settings (scope, document_key, payload_json, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (scope, document_key)
		DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at`,
		scope, documentKey, string(payload), d1TimeValue(time.Time{})); err != nil {
		return fmt.Errorf("store: save %s/%s: %w", scope, documentKey, err)
	}
	return nil
}

func (s *d1Store) DeleteTurnPreparationSettings(ctx context.Context, scope, documentKey string) error {
	if _, err := s.conn.Exec(ctx,
		`DELETE FROM d1_turn_preparation_settings WHERE scope = ? AND document_key = ?`,
		scope, documentKey); err != nil {
		return fmt.Errorf("store: delete %s/%s: %w", scope, documentKey, err)
	}
	return nil
}

func (s *d1Store) ListTurnPreparationSettingsKeys(ctx context.Context, scope string) ([]string, error) {
	rows, err := s.conn.Query(ctx,
		`SELECT document_key FROM d1_turn_preparation_settings WHERE scope = ? ORDER BY document_key`,
		scope)
	if err != nil {
		return nil, fmt.Errorf("store: list %s keys: %w", scope, err)
	}
	defer rows.Close()

	keys := make([]string, 0, 8)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("store: scan %s key: %w", scope, err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate %s keys: %w", scope, err)
	}
	// An empty result is a range with no members, not a failure, and returning
	// nil here would make it indistinguishable from an unexecuted query.
	return keys, nil
}
