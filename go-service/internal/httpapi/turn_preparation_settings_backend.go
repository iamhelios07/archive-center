package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/risulongmemory/archive-center-go/internal/store"
)

// Turn preparation settings live in one of two places, and which one depends on
// the deployment rather than on the setting.
//
// The local runtime writes memory-preprocessing.json and body-tracking.json next
// to its data directory. That is correct there: a managed local install has a
// real directory and a working backup story.
//
// A Cloudflare Container has neither. There is no managed launcher to hand it a
// persistent directory, and a Container scales to zero, so a file-backed setting
// disappears while the service is idle. The user turns a role on, the write
// succeeds, the file is written into the container layer, and the layer is
// discarded. Nothing errors. The setting simply does not take effect, which reads
// from the outside as the application ignoring the user.
//
// So on the Cloudflare profile the same documents go to D1, through the optional
// TurnPreparationSettingsStore capability. The documents are stored verbatim
// rather than decomposed, so a versioned JSON contract_version keeps its meaning
// and a new setting does not need a migration.
//
// The dispatch is on the CAPABILITY, not on the profile. That is deliberate: the
// local path is not rewritten to reach parity on a platform that has a working
// filesystem, and any other store that grows the capability inherits the durable
// behaviour without a second code path.

// turnPreparationSettingsStore returns the durable settings store when the
// configured store provides one, and nil when it does not.
//
// A nil result is not a degraded mode. It is the local runtime, which uses its
// files, and it is also correct for any store that has not implemented the
// capability yet.
func (s *Server) turnPreparationSettingsStore() store.TurnPreparationSettingsStore {
	if s == nil || s.Store == nil {
		return nil
	}
	capable, ok := s.Store.(store.TurnPreparationSettingsStore)
	if !ok {
		return nil
	}
	return capable
}

// loadTurnPreparationDocument reads one settings document, preferring the durable
// store. found=false means "nobody has changed this yet", which every caller
// answers with its defaults, so it is not an error.
func (s *Server) loadTurnPreparationDocument(ctx context.Context, scope, documentKey string) ([]byte, bool, error) {
	if capable := s.turnPreparationSettingsStore(); capable != nil {
		return capable.LoadTurnPreparationSettings(ctx, scope, documentKey)
	}
	return readTurnPreparationFile(scope, documentKey)
}

// saveTurnPreparationDocument writes one settings document. The payload is
// validated as JSON before it is stored, so an unreadable document is never
// written at all: a document that cannot be read back is a permanent loss of the
// user's configuration with no clue as to when it happened, whereas a rejected
// write is visible at the moment of the save.
func (s *Server) saveTurnPreparationDocument(ctx context.Context, scope, documentKey string, payload []byte) error {
	if !json.Valid(payload) {
		return fmt.Errorf("turn preparation settings: refusing to save %s/%s: payload is not valid JSON", scope, documentKey)
	}
	if capable := s.turnPreparationSettingsStore(); capable != nil {
		return capable.SaveTurnPreparationSettings(ctx, scope, documentKey, payload)
	}
	return writeTurnPreparationFile(scope, documentKey, payload)
}

// deleteTurnPreparationDocument restores a default. A missing document is not an
// error, so this is idempotent.
func (s *Server) deleteTurnPreparationDocument(ctx context.Context, scope, documentKey string) error {
	if capable := s.turnPreparationSettingsStore(); capable != nil {
		return capable.DeleteTurnPreparationSettings(ctx, scope, documentKey)
	}
	return deleteTurnPreparationFile(scope, documentKey)
}

// turnPreparationSettingsAvailable reports whether the deployment persists these
// settings durably. The readiness surface uses it so an operator can tell a
// setting that will survive a restart from one that will not, instead of
// discovering it after an idle period.
func (s *Server) turnPreparationSettingsAvailable() bool {
	return s.turnPreparationSettingsStore() != nil
}

// isD1Absence reports whether err is the store's empty-single-row signal. The
// durable and file backends are asked the same question and answer it
// differently, so both are mapped to one meaning here.
func isD1Absence(err error) bool {
	return err != nil && errors.Is(err, errSettingsDocumentAbsent)
}

var errSettingsDocumentAbsent = errors.New("turn preparation settings: document absent")

// ---------------------------------------------------------------------------
// local runtime file backend
// ---------------------------------------------------------------------------

// turnPreparationScopeFiles maps a scope to the file the local runtime writes. The
// names are kept as the runtime has always spelled them, because a user upgrading
// an existing installation must find their settings where they left them.
//
// This is a map rather than a switch on purpose. The parity manifest guard reads
// a type-switch clause naming a store package value as a capability requirement,
// which is a sound assumption there because the only thing the HTTP layer
// type-asserts is a store interface. These are scope keys, not capabilities, so a
// switch would have registered two phantom requirements in the manifest. The
// syntax was the lie, not the constant names.
//
// The same guard scans raw file text, comments included, so this comment
// deliberately does not reproduce that clause shape: naming it literally would
// register the placeholder as a third phantom capability.
var turnPreparationScopeFiles = map[string]string{
	store.TurnPreparationScopeMultiAgent:   "memory-preprocessing.json",
	store.TurnPreparationScopeBodyTracking: "body-tracking.json",
}

// turnPreparationFilePath resolves a scope to a path next to the multi-agent
// settings, which is the directory the local runtime already creates.
func turnPreparationFilePath(scope string) (string, error) {
	name, known := turnPreparationScopeFiles[scope]
	if !known {
		return "", fmt.Errorf("turn preparation settings: unknown scope %q", scope)
	}
	anchor, err := multiAgentSettingsPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(anchor), name), nil
}

func readTurnPreparationFile(scope, documentKey string) ([]byte, bool, error) {
	path, err := turnPreparationFilePath(scope)
	if err != nil {
		return nil, false, err
	}
	payload, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

func writeTurnPreparationFile(scope, documentKey string, payload []byte) error {
	path, err := turnPreparationFilePath(scope)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	// Write to a temporary file, fsync it, then rename over the target.
	//
	// This is the sequence the local runtime has always used and it is load
	// bearing: rename is atomic on the same filesystem, so a crash between the
	// write and the rename leaves the previous settings intact. Writing in place
	// would leave a truncated file that no longer parses, which is how a user
	// loses their configuration to a power cut.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".turn-settings-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(payload); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}

func deleteTurnPreparationFile(scope, documentKey string) error {
	path, err := turnPreparationFilePath(scope)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
