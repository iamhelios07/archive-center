package httpapi

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/store"
)

// settingsMemoryStore is a TurnPreparationSettingsStore that keeps documents in
// memory, so the dispatch can be observed without a database and without a
// filesystem. It deliberately counts calls: the point of these tests is which
// backend was chosen, not what either one stores.
type settingsMemoryStore struct {
	store.Store
	documents map[string][]byte
	loads     int
	saves     int
}

func newSettingsMemoryStore() *settingsMemoryStore {
	return &settingsMemoryStore{documents: map[string][]byte{}}
}

func settingsKey(scope, key string) string { return scope + "\x00" + key }

func (m *settingsMemoryStore) LoadTurnPreparationSettings(_ context.Context, scope, key string) ([]byte, bool, error) {
	m.loads++
	payload, ok := m.documents[settingsKey(scope, key)]
	return payload, ok, nil
}

func (m *settingsMemoryStore) SaveTurnPreparationSettings(_ context.Context, scope, key string, payload []byte) error {
	m.saves++
	if !json.Valid(payload) {
		return errSettingsDocumentAbsent
	}
	m.documents[settingsKey(scope, key)] = append([]byte(nil), payload...)
	return nil
}

func (m *settingsMemoryStore) DeleteTurnPreparationSettings(_ context.Context, scope, key string) error {
	delete(m.documents, settingsKey(scope, key))
	return nil
}

func (m *settingsMemoryStore) ListTurnPreparationSettingsKeys(_ context.Context, scope string) ([]string, error) {
	keys := []string{}
	prefix := scope + "\x00"
	for k := range m.documents {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k[len(prefix):])
		}
	}
	return keys, nil
}

// TestTurnPreparationSettingsPreferTheStoreWhenItIsAvailable is the whole point of
// this file. A Cloudflare Container has no durable filesystem, so a setting that
// falls through to a file is a setting the user loses the next time the instance
// idles. The dispatch must therefore prefer the store whenever one exists.
func TestTurnPreparationSettingsPreferTheStoreWhenItIsAvailable(t *testing.T) {
	memory := newSettingsMemoryStore()
	s := &Server{Store: memory}

	if s.turnPreparationSettingsStore() == nil {
		t.Fatal("a store implementing TurnPreparationSettingsStore was not detected")
	}
	if !s.turnPreparationSettingsAvailable() {
		t.Error("turnPreparationSettingsAvailable() = false, want true when the store implements the capability")
	}

	if err := s.saveMultiAgentSettings(multiAgentSettings{Enabled: true, CandidateChars: 4096}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if memory.saves != 1 {
		t.Errorf("store saves = %d, want 1; the document went somewhere other than the durable store", memory.saves)
	}
	if _, ok := memory.documents[settingsKey(store.TurnPreparationScopeMultiAgent, store.TurnPreparationDefaultDocumentKey)]; !ok {
		t.Error("the saved document is not under the multi_agent scope in the store")
	}

	loaded, err := s.readMultiAgentSettings()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !loaded.Enabled || loaded.CandidateChars != 4096 {
		t.Errorf("loaded = %+v, want the document that was written to the store", loaded)
	}
	if memory.loads != 1 {
		t.Errorf("store loads = %d, want 1", memory.loads)
	}
}

// TestTurnPreparationSettingsFallBackToTheFileWhenTheStoreCannotPersist is the
// other half. The local runtime has a real data directory, and a store that has not
// implemented the capability must not break it. Reusing a working, backed-up path
// to chase parity on a platform that has a filesystem would be a regression.
func TestTurnPreparationSettingsFallBackToTheFileWhenTheStoreCannotPersist(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("ARCHIVE_CENTER_DATA_DIR", dataDir)

	// A store WITHOUT the capability, which is the local runtime's case.
	s := &Server{Store: store.NewNoopStore()}
	if s.turnPreparationSettingsStore() != nil {
		t.Fatal("a store without the capability was treated as durable")
	}
	if s.turnPreparationSettingsAvailable() {
		t.Error("turnPreparationSettingsAvailable() = true, want false for a store without the capability")
	}

	if err := s.saveMultiAgentSettings(multiAgentSettings{Enabled: true, CandidateChars: 2048}); err != nil {
		t.Fatalf("save: %v", err)
	}
	path := filepath.Join(dataDir, "memory-preprocessing.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the local runtime must still write %s: %v", path, err)
	}

	// A fresh server over the same directory sees the setting, which is what
	// makes it durable in the local deployment.
	reloaded, err := (&Server{Store: store.NewNoopStore()}).readMultiAgentSettings()
	if err != nil {
		t.Fatalf("read from a fresh server: %v", err)
	}
	if !reloaded.Enabled || reloaded.CandidateChars != 2048 {
		t.Errorf("reloaded = %+v, want the persisted document", reloaded)
	}
}

// TestTurnPreparationSettingsSurviveAStoreRestart is the stateless Container
// problem stated as a test.
//
// The Container has no filesystem to rely on, so what matters is only that a NEW
// server instance over the same durable store still sees the setting. If this
// test were written against the filesystem it would pass locally and prove
// nothing about the deployment that actually needs the guarantee.
func TestTurnPreparationSettingsSurviveAStoreRestart(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("ARCHIVE_CENTER_DATA_DIR", dataDir)

	durable := newSettingsMemoryStore()
	first := &Server{Store: durable}
	if err := first.saveMultiAgentSettings(multiAgentSettings{Enabled: true, CandidateChars: 512}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// A different instance, and crucially one whose data directory has been
	// removed from underneath it, the way a scaled-to-zero Container has no layer
	// left to read.
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("remove data dir: %v", err)
	}
	second := &Server{Store: durable}
	loaded, err := second.readMultiAgentSettings()
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !loaded.Enabled || loaded.CandidateChars != 512 {
		t.Errorf("loaded = %+v, want the setting to survive with no filesystem at all", loaded)
	}
}

// TestTurnPreparationSettingsRefuseAnUnreadableDocument guards the failure that
// costs the most: a document that cannot be parsed can never be read back, so the
// settings merge has nothing to work from and the user's configuration is gone
// with no error to point at.
func TestTurnPreparationSettingsRefuseAnUnreadableDocument(t *testing.T) {
	for name, s := range map[string]*Server{
		"store":      {Store: newSettingsMemoryStore()},
		"filesystem": {Store: store.NewNoopStore()},
	} {
		t.Run(name, func(t *testing.T) {
			err := s.saveTurnPreparationDocument(t.Context(),
				store.TurnPreparationScopeMultiAgent, store.TurnPreparationDefaultDocumentKey,
				[]byte(`{"enabled":`))
			if err == nil {
				t.Error("an unparseable document was accepted; it could never be read back and the value it replaced would be unrecoverable")
			}
		})
	}
}

// TestTurnPreparationSettingsRejectAnUnknownScope keeps a typo from silently
// writing to a place no reader looks.
func TestTurnPreparationSettingsRejectAnUnknownScope(t *testing.T) {
	t.Setenv("ARCHIVE_CENTER_DATA_DIR", t.TempDir())
	if _, err := turnPreparationFilePath("multi_agent_typo"); err == nil {
		t.Error("an unknown scope resolved to a path; a typo would write a document nothing reads")
	}
}
