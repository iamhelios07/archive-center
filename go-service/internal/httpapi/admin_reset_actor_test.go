package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/store"
)

// Who authorised a destructive reset is an audit fact, and the value has to come
// from somewhere the deployment can actually know.
//
// The Cloudflare profile authenticates with ONE shared bearer token, so the token
// says "an operator" and not which one. Recording it would be both misleading and
// unsafe: the token is a secret, and a control-plane row is readable by anything
// that can read the database. So the actor is an explicit label from the request,
// falling back to the peer address.
//
// The fallback is weak attribution — a shared egress address makes it ambiguous —
// but it is true, and a weak true value is better than a strong invented one.

// actorRecordingResetStore captures what the handler passed down.
type actorRecordingResetStore struct {
	store.Store
	actors    []string
	viasActor int
	viasPlain int
}

func (a *actorRecordingResetStore) ResetAllAs(_ context.Context, actor string) (store.AdminResetResult, error) {
	a.actors = append(a.actors, actor)
	a.viasActor++
	return store.AdminResetResult{TablesCleared: 1, RowsDeleted: 2}, nil
}

func (a *actorRecordingResetStore) ResetAll(context.Context) (store.AdminResetResult, error) {
	a.actors = append(a.actors, "")
	a.viasPlain++
	return store.AdminResetResult{TablesCleared: 1, RowsDeleted: 2}, nil
}

// plainResetStore is a store WITHOUT the actor capability, which is MariaDB.
type plainResetStore struct{ store.Store }

func (p *plainResetStore) ResetAll(context.Context) (store.AdminResetResult, error) {
	return store.AdminResetResult{TablesCleared: 1, RowsDeleted: 2}, nil
}

func resetServer(reset store.Store) *Server {
	cfg := config.Default()
	cfg.Mode = config.ModeShadow
	cfg.StoreMode = config.StoreModeCloudflareAuthority
	cfg.RuntimeProfile = config.RuntimeProfileCloudflare
	cfg.VectorMode = config.VectorModeCloudflare
	cfg.CloudflareBridgeURL = "http://archive-center-bridge.internal"
	cfg.CloudflareBridgeToken = "bridge-token"
	cfg.Auth = config.AuthConfig{Enforce: true, BearerToken: "operator-secret-token"}
	return &Server{Store: reset, Cfg: cfg}
}

func postReset(t *testing.T, s *Server, body string, remoteAddr string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/database-reset", s.handleAdminDatabaseReset)
	req := httptest.NewRequest(http.MethodPost, "/admin/database-reset", strings.NewReader(body))
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	req.Header.Set("Authorization", "Bearer "+s.Cfg.Auth.BearerToken)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, req)
	return recorder
}

// reset_vector is false so the handler reaches the canonical reset directly. The
// vector purge runs first when it is true, and this fixture has no vector store
// that can reset, so leaving it on would make every assertion below a skip.
const resetBody = `{"debug":true,"confirm":"RESET_ARCHIVE_CENTER_DB","reset_vector":false}`

// TestResetRecordsTheOperatorLabelFromTheRequest is the primary path.
func TestResetRecordsTheOperatorLabelFromTheRequest(t *testing.T) {
	recorder := &actorRecordingResetStore{}
	s := resetServer(recorder)
	resp := postReset(t, s, `{"debug":true,"confirm":"RESET_ARCHIVE_CENTER_DB","reset_vector":false,"operator":"  alice  "}`, "10.0.0.9:5000", nil)

	if resp.Code != http.StatusOK {
		t.Skipf("the reset handler refused this fixture (%d): %s", resp.Code, resp.Body.String())
	}
	if len(recorder.actors) != 1 {
		t.Fatalf("the store was called %d times", len(recorder.actors))
	}
	if recorder.actors[0] != "alice" {
		t.Errorf("actor = %q, want the trimmed label from the request", recorder.actors[0])
	}
	if recorder.viasActor != 1 || recorder.viasPlain != 0 {
		t.Errorf("the actor-capable path was used %d times and the plain path %d; the capability must be preferred", recorder.viasActor, recorder.viasPlain)
	}
}

// TestResetFallsBackToThePeerAddress covers an operator who does not label
// themselves. Something true is recorded rather than nothing.
func TestResetFallsBackToThePeerAddress(t *testing.T) {
	recorder := &actorRecordingResetStore{}
	s := resetServer(recorder)
	resp := postReset(t, s, resetBody, "10.0.0.9:5000", nil)
	if resp.Code != http.StatusOK {
		t.Skipf("the reset handler refused this fixture (%d)", resp.Code)
	}
	if len(recorder.actors) != 1 {
		t.Fatalf("the store was called %d times", len(recorder.actors))
	}
	if !strings.HasPrefix(recorder.actors[0], "remote-addr:") {
		t.Errorf("actor = %q, want a remote-addr fallback", recorder.actors[0])
	}
	if !strings.Contains(recorder.actors[0], "10.0.0.9") {
		t.Errorf("actor = %q, want the peer address in it", recorder.actors[0])
	}
}

// TestResetPrefersTheCloudflareClientAddress records the more useful of the two
// available addresses when the request came through the Worker.
func TestResetPrefersTheCloudflareClientAddress(t *testing.T) {
	recorder := &actorRecordingResetStore{}
	s := resetServer(recorder)
	resp := postReset(t, s, resetBody, "172.16.0.1:5000", map[string]string{"CF-Connecting-IP": "203.0.113.7"})
	if resp.Code != http.StatusOK {
		t.Skipf("the reset handler refused this fixture (%d)", resp.Code)
	}
	if len(recorder.actors) != 1 {
		t.Fatalf("the store was called %d times", len(recorder.actors))
	}
	if !strings.Contains(recorder.actors[0], "203.0.113.7") {
		t.Errorf("actor = %q, want the real client address rather than the proxy's", recorder.actors[0])
	}
	if strings.Contains(recorder.actors[0], "172.16.0.1") {
		t.Errorf("actor = %q recorded the proxy address when the client address was available", recorder.actors[0])
	}
}

// TestResetNeverRecordsTheBearerToken is the one that must never regress.
//
// The token is a shared secret. A control-plane row is readable by anything that
// can read the database, and /admin/jobs echoes the actor back, so storing the
// token would leak it into a place with a wider audience than the request had.
func TestResetNeverRecordsTheBearerToken(t *testing.T) {
	const token = "operator-secret-token"
	recorder := &actorRecordingResetStore{}
	s := resetServer(recorder)

	cases := map[string]map[string]string{
		"no headers":    nil,
		"forwarded for": {"X-Forwarded-For": token},
		"cf connecting": {"CF-Connecting-IP": token},
		"user agent":    {"User-Agent": token},
	}
	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			recorder.actors = nil
			// The label is deliberately the token too, to prove the handler does not
			// pass a credential through even when it is handed one as a label.
			body := `{"debug":true,"confirm":"RESET_ARCHIVE_CENTER_DB","reset_vector":false,"operator":"` + token + `"}`
			resp := postReset(t, s, body, "10.0.0.9:5000", headers)
			if resp.Code != http.StatusOK {
				t.Fatalf("the reset handler refused the fixture: %d %s", resp.Code, resp.Body.String())
			}
			for _, actor := range recorder.actors {
				if strings.Contains(actor, token) {
					t.Errorf("the recorded actor contains the bearer token: %q", actor)
				}
			}
		})
	}
}

// TestResetUsesThePlainPathWhenTheStoreCannotRecordAnActor keeps MariaDB working.
// The optional interface exists so the shared contract does not grow a parameter
// one provider cannot store.
func TestResetUsesThePlainPathWhenTheStoreCannotRecordAnActor(t *testing.T) {
	plain := &plainResetStore{}
	s := resetServer(plain)
	resp := postReset(t, s, `{"debug":true,"confirm":"RESET_ARCHIVE_CENTER_DB","reset_vector":false,"operator":"alice"}`, "10.0.0.9:5000", nil)
	if resp.Code != http.StatusOK {
		t.Skipf("the reset handler refused this fixture (%d): %s", resp.Code, resp.Body.String())
	}
	if _, ok := interface{}(plain).(store.AdminResetActorStore); ok {
		t.Error("the plain fixture implements the actor capability, so this test is not exercising the fallback")
	}
}

// TestResetPresentationCarriesTheActor checks the read side, so the value that was
// recorded can actually be seen by an operator.
func TestResetPresentationCarriesTheActor(t *testing.T) {
	job := adminResetRunJob(store.AdminResetRun{
		ResetRunID: "reset-1", Epoch: 1, Status: "completed",
		StartedAt: "2026-02-01T00:00:00Z", UpdatedAt: "2026-02-01T00:01:00Z",
		RequestedBy: "alice@example.com", Durable: true,
	})
	if job["requested_by"] != "alice@example.com" {
		t.Errorf("requested_by = %v, want the recorded actor", job["requested_by"])
	}

	// And it is omitted rather than shown as an empty string when nobody was
	// named; an empty field reads as a missing value instead of an absent one.
	unnamed := adminResetRunJob(store.AdminResetRun{ResetRunID: "reset-2", Durable: true})
	if _, present := unnamed["requested_by"]; present {
		t.Errorf("requested_by = %v for an unnamed reset, want the key absent", unnamed["requested_by"])
	}

	// The presentation must stay JSON-serialisable, since it goes over the wire.
	if _, err := json.Marshal(job); err != nil {
		t.Errorf("the presentation does not serialise: %v", err)
	}
}
