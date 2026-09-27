package cloudflarebridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestServer returns a bridge test server that asserts the request
// envelope and answers with the provided status and response body.
func newTestServer(t *testing.T, token string, status int, response string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("bridge method = %q, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("bridge authorization = %q, want %q", got, "Bearer "+token)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("bridge content-type = %q, want application/json", got)
		}
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("bridge request is not a JSON envelope: %v", err)
			return
		}
		if req.Version != EnvelopeVersion {
			t.Errorf("bridge request version = %d, want %d", req.Version, EnvelopeVersion)
		}
		if strings.TrimSpace(req.ID) == "" {
			t.Error("bridge request id must not be empty")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(response))
	}))
}

func TestNewClientRejectsInvalidConfig(t *testing.T) {
	if _, err := NewClient("", "token", 0); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("empty URL: err = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewClient("http://bridge.internal", " ", 0); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("empty token: err = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewClient("ftp://bridge.internal", "token", 0); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("non-http scheme: err = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewClient("not a url", "token", 0); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("unparseable URL: err = %v, want ErrInvalidConfig", err)
	}
}

func TestDoRoundTripPreservesEnvelopeContract(t *testing.T) {
	const token = "bridge-token"
	var capturedID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		capturedID = req.ID
		if req.Operation != OpBridgePing {
			t.Errorf("operation = %q, want %q", req.Operation, OpBridgePing)
		}
		if string(req.Payload) != `{"echo":true}` {
			t.Errorf("payload = %s, want %q", req.Payload, `{"echo":true}`)
		}
		_ = json.NewEncoder(w).Encode(Response{
			Version: EnvelopeVersion,
			ID:      req.ID,
			OK:      true,
			Result:  json.RawMessage(`{"pong":true}`),
		})
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL, token, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	var result struct {
		Pong bool `json:"pong"`
	}
	if err := client.Do(context.Background(), OpBridgePing, map[string]bool{"echo": true}, &result); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !result.Pong {
		t.Error("result.pong = false, want true")
	}
	if capturedID == "" {
		t.Error("request id was not captured")
	}
}

func TestDoMapsHTTPStatusesToSentinelErrors(t *testing.T) {
	cases := []struct {
		status    int
		response  string
		wantErr   error
		wantRetry bool
	}{
		{http.StatusUnauthorized, `{"ok":false}`, ErrUnauthorized, false},
		{http.StatusForbidden, `{"ok":false}`, ErrForbidden, false},
		{http.StatusNotFound, `{"ok":false}`, ErrNotFound, false},
		{http.StatusTooManyRequests, `{"ok":false}`, ErrRateLimited, true},
		{http.StatusInternalServerError, `{"ok":false}`, ErrUnavailable, true},
		{http.StatusBadGateway, `{"ok":false}`, ErrUnavailable, true},
	}
	for _, tc := range cases {
		srv := newTestServer(t, "bridge-token", tc.status, tc.response)
		client, err := NewClient(srv.URL, "bridge-token", time.Second)
		if err != nil {
			srv.Close()
			t.Fatalf("NewClient: %v", err)
		}
		err = client.Do(context.Background(), OpD1Query, nil, nil)
		if !errors.Is(err, tc.wantErr) {
			srv.Close()
			t.Errorf("status %d: err = %v, want %v", tc.status, err, tc.wantErr)
			continue
		}
		if got := Retryable(err); got != tc.wantRetry {
			srv.Close()
			t.Errorf("status %d: Retryable = %t, want %t", tc.status, got, tc.wantRetry)
		}
		srv.Close()
	}
}

func TestDoSurfacesBridgeOperationError(t *testing.T) {
	srv := newTestServer(t, "bridge-token", http.StatusOK,
		`{"version":1,"id":"replaced","ok":false,"error_code":"not_implemented","error_message":"d1.query lands in Stage 3","retryable":false}`)
	// The ID check needs a matching id, so use a capturing server instead.
	srv.Close()
	var requestID string
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		requestID = req.ID
		_ = json.NewEncoder(w).Encode(Response{
			Version:      EnvelopeVersion,
			ID:           req.ID,
			OK:           false,
			ErrorCode:    "not_implemented",
			ErrorMessage: "d1.query lands in Stage 3",
			Retryable:    false,
		})
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL, "bridge-token", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	err = client.Do(context.Background(), OpD1Query, nil, nil)
	var bridgeErr *BridgeError
	if !errors.As(err, &bridgeErr) {
		t.Fatalf("err = %v, want *BridgeError", err)
	}
	if bridgeErr.Code != "not_implemented" {
		t.Errorf("code = %q, want not_implemented", bridgeErr.Code)
	}
	if bridgeErr.Retryable {
		t.Error("not_implemented must not be retryable")
	}
	if Retryable(err) {
		t.Error("Retryable(not_implemented) = true, want false")
	}
	if requestID == "" {
		t.Error("request id not captured")
	}
}

func TestDoRejectsEnvelopeProtocolViolations(t *testing.T) {
	const requestIDPlaceholder = "envelope-id"
	cases := []struct {
		name     string
		response string
		wantErr  error
	}{
		{"version_mismatch", `{"version":2,"id":"` + requestIDPlaceholder + `","ok":true}`, ErrVersionMismatch},
		{"id_correlation", `{"version":1,"id":"other-id","ok":true}`, ErrBadEnvelope},
		{"non_json", `not-json`, ErrBadEnvelope},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req Request
			_ = json.NewDecoder(r.Body).Decode(&req)
			body := strings.ReplaceAll(tc.response, requestIDPlaceholder, req.ID)
			_, _ = w.Write([]byte(body))
		}))
		client, err := NewClient(srv.URL, "bridge-token", time.Second)
		if err != nil {
			srv.Close()
			t.Fatalf("NewClient: %v", err)
		}
		if err := client.Do(context.Background(), OpBridgeHealth, nil, nil); !errors.Is(err, tc.wantErr) {
			srv.Close()
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.wantErr)
			continue
		}
		srv.Close()
	}
}

func TestDoMapsDeadlineToTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{"version":1,"id":"x","ok":true}`))
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL, "bridge-token", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = client.Do(ctx, OpBridgePing, nil, nil)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	if !Retryable(err) {
		t.Error("Retryable(ErrTimeout) = false, want true")
	}
}

func TestPingAndHealthHelpers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		var resp Response
		switch req.Operation {
		case OpBridgePing:
			resp = Response{Version: EnvelopeVersion, ID: req.ID, OK: true, Result: json.RawMessage(`{"pong":true}`)}
		case OpBridgeHealth:
			resp = Response{Version: EnvelopeVersion, ID: req.ID, OK: true, Result: json.RawMessage(`{"d1":"bound","vectorize":"bound","version":1}`)}
		default:
			resp = Response{Version: EnvelopeVersion, ID: req.ID, OK: false, ErrorCode: "unknown_operation"}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL, "bridge-token", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	health, err := client.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.D1 != "bound" || health.Vectorize != "bound" {
		t.Errorf("health = %+v, want both bindings bound", health)
	}
}

func TestDoRejectsEmptyOperation(t *testing.T) {
	client, err := NewClient("http://bridge.invalid", "token", time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Do(context.Background(), "  ", nil, nil); !errors.Is(err, ErrBadEnvelope) {
		t.Errorf("err = %v, want ErrBadEnvelope", err)
	}
}
