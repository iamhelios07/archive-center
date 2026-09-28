// Package cloudflarebridge provides the Container-to-Worker bridge client.
//
// The Go backend reaches D1 and Vectorize only through this virtual-host
// Worker endpoint. The client never talks to the public Cloudflare REST API,
// never holds a Cloudflare API token, and never embeds account or resource
// identifiers: the endpoint URL is an internal Worker virtual hostname and
// the credential is a shared Container-to-Worker bridge token injected from
// the environment.
package cloudflarebridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// EnvelopeVersion is the bridge envelope contract version. The Worker and
// this client must agree on it before any operation is dispatched.
const EnvelopeVersion = 1

// DefaultTimeout bounds one bridge round trip when no explicit timeout is
// configured.
const DefaultTimeout = 30 * time.Second

// maxResponseBytes bounds a bridge response body to prevent unbounded reads.
const maxResponseBytes = 16 << 20

// Operation names. Stage 2 fixes the generic operation contract for canonical
// queries/transactions, vector mutations/queries, and operator jobs. The data
// operations themselves are implemented by the D1 store (Stage 3), the
// Vectorize provider (Stage 4), and the operator surfaces (Stage 5).
const (
	OpBridgePing   = "bridge.ping"
	OpBridgeHealth = "bridge.health"
	OpD1Query      = "d1.query"
	OpD1Batch      = "d1.batch"
	OpVectorUpsert = "vector.upsert"
	OpVectorDelete = "vector.delete"
	OpVectorQuery  = "vector.query"
	OpVectorGet    = "vector.get"
	OpVectorHealth = "vector.health"
	OpAdminJob     = "admin.job"
)

// Sentinel errors for transport-level and protocol-level bridge failures.
// Operation-level failures are reported as *BridgeError values.
var (
	// ErrInvalidConfig is returned when the client is constructed without a
	// usable virtual-host endpoint or bridge token.
	ErrInvalidConfig = errors.New("cloudflarebridge: invalid client config")
	// ErrUnauthorized marks a rejected bridge token.
	ErrUnauthorized = errors.New("cloudflarebridge: unauthorized")
	// ErrForbidden marks an authorized request the Worker refused to route.
	ErrForbidden = errors.New("cloudflarebridge: forbidden")
	// ErrNotFound marks an unknown bridge endpoint route.
	ErrNotFound = errors.New("cloudflarebridge: bridge route not found")
	// ErrRateLimited marks a retryable rate-limit response.
	ErrRateLimited = errors.New("cloudflarebridge: rate limited")
	// ErrTimeout marks a deadline that elapsed before the Worker answered.
	ErrTimeout = errors.New("cloudflarebridge: request timed out")
	// ErrUnavailable marks a retryable Worker-side failure.
	ErrUnavailable = errors.New("cloudflarebridge: bridge unavailable")
	// ErrBadEnvelope marks a response that does not satisfy the envelope
	// contract, including request-id correlation failures.
	ErrBadEnvelope = errors.New("cloudflarebridge: malformed bridge response")
	// ErrVersionMismatch marks an envelope version the client cannot handle.
	ErrVersionMismatch = errors.New("cloudflarebridge: envelope version mismatch")
)

// Retryable reports whether err is advisory-retryable per the bridge contract:
// rate limits and Worker-side unavailability may be retried with backoff, while
// protocol, authorization, and operation-level failures must not be blindly
// replayed.
func Retryable(err error) bool {
	var bridgeErr *BridgeError
	if errors.As(err, &bridgeErr) {
		return bridgeErr.Retryable
	}
	return errors.Is(err, ErrRateLimited) || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrTimeout)
}

// BridgeError is an operation-level error reported by the Worker envelope.
// It must be surfaced to callers as a typed failure: callers must not blind
// retry non-retryable codes and must not silently coerce them to success.
type BridgeError struct {
	// Code is the stable Worker-side error code, for example "not_implemented".
	Code string
	// Message is a human-readable description safe for logs.
	Message string
	// Retryable is the Worker's advisory on whether a bounded retry is safe.
	Retryable bool
}

func (e *BridgeError) Error() string {
	return fmt.Sprintf("cloudflarebridge: %s: %s", e.Code, e.Message)
}

// Request is the versioned bridge request envelope.
type Request struct {
	Version   int             `json:"version"`
	ID        string          `json:"id"`
	Operation string          `json:"operation"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// Response is the versioned bridge response envelope.
type Response struct {
	Version      int             `json:"version"`
	ID           string          `json:"id"`
	OK           bool            `json:"ok"`
	Result       json.RawMessage `json:"result,omitempty"`
	ErrorCode    string          `json:"error_code,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	Retryable    bool            `json:"retryable,omitempty"`
}

// Client is the virtual-host Worker bridge client. It is safe for concurrent
// use.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewClient validates the virtual-host endpoint and bridge token and returns a
// client bound to them. The endpoint must be an http(s) URL; it is a Worker
// virtual hostname, never a Cloudflare API REST endpoint.
func NewClient(baseURL, token string, timeout time.Duration) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" || strings.TrimSpace(token) == "" {
		return nil, ErrInvalidConfig
	}
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Host == "" {
		return nil, ErrInvalidConfig
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, ErrInvalidConfig
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Client{
		baseURL:    strings.TrimRight(parsed.String(), "/"),
		token:      token,
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

// Do dispatches one versioned bridge operation. payload may be nil; result, if
// non-nil, receives the decoded operation result on success. The returned
// error is either a sentinel protocol/transport error or a *BridgeError
// carrying the Worker-reported operation failure.
func (c *Client) Do(ctx context.Context, operation string, payload, result any) error {
	if c == nil {
		return ErrInvalidConfig
	}
	if strings.TrimSpace(operation) == "" {
		return fmt.Errorf("%w: empty operation", ErrBadEnvelope)
	}
	requestID, err := newRequestID()
	if err != nil {
		return fmt.Errorf("cloudflarebridge: request id: %w", err)
	}
	envelope := Request{
		Version:   EnvelopeVersion,
		ID:        requestID,
		Operation: operation,
	}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("cloudflarebridge: encode payload: %w", err)
		}
		envelope.Payload = raw
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("cloudflarebridge: encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Accept", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())
		}
		return fmt.Errorf("cloudflarebridge: transport: %w", err)
	}
	defer httpResp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("%w: read body: %v", ErrBadEnvelope, err)
	}

	switch {
	case httpResp.StatusCode == http.StatusUnauthorized:
		return fmt.Errorf("%w: status 401", ErrUnauthorized)
	case httpResp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("%w: status 403", ErrForbidden)
	case httpResp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: status 404", ErrNotFound)
	case httpResp.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("%w: status 429", ErrRateLimited)
	case httpResp.StatusCode >= 500:
		return fmt.Errorf("%w: status %d", ErrUnavailable, httpResp.StatusCode)
	case httpResp.StatusCode == http.StatusBadRequest:
		// The Worker reports protocol violations in the envelope even with a
		// 400 status; fall through so the operation-level error is preserved.
	case httpResp.StatusCode < 200 || httpResp.StatusCode >= 300:
		return fmt.Errorf("%w: status %d", ErrUnavailable, httpResp.StatusCode)
	}

	var resp Response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("%w: decode: %v (status %d)", ErrBadEnvelope, err, httpResp.StatusCode)
	}
	if resp.Version != EnvelopeVersion {
		return fmt.Errorf("%w: worker version %d, client version %d", ErrVersionMismatch, resp.Version, EnvelopeVersion)
	}
	if resp.ID != requestID {
		return fmt.Errorf("%w: response id %q does not match request id %q", ErrBadEnvelope, resp.ID, requestID)
	}
	if !resp.OK {
		return &BridgeError{
			Code:      resp.ErrorCode,
			Message:   resp.ErrorMessage,
			Retryable: resp.Retryable,
		}
	}
	if result != nil && len(resp.Result) > 0 {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return fmt.Errorf("%w: decode result: %v", ErrBadEnvelope, err)
		}
	}
	return nil
}

// Ping verifies bridge reachability and contract agreement. It is a cheap
// diagnostic and must not be used as a data-feature readiness proof.
func (c *Client) Ping(ctx context.Context) error {
	var result json.RawMessage
	return c.Do(ctx, OpBridgePing, nil, &result)
}

// BridgeHealth describes the Worker-side binding state.
type BridgeHealth struct {
	D1        string `json:"d1"`
	Vectorize string `json:"vectorize"`
	Version   int    `json:"version"`
}

// Health fetches the Worker-side D1/Vectorize binding presence report. Like
// Ping it is a bootstrap diagnostic, not a parity gate.
func (c *Client) Health(ctx context.Context) (BridgeHealth, error) {
	var health BridgeHealth
	err := c.Do(ctx, OpBridgeHealth, nil, &health)
	return health, err
}

func newRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
