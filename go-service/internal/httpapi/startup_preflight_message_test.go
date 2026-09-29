package httpapi

import (
	"context"
	"strings"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
)

// The preflight message is how an operator finds out which dependency to go and
// fix. Getting that name wrong is not a wording problem: it sends someone to a
// server that does not exist in their deployment and costs them the time between
// the container refusing to start and them believing the log.
//
// This was found by running the built image, not by a unit test. The Cloudflare
// container was configured correctly and refused to start with
//
//	"chromadb startup preflight failed (api_path=/api/v2)": ... connection refused
//
// naming a ChromaDB server and a Chroma API path that exist in no part of a
// Cloudflare deployment. The refusal was right; the message was wrong.

func cloudflarePreflightConfig() config.Config {
	return config.Config{
		Mode:                  config.ModeShadow,
		RuntimeProfile:        config.RuntimeProfileCloudflare,
		StoreMode:             config.StoreModeCloudflareAuthority,
		VectorMode:            config.VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "super-secret-bridge-token",
		ChromaEnabled:         false,
		ChromaEndpoint:        "",
		ChromaAPIPath:         "/api/v2",
		ChromaCollection:      "",
	}
}

// TestPreflightNamesVectorizeWhenTheBridgeIsUnreachable is the regression, and it
// covers BOTH failure paths rather than the convenient one.
//
// There were three separate literals naming ChromaDB: the VectorOpenError path,
// the Health error path, and the unhealthy-Health path. The container reaches the
// Health one first, so fixing only VectorOpenError — which is exactly what a test
// aimed at that field alone invites — left the deployed image still reporting
// "chromadb". All three now go through one helper, and the reachable ones are
// both asserted here.
func TestPreflightNamesVectorizeWhenTheBridgeIsUnreachable(t *testing.T) {
	cases := map[string]*Server{
		"open error": {
			Cfg:             cloudflarePreflightConfig(),
			Vector:          &journalingVectorStore{},
			VectorOpenError: context.DeadlineExceeded,
		},
		// No VectorOpenError, and Health fails instead: this is the path a running
		// container takes when the bridge refuses a connection, and it is the one
		// that actually fired in the image.
		"health error": {
			Cfg:    cloudflarePreflightConfig(),
			Vector: &unhealthyVectorStore{},
		},
		"health not ready": {
			Cfg:    cloudflarePreflightConfig(),
			Vector: &unreadyVectorStore{},
		},
	}

	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			err := s.ValidateRuntimeDependencies(context.Background())
			if err == nil {
				t.Fatal("ValidateRuntimeDependencies returned nil with an unreachable accelerator; a Cloudflare deployment is built around Vectorize and must not serve against it")
			}
			message := err.Error()

			if strings.Contains(strings.ToLower(message), "chromadb") {
				t.Errorf("preflight failure names ChromaDB on the Cloudflare profile: %q; there is no ChromaDB in this deployment to go and inspect", message)
			}
			if strings.Contains(message, "/api/v2") {
				t.Errorf("preflight failure carries the Chroma API path on the Cloudflare profile: %q", message)
			}
			if !strings.Contains(message, "vectorize") {
				t.Errorf("preflight failure does not name the accelerator this deployment uses: %q", message)
			}
			if !strings.Contains(message, "archive-center-bridge.internal") {
				t.Errorf("preflight failure does not say which bridge failed, so an operator with several cannot tell them apart: %q", message)
			}
		})
	}
}

// TestPreflightNeverEchoesTheBridgeToken is the other half. The message goes to
// logs, and a URL is exactly the shape people paste a token into, so the
// identifier is reduced to scheme, host and port.
func TestPreflightNeverEchoesTheBridgeToken(t *testing.T) {
	const token = "super-secret-bridge-token"
	cases := map[string]string{
		"token in userinfo": "http://user:" + token + "@archive-center-bridge.internal",
		"token in query":    "http://archive-center-bridge.internal/bridge?token=" + token,
		"token in path":     "http://archive-center-bridge.internal/" + token,
	}
	for name, rawURL := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := cloudflarePreflightConfig()
			cfg.CloudflareBridgeURL = rawURL
			cfg.CloudflareBridgeToken = token

			s := &Server{Cfg: cfg, Vector: &journalingVectorStore{}, VectorOpenError: context.DeadlineExceeded}
			err := s.ValidateRuntimeDependencies(context.Background())
			if err == nil {
				t.Fatal("ValidateRuntimeDependencies returned nil; the accelerator was unreachable")
			}
			if strings.Contains(err.Error(), token) {
				t.Errorf("preflight failure echoed the bridge token: %q", err.Error())
			}
			if !strings.Contains(err.Error(), "archive-center-bridge.internal") {
				t.Errorf("preflight failure lost the host entirely, so the operator is not told which bridge failed: %q", err.Error())
			}
		})
	}
}

// TestPreflightStillNamesChromaOnALocalDeployment is the direction that must not
// break. A local deployment's error must keep pointing at ChromaDB, because
// making the message generic for everyone would be the same mistake in the other
// direction.
func TestPreflightStillNamesChromaOnALocalDeployment(t *testing.T) {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileFullLocal
	cfg.StoreMode = config.StoreModeMariaDBAuthority
	cfg.VectorMode = config.VectorModeExternal
	cfg.ChromaEnabled = true
	cfg.ChromaEndpoint = "http://127.0.0.1:8000"
	cfg.ChromaAPIPath = "/api/v2"

	s := &Server{Cfg: cfg, Vector: &journalingVectorStore{}, VectorOpenError: context.DeadlineExceeded}
	err := s.ValidateRuntimeDependencies(context.Background())
	if err == nil {
		t.Fatal("ValidateRuntimeDependencies returned nil with an unreachable ChromaDB")
	}
	if !strings.Contains(err.Error(), "chromadb") || !strings.Contains(err.Error(), "/api/v2") {
		t.Errorf("local preflight failure stopped naming ChromaDB and its API path: %q", err.Error())
	}
}

func TestRedactedBridgeHost(t *testing.T) {
	cases := map[string]string{
		"http://archive-center-bridge.internal":            "http://archive-center-bridge.internal",
		"http://u:p@host.internal:8787/deep/path?q=secret": "http://host.internal:8787",
		"https://bridge.example.invalid/":                  "https://bridge.example.invalid",
		"":                                                 "unset",
		"   ":                                              "unset",
		// The space is in the HOST portion, so the URL genuinely does not parse.
		// Returning "unparseable" rather than the input is deliberate: an
		// unparseable value is exactly the one most likely to be a hand-pasted
		// URL with a token somewhere in it, and this string ends up in logs.
		"http://host with spaces?token=supersecret": "unparseable",
	}
	for input, want := range cases {
		if got := redactedBridgeHost(input); got != want {
			t.Errorf("redactedBridgeHost(%q) = %q, want %q", input, got, want)
		}
	}
}
