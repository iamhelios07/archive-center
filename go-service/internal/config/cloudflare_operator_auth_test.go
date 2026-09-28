package config

import (
	"strings"
	"testing"
)

// The Cloudflare profile is the one deployment shape that is reachable from the
// internet, and it was the only shape that did not require operator
// authentication.
//
// authMiddleware is a pass-through unless Auth.Enforce is set. That default is
// correct for a service on loopback and wrong for a Cloudflare Container, which
// the platform gives a public address precisely so it can serve. The consequence
// was that POST /admin/database-reset answered anyone who asked, and the route's
// own confirmation token did not help: it is a constant compiled into the binary
// and readable in the source, so it stops a mistyped reset and authorises nobody.
//
// These tests pin the requirement, not the wiring.

func validCloudflareConfig() Config {
	return Config{
		Mode:                  ModeShadow,
		RuntimeProfile:        RuntimeProfileCloudflare,
		StoreMode:             StoreModeCloudflareAuthority,
		VectorMode:            VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "bridge-token",
		ChromaEnabled:         false,
		ChromaEndpoint:        "",
	}
}

// TestCloudflareProfileRefusesToStartWithoutOperatorAuth is the security
// property. Refusing to start is deliberate: an operator who cannot set a token
// cannot run this profile, and finding that out from a config error is much
// better than finding it out from an audit log.
func TestCloudflareProfileRefusesToStartWithoutOperatorAuth(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*Config)
		wantAll []string
	}{
		"no auth at all": {
			mutate:  func(c *Config) {},
			wantAll: []string{"operator authentication", "AC_ENFORCE_AUTH"},
		},
		"enforced but no token": {
			// Enforcing an empty token is worse than not enforcing: it rejects
			// every request, including the operator's own, and looks like an outage.
			mutate:  func(c *Config) { c.Auth.Enforce = true },
			wantAll: []string{"AC_BEARER_TOKEN", "empty"},
		},
		"token but not enforced": {
			// A token that is never checked is the most misleading state of the
			// three, because an operator reading the configuration believes the
			// route is protected.
			mutate:  func(c *Config) { c.Auth.BearerToken = "operator-token" },
			wantAll: []string{"operator authentication", "AC_ENFORCE_AUTH"},
		},
		"whitespace token": {
			mutate:  func(c *Config) { c.Auth.Enforce = true; c.Auth.BearerToken = "   " },
			wantAll: []string{"AC_BEARER_TOKEN"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := validCloudflareConfig()
			tc.mutate(&cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() accepted a Cloudflare profile with %s; that deployment is reachable from the internet", name)
			}
			for _, want := range tc.wantAll {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q, so the operator is not told what to set", err.Error(), want)
				}
			}
		})
	}
}

// TestCloudflareProfileStartsWithOperatorAuth is the other half: the requirement
// must not make the profile unusable, or the fix would just be a way to refuse
// the deployment.
func TestCloudflareProfileStartsWithOperatorAuth(t *testing.T) {
	cfg := validCloudflareConfig()
	cfg.Auth.Enforce = true
	cfg.Auth.BearerToken = "operator-token"
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() rejected a properly authenticated Cloudflare profile: %v", err)
	}
}

// TestOtherProfilesKeepTheDefaultNoAuth is the direction that must not break. A
// local installation on loopback must still start without configuring a token;
// requiring one everywhere would be a behaviour change to a working deployment.
//
// Default() is used unmodified because that is a configuration known to
// validate: this test is about the authentication rule, and a fixture that fails
// for an unrelated reason would hide that.
func TestOtherProfilesKeepTheDefaultNoAuth(t *testing.T) {
	cfg := Default()
	if cfg.RuntimeProfile == RuntimeProfileCloudflare {
		t.Fatalf("Default() is now the Cloudflare profile; pick a local fixture for this test")
	}
	if cfg.Auth.Enforce {
		t.Fatalf("Default() now enforces auth, so the 'no auth by default' premise of this test is wrong")
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("a local profile now requires operator authentication: %v", err)
	}
}
