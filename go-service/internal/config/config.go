// Package config provides application configuration for the Archive Center Go shadow service.
// All values have safe defaults suitable for local development and shadow mode.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Mode describes the runtime role of this service instance.
type Mode string

const (
	ModeShadow  Mode = "shadow"
	ModeLive    Mode = "live"
	ModeCutover Mode = "cutover"
)

// StoreMode selects the concrete store implementation.
type StoreMode string

const (
	StoreModeNoop              StoreMode = "noop"
	StoreModeDualShadow        StoreMode = "dual_shadow"
	StoreModeMariaDBShadow     StoreMode = "mariadb_shadow"
	StoreModeFixtureShadow     StoreMode = "fixture_shadow"
	StoreModeMariaDBReadShadow StoreMode = "mariadb_read_shadow"
	StoreModeMariaDBAuthority  StoreMode = "mariadb_authority"
	// StoreModeCloudflareAuthority is a Stage 2 bootstrap selector. Its D1
	// implementation arrives in Stage 3; it must not be treated as a ready
	// product authority before the parity gates are complete.
	StoreModeCloudflareAuthority StoreMode = "cloudflare_authority"
)

// RuntimeProfile selects the deployment shape for 2.0.1 packages.
type RuntimeProfile string

const (
	RuntimeProfileClientOnly        RuntimeProfile = "client_only"
	RuntimeProfileCoreLite          RuntimeProfile = "core_lite"
	RuntimeProfileVectorExternal    RuntimeProfile = "vector_external"
	RuntimeProfileVectorLocalNative RuntimeProfile = "vector_local_native"
	RuntimeProfileFullLocal         RuntimeProfile = "full_local"
	// RuntimeProfileCloudflare selects the Container -> Worker -> D1/Vectorize
	// deployment shape. It is explicit opt-in and remains unready until parity.
	RuntimeProfileCloudflare RuntimeProfile = "cloudflare"
)

// VectorMode selects whether the vector accelerator is disabled, degraded, or
// backed by a ChromaDB endpoint.
type VectorMode string

const (
	VectorModeOff         VectorMode = "off"
	VectorModeFallback    VectorMode = "fallback"
	VectorModeExternal    VectorMode = "external"
	VectorModeLocalNative VectorMode = "local_native"
	VectorModeLocalProot  VectorMode = "local_proot"
	VectorModeBundled     VectorMode = "bundled"
	// VectorModeCloudflare selects the Worker-mediated Vectorize accelerator.
	VectorModeCloudflare VectorMode = "cloudflare"
)

// Config holds the entire service configuration.
// It contains no secrets; secrets must be injected via the environment
// or a future secret management integration.
type Config struct {
	// HTTP bind address. Default is 127.0.0.1:28080 to avoid conflict with the 0.8 backend.
	BindAddr string

	// AllowedOrigins controls CORS response headers. "*" is the local default.
	AllowedOrigins []string

	// Service mode. Default is shadow. Live and cutover are disabled by default.
	Mode Mode

	// StoreMode selects the store backend. Default is noop.
	// dual_shadow enables the R1 dual-write wrapper over two no-op stores.
	// mariadb_shadow enables the R1 dual-write wrapper with noop primary and
	// MariaDB shadow target. It is not an authority switch.
	StoreMode StoreMode

	// RuntimeProfile selects the 2.0.1 service bundle shape.
	RuntimeProfile RuntimeProfile

	// VectorMode selects how vector recall is provided for this profile.
	VectorMode VectorMode

	// MariaDBDSN is used only when StoreMode is a MariaDB-backed mode.
	MariaDBDSN string

	// CloudflareBridgeURL is the virtual-host Worker bridge endpoint reached by
	// the Container. It is not a Cloudflare API endpoint and contains no account
	// or resource identifier.
	CloudflareBridgeURL string

	// CloudflareBridgeToken is the shared Container-to-Worker bridge secret.
	// It is loaded only from the environment and never included in String().
	CloudflareBridgeToken string

	// MariaDBProductReadEnabled marks an explicit R2 product-read proof where
	// read-only HTTP surfaces are allowed to use MariaDB as their selected
	// relational read source. Default is false and it is accepted only in
	// mariadb_read_shadow mode with a configured DSN.
	MariaDBProductReadEnabled bool

	// StoreFixtureDir is used only by fixture_shadow R1 evidence mode.
	// It points at sqlite-export NDJSON and is not a product authority path.
	StoreFixtureDir string

	// ChromaEnabled records whether this runtime profile should wire the
	// ChromaDB-compatible vector accelerator. MariaDB remains canonical truth.
	ChromaEnabled bool

	// ChromaEndpoint is the ChromaDB HTTP base URL, for example http://127.0.0.1:8000.
	ChromaEndpoint string

	// ChromaCollection is the collection used for Archive Center vectors.
	ChromaCollection string

	// ReferenceChromaCollection is a separate collection for reusable original-
	// work material. It must never share the session-memory collection.
	ReferenceChromaCollection string

	// ChromaAPIPath is the Chroma HTTP API prefix. Default is /api/v2.
	ChromaAPIPath string

	// PromptDir is the editable prompt directory used by the migrated prompt
	// editor. Only known prompt files are exposed by the HTTP API.
	PromptDir string

	// Readiness probe settings.
	Readiness ReadinessConfig

	// Build metadata injected at compile time.
	BuildVersion string
	BuildCommit  string
	BuildTime    string

	// Auth holds the bearer-token envelope. Enforce is off by default.
	Auth AuthConfig
	// ChromaShadowPersistDir is the Chroma shadow persist directory for preflight parity.
	ChromaShadowPersistDir string

	// Embedder config for preflight parity.
	EmbedderProvider string
	EmbedderModel    string
	EmbedderEndpoint string

	// PrunePolicy controls critic prune target handling: "soft" or "off".
	PrunePolicy string

	// CriticLedgerPreviewEnabled exposes the read-only 2.1 critic ledger preview route.
	CriticLedgerPreviewEnabled bool

	// CriticLedgerEnabled allows the ledger to be wired into live Critic calls.
	// It stays false by default; 2.1-7 live wiring requires explicit opt-in.
	CriticLedgerEnabled bool

	// Update settings power the GitHub Releases update check/download flow.
	// The updater only stages verified packages; applying and rollback are
	// intentionally handled by a later helper process.
	UpdateEnabled          bool
	UpdateGitHubRepo       string
	UpdateChannel          string
	UpdateStagingDir       string
	UpdateMaxDownloadBytes int64
}

// ReadinessConfig describes the state of external dependencies for readiness checks.
type ReadinessConfig struct {
	// MariaDBConfigured is true when a MariaDB DSN is present in the environment.
	MariaDBConfigured bool

	// ChromaConfigured is true when a Chroma endpoint is present in the environment.
	ChromaConfigured bool
}

// AuthConfig holds bearer-token authentication settings.
// Enforce is false by default in R0/R1; live enforcement requires explicit approval.
type AuthConfig struct {
	BearerToken string
	Enforce     bool
}

// Default returns a Config populated with safe defaults.
func Default() Config {
	return Config{
		BindAddr:                  "127.0.0.1:28080",
		AllowedOrigins:            []string{"*"},
		Mode:                      ModeShadow,
		StoreMode:                 StoreModeNoop,
		RuntimeProfile:            RuntimeProfileCoreLite,
		VectorMode:                VectorModeFallback,
		MariaDBProductReadEnabled: false,
		ChromaEnabled:             false,
		ChromaEndpoint:            "",
		ChromaCollection:          "archive_center_vectors",
		ReferenceChromaCollection: "archive_center_reference_vectors",
		ChromaAPIPath:             "/api/v2",
		PromptDir:                 "",
		BuildVersion:              "4.8.0",
		BuildCommit:               "unknown",
		BuildTime:                 time.Now().UTC().Format(time.RFC3339),
		Readiness: ReadinessConfig{
			MariaDBConfigured: false,
			ChromaConfigured:  false,
		},
		Auth: AuthConfig{
			BearerToken: "",
			Enforce:     false,
		},
		PrunePolicy:                "soft",
		CriticLedgerPreviewEnabled: true,
		CriticLedgerEnabled:        false,
		UpdateEnabled:              true,
		UpdateGitHubRepo:           "Flazer31/archive-center",
		UpdateChannel:              "stable",
		UpdateStagingDir:           ".updates",
		UpdateMaxDownloadBytes:     1024 * 1024 * 1024,
	}
}

// Load builds a Config from defaults, then overrides with environment variables.
func Load() Config {
	cfg := Default()

	if v := os.Getenv("AC_BIND_ADDR"); v != "" {
		cfg.BindAddr = v
	}
	if v := os.Getenv("AC_ALLOWED_ORIGINS"); v != "" {
		cfg.AllowedOrigins = splitCSV(v)
		if len(cfg.AllowedOrigins) == 0 {
			cfg.AllowedOrigins = []string{"*"}
		}
	}

	if v := os.Getenv("AC_MODE"); v != "" {
		switch strings.ToLower(v) {
		case string(ModeLive):
			cfg.Mode = ModeLive
		case string(ModeCutover):
			cfg.Mode = ModeCutover
		default:
			cfg.Mode = ModeShadow
		}
	}

	if v := os.Getenv("AC_STORE_MODE"); v != "" {
		switch strings.ToLower(v) {
		case string(StoreModeNoop):
			cfg.StoreMode = StoreModeNoop
		case string(StoreModeDualShadow):
			cfg.StoreMode = StoreModeDualShadow
		case string(StoreModeMariaDBShadow):
			cfg.StoreMode = StoreModeMariaDBShadow
		case string(StoreModeFixtureShadow):
			cfg.StoreMode = StoreModeFixtureShadow
		case string(StoreModeMariaDBReadShadow):
			cfg.StoreMode = StoreModeMariaDBReadShadow
		case string(StoreModeMariaDBAuthority):
			cfg.StoreMode = StoreModeMariaDBAuthority
		case string(StoreModeCloudflareAuthority):
			cfg.StoreMode = StoreModeCloudflareAuthority
		default:
			cfg.StoreMode = StoreMode(strings.ToLower(v))
		}
	}

	runtimeProfileExplicit := false
	if v := os.Getenv("AC_RUNTIME_PROFILE"); v != "" {
		runtimeProfileExplicit = true
		cfg.RuntimeProfile = parseRuntimeProfile(v)
	}

	if v := os.Getenv("AC_BUILD_VERSION"); v != "" {
		cfg.BuildVersion = v
	}

	if v := os.Getenv("AC_BUILD_COMMIT"); v != "" {
		cfg.BuildCommit = v
	}

	if v := os.Getenv("AC_BUILD_TIME"); v != "" {
		cfg.BuildTime = v
	}

	cfg.MariaDBDSN = os.Getenv("AC_MARIADB_DSN")
	cfg.CloudflareBridgeURL = strings.TrimSpace(os.Getenv("AC_CLOUDFLARE_BRIDGE_URL"))
	cfg.CloudflareBridgeToken = os.Getenv("AC_CLOUDFLARE_BRIDGE_TOKEN")
	cfg.StoreFixtureDir = os.Getenv("AC_STORE_FIXTURE_DIR")
	cfg.Readiness.MariaDBConfigured = cfg.MariaDBDSN != ""
	cfg.ChromaEndpoint = os.Getenv("AC_CHROMA_ENDPOINT")
	cfg.Readiness.ChromaConfigured = cfg.ChromaEndpoint != ""
	vectorModeExplicit := false
	if v := os.Getenv("AC_VECTOR_MODE"); v != "" {
		vectorModeExplicit = true
		cfg.VectorMode = parseVectorMode(v)
	} else if !runtimeProfileExplicit && strings.TrimSpace(cfg.ChromaEndpoint) != "" {
		cfg.RuntimeProfile = RuntimeProfileFullLocal
		cfg.VectorMode = VectorModeBundled
	} else {
		cfg.VectorMode = defaultVectorMode(cfg.RuntimeProfile)
	}
	cfg.ChromaEnabled = cfg.VectorRequiresEndpoint()
	if vectorModeExplicit && (cfg.VectorMode == VectorModeOff || cfg.VectorMode == VectorModeFallback) {
		cfg.ChromaEnabled = false
	}
	if v := os.Getenv("AC_CHROMA_COLLECTION"); v != "" {
		cfg.ChromaCollection = v
	}
	if v := os.Getenv("AC_REFERENCE_CHROMA_COLLECTION"); v != "" {
		cfg.ReferenceChromaCollection = v
	}
	if v := os.Getenv("AC_CHROMA_API_PATH"); v != "" {
		cfg.ChromaAPIPath = v
	}
	cfg.PromptDir = os.Getenv("AC_PROMPT_DIR")
	cfg.ChromaShadowPersistDir = os.Getenv("AC_CHROMA_SHADOW_PERSIST_DIR")
	cfg.EmbedderProvider = os.Getenv("AC_EMBEDDER_PROVIDER")
	cfg.EmbedderModel = os.Getenv("AC_EMBEDDER_MODEL")
	cfg.EmbedderEndpoint = os.Getenv("AC_EMBEDDER_ENDPOINT")
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("AC_PRUNE_POLICY"))); v != "" {
		switch v {
		case "off", "soft":
			cfg.PrunePolicy = v
		default:
			cfg.PrunePolicy = "soft"
		}
	}
	if os.Getenv("AC_MARIADB_PRODUCT_READ_ENABLED") == "true" {
		cfg.MariaDBProductReadEnabled = true
	}
	if v := strings.TrimSpace(os.Getenv("AC_CRITIC_LEDGER_PREVIEW_ENABLED")); v != "" {
		cfg.CriticLedgerPreviewEnabled = strings.EqualFold(v, "true")
	}
	if os.Getenv("AC_CRITIC_LEDGER_ENABLED") == "true" {
		cfg.CriticLedgerEnabled = true
	}
	if v := strings.TrimSpace(os.Getenv("AC_UPDATE_ENABLED")); v != "" {
		cfg.UpdateEnabled = strings.EqualFold(v, "true")
	}
	if v := strings.TrimSpace(os.Getenv("AC_UPDATE_GITHUB_REPO")); v != "" {
		cfg.UpdateGitHubRepo = v
	}
	if v := strings.TrimSpace(os.Getenv("AC_UPDATE_CHANNEL")); v != "" {
		cfg.UpdateChannel = v
	}
	if v := strings.TrimSpace(os.Getenv("AC_UPDATE_STAGING_DIR")); v != "" {
		cfg.UpdateStagingDir = v
	}
	if v := strings.TrimSpace(os.Getenv("AC_UPDATE_MAX_DOWNLOAD_MB")); v != "" {
		if mb, err := strconv.ParseInt(v, 10, 64); err == nil && mb > 0 {
			cfg.UpdateMaxDownloadBytes = mb * 1024 * 1024
		}
	}

	if v := os.Getenv("AC_BEARER_TOKEN"); v != "" {
		cfg.Auth.BearerToken = v
	}
	if os.Getenv("AC_ENFORCE_AUTH") == "true" {
		cfg.Auth.Enforce = true
	}

	return cfg
}

func parseRuntimeProfile(raw string) RuntimeProfile {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(RuntimeProfileClientOnly):
		return RuntimeProfileClientOnly
	case string(RuntimeProfileVectorExternal):
		return RuntimeProfileVectorExternal
	case string(RuntimeProfileVectorLocalNative):
		return RuntimeProfileVectorLocalNative
	case string(RuntimeProfileFullLocal):
		return RuntimeProfileFullLocal
	case string(RuntimeProfileCoreLite):
		return RuntimeProfileCoreLite
	case string(RuntimeProfileCloudflare):
		return RuntimeProfileCloudflare
	default:
		return RuntimeProfile(strings.ToLower(strings.TrimSpace(raw)))
	}
}

func parseVectorMode(raw string) VectorMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(VectorModeOff):
		return VectorModeOff
	case string(VectorModeExternal):
		return VectorModeExternal
	case string(VectorModeLocalNative):
		return VectorModeLocalNative
	case string(VectorModeLocalProot):
		return VectorModeLocalProot
	case string(VectorModeBundled):
		return VectorModeBundled
	case string(VectorModeFallback):
		return VectorModeFallback
	case string(VectorModeCloudflare):
		return VectorModeCloudflare
	default:
		return VectorMode(strings.ToLower(strings.TrimSpace(raw)))
	}
}

func defaultVectorMode(profile RuntimeProfile) VectorMode {
	switch profile {
	case RuntimeProfileClientOnly:
		return VectorModeOff
	case RuntimeProfileVectorExternal:
		return VectorModeExternal
	case RuntimeProfileVectorLocalNative:
		return VectorModeLocalNative
	case RuntimeProfileFullLocal:
		return VectorModeBundled
	case RuntimeProfileCloudflare:
		return VectorModeCloudflare
	default:
		return VectorModeFallback
	}
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Validate returns an error if the configuration is inconsistent or unsafe.
func (c Config) Validate() error {
	if c.Mode != ModeShadow && c.Mode != ModeLive && c.Mode != ModeCutover {
		return fmt.Errorf("config: mode %q is not allowed", c.Mode)
	}
	if !isAllowedRuntimeProfile(c.RuntimeProfile) {
		return fmt.Errorf("config: runtime_profile %q is not allowed", c.RuntimeProfile)
	}
	if !isAllowedVectorMode(c.VectorMode) {
		return fmt.Errorf("config: vector_mode %q is not allowed", c.VectorMode)
	}
	if err := c.validateProfileVectorPair(); err != nil {
		return err
	}
	if (c.Mode == ModeLive || c.Mode == ModeCutover) && c.StoreMode != StoreModeMariaDBAuthority && c.StoreMode != StoreModeCloudflareAuthority {
		return fmt.Errorf("config: mode %q requires an authoritative store mode (%q or %q)", c.Mode, StoreModeMariaDBAuthority, StoreModeCloudflareAuthority)
	}
	if c.StoreMode != StoreModeNoop && c.StoreMode != StoreModeDualShadow && c.StoreMode != StoreModeMariaDBShadow && c.StoreMode != StoreModeFixtureShadow && c.StoreMode != StoreModeMariaDBReadShadow && c.StoreMode != StoreModeMariaDBAuthority && c.StoreMode != StoreModeCloudflareAuthority {
		return fmt.Errorf("config: store_mode %q is not allowed", c.StoreMode)
	}
	if (c.StoreMode == StoreModeMariaDBShadow || c.StoreMode == StoreModeMariaDBReadShadow || c.StoreMode == StoreModeMariaDBAuthority) && strings.TrimSpace(c.MariaDBDSN) == "" {
		return fmt.Errorf("config: store_mode %q requires AC_MARIADB_DSN", c.StoreMode)
	}
	if c.MariaDBProductReadEnabled && (c.StoreMode != StoreModeMariaDBReadShadow || strings.TrimSpace(c.MariaDBDSN) == "") {
		return fmt.Errorf("config: mariadb product read proof requires AC_STORE_MODE=%q and AC_MARIADB_DSN", StoreModeMariaDBReadShadow)
	}
	if c.StoreMode == StoreModeFixtureShadow && strings.TrimSpace(c.StoreFixtureDir) == "" {
		return fmt.Errorf("config: store_mode %q requires AC_STORE_FIXTURE_DIR", c.StoreMode)
	}
	if c.RuntimeProfile == RuntimeProfileCloudflare {
		if c.StoreMode != StoreModeCloudflareAuthority || c.VectorMode != VectorModeCloudflare {
			return fmt.Errorf("config: runtime_profile %q requires store_mode %q and vector_mode %q", RuntimeProfileCloudflare, StoreModeCloudflareAuthority, VectorModeCloudflare)
		}
		if strings.TrimSpace(c.CloudflareBridgeURL) == "" || strings.TrimSpace(c.CloudflareBridgeToken) == "" {
			return fmt.Errorf("config: runtime_profile %q requires AC_CLOUDFLARE_BRIDGE_URL and AC_CLOUDFLARE_BRIDGE_TOKEN", RuntimeProfileCloudflare)
		}
		// Operator routes are reachable from the internet on this profile and
		// nowhere else. A local installation listens on loopback or a LAN the
		// operator chose; a Cloudflare Container is given a public address by the
		// platform as the whole point of running it there.
		//
		// authMiddleware is a pass-through unless Auth.Enforce is set, and that
		// default is right for a loopback service and wrong here. Without this,
		// POST /admin/database-reset answers anyone who asks. The route's own
		// confirmation token is not a substitute: it is a constant compiled into
		// the binary and readable in the source, so it prevents a mistyped reset
		// and authorises nothing.
		//
		// This is the difference between a deployment that is merely reachable and
		// one that is merely defenceless, so it is a startup failure rather than a
		// warning: an operator who cannot set a token cannot run this profile, and
		// should be told that here rather than discovering it from an audit log.
		if !c.Auth.Enforce {
			return fmt.Errorf("config: runtime_profile %q requires operator authentication; set AC_ENFORCE_AUTH=true and AC_BEARER_TOKEN, because this profile is reachable from the internet", RuntimeProfileCloudflare)
		}
		if strings.TrimSpace(c.Auth.BearerToken) == "" {
			return fmt.Errorf("config: runtime_profile %q requires a non-empty AC_BEARER_TOKEN; an enforced empty token would reject every request including the operator's own", RuntimeProfileCloudflare)
		}
	}
	if c.StoreMode == StoreModeCloudflareAuthority && c.RuntimeProfile != RuntimeProfileCloudflare {
		return fmt.Errorf("config: store_mode %q requires runtime_profile %q", StoreModeCloudflareAuthority, RuntimeProfileCloudflare)
	}
	if c.VectorRequiresEndpoint() && strings.TrimSpace(c.ChromaEndpoint) == "" {
		return fmt.Errorf("config: vector_mode %q requires AC_CHROMA_ENDPOINT", c.VectorMode)
	}
	if strings.TrimSpace(c.ChromaEndpoint) != "" && strings.TrimSpace(c.ChromaCollection) == "" {
		return fmt.Errorf("config: chroma vector store requires AC_CHROMA_COLLECTION")
	}
	if strings.TrimSpace(c.ChromaEndpoint) != "" && strings.TrimSpace(c.ReferenceChromaCollection) == "" {
		return fmt.Errorf("config: reference vector store requires AC_REFERENCE_CHROMA_COLLECTION")
	}
	if strings.TrimSpace(c.ChromaEndpoint) != "" && strings.EqualFold(strings.TrimSpace(c.ChromaCollection), strings.TrimSpace(c.ReferenceChromaCollection)) {
		return fmt.Errorf("config: AC_REFERENCE_CHROMA_COLLECTION must differ from AC_CHROMA_COLLECTION")
	}
	return nil
}

func isAllowedRuntimeProfile(profile RuntimeProfile) bool {
	switch profile {
	case RuntimeProfileClientOnly, RuntimeProfileCoreLite, RuntimeProfileVectorExternal, RuntimeProfileVectorLocalNative, RuntimeProfileFullLocal, RuntimeProfileCloudflare:
		return true
	default:
		return false
	}
}

func isAllowedVectorMode(mode VectorMode) bool {
	switch mode {
	case VectorModeOff, VectorModeFallback, VectorModeExternal, VectorModeLocalNative, VectorModeLocalProot, VectorModeBundled, VectorModeCloudflare:
		return true
	default:
		return false
	}
}

func (c Config) validateProfileVectorPair() error {
	switch c.RuntimeProfile {
	case RuntimeProfileClientOnly:
		if c.VectorMode != VectorModeOff {
			return fmt.Errorf("config: runtime_profile %q requires vector_mode %q", c.RuntimeProfile, VectorModeOff)
		}
	case RuntimeProfileCoreLite:
		if c.VectorMode != VectorModeFallback && c.VectorMode != VectorModeOff {
			return fmt.Errorf("config: runtime_profile %q supports only vector_mode %q or %q", c.RuntimeProfile, VectorModeFallback, VectorModeOff)
		}
	case RuntimeProfileVectorExternal:
		if c.VectorMode != VectorModeExternal {
			return fmt.Errorf("config: runtime_profile %q requires vector_mode %q", c.RuntimeProfile, VectorModeExternal)
		}
	case RuntimeProfileVectorLocalNative:
		if c.VectorMode != VectorModeLocalNative && c.VectorMode != VectorModeBundled {
			return fmt.Errorf("config: runtime_profile %q supports only vector_mode %q or %q", c.RuntimeProfile, VectorModeLocalNative, VectorModeBundled)
		}
	case RuntimeProfileFullLocal:
		if c.VectorMode != VectorModeLocalNative && c.VectorMode != VectorModeLocalProot && c.VectorMode != VectorModeBundled {
			return fmt.Errorf("config: runtime_profile %q requires a local vector mode", c.RuntimeProfile)
		}
	case RuntimeProfileCloudflare:
		if c.VectorMode != VectorModeCloudflare {
			return fmt.Errorf("config: runtime_profile %q requires vector_mode %q", c.RuntimeProfile, VectorModeCloudflare)
		}
	}
	return nil
}

// IsCloudflareProfile reports whether this process is configured for the
// Container-to-Worker Cloudflare topology.
func (c Config) IsCloudflareProfile() bool {
	return c.RuntimeProfile == RuntimeProfileCloudflare && c.StoreMode == StoreModeCloudflareAuthority && c.VectorMode == VectorModeCloudflare
}

// VectorRequiresEndpoint reports whether this profile must have a ChromaDB
// endpoint to satisfy readiness.
func (c Config) VectorRequiresEndpoint() bool {
	switch c.VectorMode {
	case VectorModeExternal, VectorModeLocalNative, VectorModeLocalProot, VectorModeBundled:
		return true
	default:
		return false
	}
}

// VectorPolicySatisfied reports whether the selected vector policy can be
// considered configured. In core_lite fallback/off modes, missing ChromaDB is a
// degraded feature state rather than a service blocker.
func (c Config) VectorPolicySatisfied() bool {
	return !c.VectorRequiresEndpoint() || strings.TrimSpace(c.ChromaEndpoint) != ""
}

// VectorAcceleratorEnabled reports whether this profile is MEANT to run a vector
// accelerator at all, as opposed to whether one happens to be reachable.
//
// The two questions were previously conflated, and they are genuinely different.
// A core_lite fallback deployment sets neither, and an explicit AC_CHROMA_ENDPOINT
// left over in its environment must not turn a deliberately index-less
// deployment into one that claims an index. Readiness and health ask this one;
// the mutation and recall call sites ask the other.
func (c Config) VectorAcceleratorEnabled() bool {
	if c.VectorMode == VectorModeCloudflare {
		// The Cloudflare profile selects Vectorize as a required part of the
		// deployment, not as an optional accelerator, so there is no equivalent of
		// ChromaEnabled falling to false.
		return true
	}
	return c.ChromaEnabled
}

// VectorAcceleratorConfigured reports whether this process has a REACHABLE vector
// accelerator, whichever one the selected profile names.
//
// This exists because a dozen call sites had grown their own private answer to
// the same question, and they did not agree:
//
//	if strings.TrimSpace(s.Cfg.ChromaEndpoint) == "" { /* vector unavailable */ }
//
// That test reads as "is a vector accelerator configured?" but it actually asks
// "is ChromaDB configured?", and on the Cloudflare profile ChromaEndpoint is
// DELIBERATELY empty — the Cloudflare accelerator is Vectorize, reached through
// the Worker bridge, and Load even asserts that ChromaEnabled stays false in
// cloudflare vector mode. Every one of those sites would have reported the
// vector accelerator unavailable on a deployment that has one, silently turning
// semantic recall off: sessions would fall back to lexical fill, a clean upsert
// would skip, and admin reindex would report nothing to do. None of that
// announces itself as a missing provider.
//
// The predicate answers the provider-agnostic question instead, and each call
// site keeps asking about the thing it actually needs — an endpoint, a
// collection name, a probe — rather than about a provider it may not be running.
func (c Config) VectorAcceleratorConfigured() bool {
	if c.VectorMode == VectorModeCloudflare {
		// The bridge URL and token are the accelerator's credentials here. The
		// Cloudflare profile validation already refuses to start without both, so
		// reaching this with an empty value means the process is misconfigured and
		// should degrade honestly rather than assume a working accelerator.
		return strings.TrimSpace(c.CloudflareBridgeURL) != "" && strings.TrimSpace(c.CloudflareBridgeToken) != ""
	}
	return strings.TrimSpace(c.ChromaEndpoint) != ""
}

// VectorAcceleratorName returns the stable, account-neutral label of the
// configured vector accelerator, for health and readiness reporting.
//
// It is a label, not an address: it never contains an endpoint URL, a collection
// name, an index name, or any credential, because those values are rendered into
// operator-facing diagnostics.
func (c Config) VectorAcceleratorName() string {
	if c.VectorMode == VectorModeCloudflare {
		return "vectorize"
	}
	if strings.TrimSpace(c.ChromaEndpoint) != "" {
		return "chromadb"
	}
	return ""
}

// VectorAcceleratorSelected returns the accelerator family this deployment is
// BUILT AROUND, whether or not one is currently reachable.
//
// It is deliberately separate from VectorAcceleratorName, which answers "can this
// process reach its accelerator now". The recall trace names the engine the
// product uses even while it is running a bounded read shadow with nothing
// reachable, because that label is the product's answer to "which index is this
// system for", not a claim that a process is connected to one. Collapsing the two
// would blank that label in read-shadow mode, and — worse — would let a read
// shadow be reported as a live read.
//
// It returns "vectorize" only for the Cloudflare profile and "chromadb"
// otherwise, because the local runtime is MariaDB with ChromaDB and the Cloudflare
// runtime is D1 with Vectorize. A profile that has switched neither is still a
// MariaDB deployment, so naming ChromaDB is the truthful answer, not a default.
func (c Config) VectorAcceleratorSelected() string {
	if c.IsCloudflareProfile() || c.VectorMode == VectorModeCloudflare {
		return "vectorize"
	}
	return "chromadb"
}

// IsLiveCutoverAllowed is the runtime guard for product-mode execution.
// MariaDB and Cloudflare are both canonical authority deployments; each has a
// distinct, explicit connectivity contract.
func (c Config) IsLiveCutoverAllowed() bool {
	if c.Mode != ModeLive && c.Mode != ModeCutover {
		return false
	}
	switch c.StoreMode {
	case StoreModeMariaDBAuthority:
		return strings.TrimSpace(c.MariaDBDSN) != "" && c.VectorPolicySatisfied()
	case StoreModeCloudflareAuthority:
		return c.IsCloudflareProfile() && c.VectorAcceleratorConfigured()
	default:
		return false
	}
}

// String returns a redacted string representation safe for logs.
func (c Config) String() string {
	return fmt.Sprintf("Config{BindAddr=%s Mode=%s StoreMode=%s RuntimeProfile=%s VectorMode=%s MariaDBProductReadEnabled=%t ChromaEnabled=%t Version=%s}", c.BindAddr, c.Mode, c.StoreMode, c.RuntimeProfile, c.VectorMode, c.MariaDBProductReadEnabled, c.ChromaEnabled, c.BuildVersion)
}
