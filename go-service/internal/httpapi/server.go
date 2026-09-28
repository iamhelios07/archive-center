// Package httpapi provides HTTP handlers for the Archive Center Go shadow service.
// Route groups are split by domain (health, turn, memory, proxy, admin, narrative)
// to keep files small and focused.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/diagnostics"
	"github.com/risulongmemory/archive-center-go/internal/store"
	"github.com/risulongmemory/archive-center-go/internal/vector"
)

const (
	// UpdateApplyExitCode is the process exit code emitted after POST
	// /update/apply has staged a verified package and requested a graceful
	// shutdown. Managed launchers use this documented code to distinguish an
	// update handoff from an ordinary service stop.
	UpdateApplyExitCode = 75
	// UpdateApplyManagedLauncherMode is the explicit launcher capability token
	// required before the backend exposes the update shutdown callback.
	UpdateApplyManagedLauncherMode = "managed_launcher_exit_75"
)

// Server holds the HTTP handler dependencies.
type Server struct {
	Cfg                      config.Config
	Started                  time.Time
	BackendInstanceID        string
	Store                    store.Store
	StoreOpenError           error
	Vector                   vector.VectorStore
	VectorOpenError          error
	ReferenceVector          vector.VectorStore
	ReferenceVectorOpenError error
	RuntimeConfig            RuntimeConfig
	RuntimeConfigMu          sync.RWMutex
	memoryWorkerWake         chan struct{}
	memoryWorkerWakeOnce     sync.Once
	memoryWorkerStartOnce    sync.Once
	AdminJobs                *adminJobManager
	CompleteTurns            *completeTurnRequestLedger
	TurnWorkflows            *turnWorkflowHUDLedger
	SourceAcceptances        *completeTurnSourceAcceptanceLedger
	RollbackDecisions        *rollbackDecisionLedger
	RequestShutdown          func(exitCode int)
	DiagnosticWriter         *diagnostics.Writer
	indexRecoveryState       atomic.Int32 // 0: ready, 1: settings needed, 2: rebuilding, 3: failed
	indexRecoveryContext     context.Context
}

// vectorPreflightError names the accelerator this deployment actually uses.
//
// It used to be three separate literals that all said "chromadb startup
// preflight failed (api_path=...)", one per failure path. A Cloudflare
// deployment therefore reported a dead Vectorize bridge as a ChromaDB failure
// and pointed the operator at a Chroma API path that exists in no part of that
// deployment. The refusal to start was right; the message sent someone to the
// wrong place to fix it, and it cost them every minute between the container
// exiting and their believing the log.
//
// This is found by running the built image, not by a unit test: the image was
// configured correctly and exited with the wrong name, because the path that
// fires first in a container is the Health one, and only the VectorOpenError
// path had been corrected.
//
// One function, used by every path, so the next failure cannot reintroduce a
// fourth name.
//
// The cause is wrapped with %w rather than flattened to a string. A caller — and
// a test — decides what to do with this error by matching on it, and callers
// check for context.Canceled when a startup is aborted. An earlier draft took
// err.Error() and formatted it with %s, which preserved the text and destroyed
// the chain, so errors.Is(err, context.Canceled) went false.
func (s *Server) vectorPreflightError(cause error) error {
	if s.Cfg.IsCloudflareProfile() {
		return fmt.Errorf("cloudflare vectorize startup preflight failed (bridge=%s): %w",
			redactedBridgeHost(s.Cfg.CloudflareBridgeURL), cause)
	}
	return fmt.Errorf("chromadb startup preflight failed (api_path=%s): %w", s.Cfg.ChromaAPIPath, cause)
}

// redactedBridgeHost reduces a bridge URL to scheme, host and port.
//
// The preflight message goes to logs and to whoever is reading them, and a URL is
// exactly the shape people paste a token into. Dropping userinfo, path and query
// keeps the one part that identifies which bridge failed — and if the URL does not
// parse, returning the raw string is worse than returning nothing useful, so the
// value is replaced rather than echoed.
func redactedBridgeHost(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		if trimmed := strings.TrimSpace(raw); trimmed == "" {
			return "unset"
		}
		return "unparseable"
	}
	return parsed.Scheme + "://" + parsed.Host
}

// ValidateRuntimeDependencies verifies live dependencies before the HTTP
// service advertises readiness. Chroma Health checks both the configured API
// heartbeat and collection endpoint without deleting existing data.
func (s *Server) ValidateRuntimeDependencies(ctx context.Context) error {
	// A canonical authority whose store could not open must fail startup rather
	// than serve requests against a no-op store. The Cloudflare D1 provider is an
	// authority too, so it is covered by the same gate.
	if s.StoreOpenError != nil {
		switch s.Cfg.StoreMode {
		case config.StoreModeMariaDBAuthority:
			return fmt.Errorf("mariadb startup preflight failed: %w", s.StoreOpenError)
		case config.StoreModeCloudflareAuthority:
			return fmt.Errorf("cloudflare d1 startup preflight failed: %w", s.StoreOpenError)
		}
	}
	if !s.Cfg.VectorAcceleratorEnabled() && s.Cfg.VectorAcceleratorConfigured() {
		return nil
	}
	if s.VectorOpenError != nil {
		return s.vectorPreflightError(s.VectorOpenError)
	}
	if recovery, ok := s.Vector.(vector.IndexRecovery); ok && s.Cfg.StoreMode == config.StoreModeMariaDBAuthority {
		if err := recovery.ResumeIndexRecovery(ctx, s.indexRecoveryJournalPath()); err != nil {
			return fmt.Errorf("resume chromadb index recovery: %w", err)
		}
	}
	health, err := s.Vector.Health(ctx)
	if vector.IsIndexLoadError(err) && s.Cfg.StoreMode == config.StoreModeMariaDBAuthority {
		if _, ok := s.Vector.(vector.IndexRecovery); ok {
			s.indexRecoveryContext = ctx
			s.indexRecoveryState.Store(1)
			// Management must be reachable while rebuilding or waiting for Host
			// embedding settings. The existing recovery owner starts workers only
			// after it finishes; /ready continues to report vector readiness.
			s.retryIndexRecoveryAfterConfigSync()
			return nil
		}
	}
	if err != nil {
		return s.vectorPreflightError(err)
	}
	if strings.TrimSpace(health.Status) != "ok" || !health.ModelReady {
		return s.vectorPreflightError(fmt.Errorf("status=%s model_ready=%t", health.Status, health.ModelReady))
	}
	// Original-work reference retrieval is an optional capability. Its separate
	// collection health is reported by /ready and must not block the main store,
	// session-memory vector lane, or turn runtime from starting.
	return nil
}

// NewServer creates a Server with the given configuration.
func NewServer(cfg config.Config) *Server {
	started := time.Now().UTC()
	st, storeErr := newStoreForConfig(cfg)
	var vs vector.VectorStore
	var vectorErr error
	var referenceVS vector.VectorStore
	var referenceVectorErr error
	switch {
	case cfg.IsCloudflareProfile():
		// The Cloudflare profile reaches Vectorize only through the Worker bridge,
		// and the bridge token is the accelerator's credential — there is no
		// endpoint to hand to a Chroma constructor. An unusable bridge is reported
		// as a loud open error rather than falling back to the fake store, so an
		// unwired deployment cannot appear to be persisting a vector index.
		client, err := cloudflarebridge.NewClient(cfg.CloudflareBridgeURL, cfg.CloudflareBridgeToken, 0)
		if err != nil {
			vectorErr = fmt.Errorf("cloudflare bridge client: %w", err)
			referenceVectorErr = vectorErr
		} else if provider, err := vector.NewVectorizeStore(client); err != nil {
			vectorErr = err
			referenceVectorErr = err
		} else {
			// One Vectorize index holds both document families, so the two stores
			// must stay separable. The discriminator is the document tier, not a
			// second index: session documents are addressed by chat_session_id and
			// reference documents by a "reference_" tier prefix, and neither
			// audience queries the other key. A reference document that acquired a
			// chat_session_id would leak into a session recall, so the separation
			// has to be asserted rather than assumed.
			vs = provider
			referenceVS = provider
		}
	case cfg.ChromaEnabled && strings.TrimSpace(cfg.ChromaEndpoint) != "":
		vs, vectorErr = vector.NewChromaStore(cfg.ChromaEndpoint, cfg.ChromaCollection, cfg.ChromaAPIPath)
		referenceVS, referenceVectorErr = vector.NewChromaStore(cfg.ChromaEndpoint, cfg.ReferenceChromaCollection, cfg.ChromaAPIPath)
	default:
		vs = vector.NewFakeVectorStore()
		referenceVS = vector.NewFakeVectorStore()
	}
	if vectorErr != nil {
		vs = vector.NewFakeVectorStore()
	}
	if referenceVectorErr != nil {
		referenceVS = vector.NewFakeVectorStore()
	}
	vs = vector.NewMutationFencedStore(vs)
	referenceVS = vector.NewMutationFencedStore(referenceVS)
	return &Server{
		Cfg:                      cfg,
		Started:                  started,
		BackendInstanceID:        newBackendInstanceID(started),
		Store:                    st,
		StoreOpenError:           storeErr,
		Vector:                   vs,
		VectorOpenError:          vectorErr,
		ReferenceVector:          referenceVS,
		ReferenceVectorOpenError: referenceVectorErr,
		memoryWorkerWake:         make(chan struct{}, 1),
		AdminJobs:                newAdminJobManager(),
		CompleteTurns:            newCompleteTurnRequestLedger(),
		TurnWorkflows:            newTurnWorkflowHUDLedger(),
		SourceAcceptances:        newCompleteTurnSourceAcceptanceLedger(),
		RollbackDecisions:        newRollbackDecisionLedger(),
	}
}

func newBackendInstanceID(started time.Time) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return fmt.Sprintf("started-%d", started.UnixNano())
}

func (s *Server) backendInstanceID() string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(s.BackendInstanceID)
}

// newStoreForConfig picks the store implementation based on the config store mode.
func newStoreForConfig(cfg config.Config) (store.Store, error) {
	switch cfg.StoreMode {
	case config.StoreModeDualShadow:
		return store.NewDualWriteStore(store.NewNoopStore(), store.NewNoopStore()), nil
	case config.StoreModeMariaDBShadow:
		maria, err := store.OpenMariaDB(cfg.MariaDBDSN)
		if err != nil {
			return store.NewNoopStore(), err
		}
		return store.NewDualWriteStore(store.NewNoopStore(), maria), nil
	case config.StoreModeMariaDBReadShadow:
		maria, err := store.OpenMariaDB(cfg.MariaDBDSN)
		if err != nil {
			return store.NewNoopStore(), err
		}
		return store.NewReadOnlyStore(maria), nil
	case config.StoreModeMariaDBAuthority:
		maria, err := store.OpenMariaDB(cfg.MariaDBDSN)
		if err != nil {
			return store.NewNoopStore(), err
		}
		return maria, nil
	case config.StoreModeFixtureShadow:
		fixture, err := store.NewFixtureStoreFromExportDir(cfg.StoreFixtureDir)
		if err != nil {
			return store.NewNoopStore(), err
		}
		return fixture, nil
	case config.StoreModeCloudflareAuthority:
		// The Cloudflare profile reaches D1 only through the Worker bridge. An
		// unusable bridge configuration is reported as a loud open error rather
		// than falling back to a no-op store, so an unwired deployment cannot
		// appear to persist canonical writes.
		client, err := cloudflarebridge.NewClient(cfg.CloudflareBridgeURL, cfg.CloudflareBridgeToken, 0)
		if err != nil {
			return store.NewNoopStore(), fmt.Errorf("cloudflare bridge client: %w", err)
		}
		conn, err := store.NewD1BridgeConn(client)
		if err != nil {
			return store.NewNoopStore(), err
		}
		d1, err := store.NewD1Store(conn)
		if err != nil {
			return store.NewNoopStore(), err
		}
		return d1, nil
	default:
		return store.NewNoopStore(), nil
	}
}

// hasCanonicalWriteCapability reports whether the selected provider can own
// canonical writes.
//
// It replaces a MariaDB-only mode list: the Cloudflare D1 provider is a
// first-class canonical authority, so keying the write and reset routes off
// MariaDB mode names would silently lock Cloudflare out of them.
func (s *Server) hasCanonicalWriteCapability() bool {
	if errors.Is(s.StoreOpenError, store.ErrNotEnabled) {
		return false
	}
	switch s.Cfg.StoreMode {
	case config.StoreModeDualShadow,
		config.StoreModeMariaDBShadow,
		config.StoreModeMariaDBAuthority,
		config.StoreModeCloudflareAuthority:
		return true
	default:
		return false
	}
}

// usesShadowWriteStore keeps the original call-site name while delegating to the
// provider-neutral capability check.
func (s *Server) usesShadowWriteStore() bool {
	return s.hasCanonicalWriteCapability()
}

func (s *Server) storeWriteSource() string {
	switch s.Cfg.StoreMode {
	case config.StoreModeMariaDBAuthority:
		return "mariadb_authority"
	case config.StoreModeMariaDBShadow:
		return "mariadb_shadow"
	case config.StoreModeDualShadow:
		return "dual_shadow"
	case config.StoreModeCloudflareAuthority:
		return "cloudflare_d1"
	default:
		return "shadow"
	}
}

// RegisterRoutes mounts all handler routes on the provided mux.
// Tier mapping (mirrors contracts/go-route-group-design.md):
//
//	R0: health, config static, chroma-shadow probes
//	R1: search, retrieval-index read, explorer read, narrative read, metrics read, audit
//	R2: turn write, memory write, proxy config write, admin ops, narrative write, import
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	sub := http.NewServeMux()
	s.registerHealthRoutes(sub)    // R0 + R1 read
	s.registerConfigRoutes(sub)    // R1 read + R2 write
	s.registerTurnRoutes(sub)      // R2 write
	s.registerMemoryRoutes(sub)    // R1 read + R2 write
	s.registerCanonicalRoutes(sub) // R1 store-backed canonical shadow
	// Deprecated vector shadow/proof routes are intentionally not mounted in
	// the active 2.0 runtime. ChromaDB is the selected vector accelerator.
	s.registerProxyRoutes(sub)        // R2 write
	s.registerAdminRoutes(sub)        // R2 write
	s.registerTimelineRoutes(sub)     // R1 read
	s.registerDashboardRoutes(sub)    // R1 presentation model
	s.registerPresentationRoutes(sub) // R1 timeline/explorer presentation model
	s.registerSessionMigrationRoutes(sub)
	s.registerNarrativeRoutes(sub) // R1 read + R2 write
	s.registerPersonaRoutes(sub)   // R1 read + R2 write
	s.registerCriticLedgerRoutes(sub)
	s.registerStep22ValidationRoutes(sub)
	s.registerStep23ConsequenceRoutes(sub)
	s.registerStep23PsychologyRoutes(sub)
	s.registerStep23ForkLineageRoutes(sub)
	s.registerStep23ThemeOffscreenRoutes(sub)
	s.registerStep23CaptureVerificationRoutes(sub)
	s.registerStatusSchemaRoutes(sub)
	s.registerReferenceLibraryRoutes(sub)
	s.registerLorebookReferenceRoutes(sub)
	s.registerCanonPackPreviewRoutes(sub)
	s.registerSourceDiscoveryRoutes(sub)
	s.registerUpdateRoutes(sub)
	sub.HandleFunc("GET /diagnostics/report", s.handleDiagnosticReport)
	sub.HandleFunc("POST /diagnostics/logging", s.handleDiagnosticLogging)
	mux.Handle("/", s.diagnosticMiddleware(s.corsMiddleware(s.authMiddleware(s.reverseProxyBasePathMiddleware(sub)))))
}

func (s *Server) reverseProxyBasePathMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		strippedPath, ok := stripUnknownReverseProxyBasePath(r.URL.Path)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		clone := r.Clone(r.Context())
		urlCopy := *clone.URL
		urlCopy.Path = strippedPath
		urlCopy.RawPath = ""
		clone.URL = &urlCopy
		next.ServeHTTP(w, clone)
	})
}

func stripUnknownReverseProxyBasePath(requestPath string) (string, bool) {
	path := strings.TrimSpace(requestPath)
	if path == "" || path == "/" || !strings.HasPrefix(path, "/") {
		return requestPath, false
	}
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 3)
	if len(parts) < 2 {
		return requestPath, false
	}
	first := strings.TrimSpace(parts[0])
	second := strings.TrimSpace(parts[1])
	if first == "" || second == "" || isArchiveRouteRoot(first) || !isArchiveRouteRoot(second) {
		return requestPath, false
	}
	if len(parts) == 2 {
		return "/" + second, true
	}
	return "/" + second + "/" + parts[2], true
}

func isArchiveRouteRoot(segment string) bool {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "active-states",
		"admin",
		"arc",
		"arcs",
		"canonical",
		"characters",
		"chapter",
		"chapters",
		"chroma-shadow",
		"complete-turn",
		"config",
		"continuity-pack",
		"diagnostics",
		"effective-inputs",
		"episodes",
		"explorer",
		"feedback",
		"health",
		"import",
		"intent-routing",
		"kg",
		"long-session-health",
		"maintenance",
		"maintenance-pass",
		"metrics",
		"momentum-packet",
		"narrative-control",
		"pending-threads",
		"persona-capsules",
		"prepare-turn",
		"prompts",
		"proxy",
		"ready",
		"retrieval-index",
		"rollback",
		"saga",
		"sagas",
		"search",
		"session",
		"session-state",
		"sessions",
		"stats",
		"status-schema",
		"step22",
		"step23",
		"storylines",
		"subjective-entity-memories",
		"supervisor",
		"timeline",
		"turns",
		"turn-workflow",
		"update",
		"validation",
		"version",
		"wakeup",
		"world-rules":
		return true
	default:
		return false
	}
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		allowed := s.resolveAllowedOrigin(origin)
		if allowed != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowed)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) resolveAllowedOrigin(origin string) string {
	origins := s.Cfg.AllowedOrigins
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	for _, item := range origins {
		item = strings.TrimSpace(item)
		switch {
		case item == "*":
			return "*"
		case origin != "" && strings.EqualFold(item, origin):
			return origin
		}
	}
	return ""
}
