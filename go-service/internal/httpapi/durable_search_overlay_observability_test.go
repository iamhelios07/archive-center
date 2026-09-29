package httpapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/dto"
	"github.com/risulongmemory/archive-center-go/internal/vector"
)

type durableOverlayObservationProvider struct {
	vector.VectorStore
	snapshot vector.DurableSearchOverlaySnapshot
	err      error
	session  string
	maximum  int
}

func (p *durableOverlayObservationProvider) DurableSearchOverlay(_ context.Context, sessionID string, maximum int) (vector.DurableSearchOverlaySnapshot, error) {
	p.session, p.maximum = sessionID, maximum
	return p.snapshot, p.err
}

func TestDurableSearchOverlayObservationClassifiesTTLAndDoesNotMutate(t *testing.T) {
	provider := &durableOverlayObservationProvider{
		VectorStore: vector.NewFakeVectorStore(),
		snapshot: vector.DurableSearchOverlaySnapshot{
			PendingCount:    3,
			OldestPendingAt: time.Now().Add(-vector.DurableSearchOverlaySoftTTL - time.Second),
		},
	}
	s := &Server{Vector: provider}

	got := s.durableSearchOverlayObservation(context.Background(), "session-1")
	if !boolFromAny(got["enabled"]) || got["state"] != "degraded" || got["warning"] != "soft_ttl_exceeded" {
		t.Fatalf("observation = %#v, want enabled soft-TTL degraded state", got)
	}
	if provider.session != "session-1" || provider.maximum != vector.DurableSearchOverlayMaxDocuments {
		t.Fatalf("provider call session=%q maximum=%d", provider.session, provider.maximum)
	}
}

func TestDurableSearchOverlayObservationReportsFailuresAndOverflow(t *testing.T) {
	for name, testCase := range map[string]struct {
		provider    *durableOverlayObservationProvider
		wantState   string
		wantWarning string
		wantError   string
	}{
		"snapshot failure": {
			provider:    &durableOverlayObservationProvider{VectorStore: vector.NewFakeVectorStore(), err: errors.New("D1 unavailable")},
			wantState:   "degraded",
			wantError:   "snapshot_read_failed",
			wantWarning: "",
		},
		"bounded overflow": {
			provider:    &durableOverlayObservationProvider{VectorStore: vector.NewFakeVectorStore(), snapshot: vector.DurableSearchOverlaySnapshot{PendingCount: 201, Truncated: true}},
			wantState:   "degraded",
			wantWarning: "correction_cap_exceeded",
			wantError:   "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := (&Server{Vector: testCase.provider}).durableSearchOverlayObservation(context.Background(), "")
			if got["state"] != testCase.wantState || got["warning"] != testCase.wantWarning || got["error"] != testCase.wantError {
				t.Fatalf("observation = %#v", got)
			}
		})
	}
}

func TestDurableSearchOverlayObservationReportsHardAlert(t *testing.T) {
	s := &Server{Vector: &durableOverlayObservationProvider{
		VectorStore: vector.NewFakeVectorStore(),
		snapshot:    vector.DurableSearchOverlaySnapshot{OldestPendingAt: time.Now().Add(-vector.DurableSearchOverlayHardAlertTTL)},
	}}
	got := s.durableSearchOverlayObservation(context.Background(), "")
	if got["state"] != "alert" || got["warning"] != "hard_alert_ttl_exceeded" {
		t.Fatalf("observation = %#v, want hard alert", got)
	}
}

func TestDurableSearchOverlayReadinessAddsStableChecks(t *testing.T) {
	s := &Server{Vector: &durableOverlayObservationProvider{
		VectorStore: vector.NewFakeVectorStore(),
		snapshot:    vector.DurableSearchOverlaySnapshot{PendingCount: 2},
	}}
	checks := map[string]string{}
	if degraded := s.addDurableSearchOverlayReadiness(context.Background(), checks); degraded {
		t.Fatalf("healthy overlay reported degraded: %#v", checks)
	}
	if checks["durable_search_overlay"] != "true" || checks["durable_search_overlay_state"] != "healthy" || checks["durable_search_overlay_pending_count"] != "2" {
		t.Fatalf("readiness checks = %#v", checks)
	}
}

func TestPrepareTurnVectorTraceIncludesDurableSearchOverlay(t *testing.T) {
	s := &Server{Vector: &durableOverlayObservationProvider{
		VectorStore: vector.NewFakeVectorStore(),
		snapshot:    vector.DurableSearchOverlaySnapshot{PendingCount: 1},
	}}
	trace := s.prepareTurnVectorShadow(context.Background(), dto.PrepareTurnRequest{ChatSessionID: "session-1"}, 5)
	overlay := mapFromAny(trace["durable_search_overlay"])
	if !boolFromAny(overlay["enabled"]) || overlay["pending_count"] != 1 || overlay["state"] != "healthy" {
		t.Fatalf("durable overlay trace = %#v", overlay)
	}
}
