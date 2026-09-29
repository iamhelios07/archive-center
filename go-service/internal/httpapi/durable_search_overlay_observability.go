package httpapi

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// durableSearchOverlayObservation is intentionally read-only. It uses the same
// D1 snapshot capability as Search but neither retries outbox work nor changes
// visibility state while an operator or trace is being rendered.
func (s *Server) durableSearchOverlayObservation(ctx context.Context, sessionID string) map[string]any {
	observation := map[string]any{
		"enabled":                    false,
		"state":                      "disabled",
		"pending_count":              0,
		"oldest_pending_age":         "0s",
		"oldest_pending_age_seconds": 0,
		"warning":                    "",
		"error":                      "",
	}
	if s.Vector == nil {
		return observation
	}
	provider, ok := s.Vector.(vector.DurableSearchOverlayProvider)
	if !ok {
		return observation
	}

	snapshot, err := provider.DurableSearchOverlay(ctx, sessionID, vector.DurableSearchOverlayMaxDocuments)
	if errors.Is(err, vector.ErrNotEnabled) {
		return observation
	}
	observation["enabled"] = true
	if err != nil {
		// The raw storage error belongs in protected server logs. A stable category
		// is enough for public readiness and prepare-turn diagnostics.
		observation["state"] = "degraded"
		observation["error"] = "snapshot_read_failed"
		return observation
	}

	observation["pending_count"] = snapshot.PendingCount
	if snapshot.Truncated {
		observation["state"] = "degraded"
		observation["warning"] = "correction_cap_exceeded"
		return observation
	}
	if snapshot.OldestPendingAt.IsZero() {
		observation["state"] = "healthy"
		return observation
	}

	age := time.Since(snapshot.OldestPendingAt)
	if age < 0 {
		age = 0
	}
	ageSeconds := int64(age / time.Second)
	observation["oldest_pending_age"] = age.Round(time.Second).String()
	observation["oldest_pending_age_seconds"] = ageSeconds
	switch {
	case age >= vector.DurableSearchOverlayHardAlertTTL:
		observation["state"] = "alert"
		observation["warning"] = "hard_alert_ttl_exceeded"
	case age >= vector.DurableSearchOverlaySoftTTL:
		observation["state"] = "degraded"
		observation["warning"] = "soft_ttl_exceeded"
	default:
		observation["state"] = "healthy"
	}
	return observation
}

// addDurableSearchOverlayReadiness emits stable scalar checks for /ready and
// returns whether its condition should mark an otherwise-ready profile degraded.
func (s *Server) addDurableSearchOverlayReadiness(ctx context.Context, checks map[string]string) bool {
	observation := s.durableSearchOverlayObservation(ctx, "")
	checks["durable_search_overlay"] = strconv.FormatBool(boolFromAny(observation["enabled"]))
	checks["durable_search_overlay_state"] = stringFromAny(observation["state"])
	checks["durable_search_overlay_pending_count"] = strconv.Itoa(intFromAny(observation["pending_count"], 0))
	checks["durable_search_overlay_oldest_pending_age"] = stringFromAny(observation["oldest_pending_age"])
	checks["durable_search_overlay_oldest_pending_age_seconds"] = strconv.FormatInt(int64(intFromAny(observation["oldest_pending_age_seconds"], 0)), 10)
	checks["durable_search_overlay_warning"] = stringFromAny(observation["warning"])
	checks["durable_search_overlay_error"] = stringFromAny(observation["error"])
	state := stringFromAny(observation["state"])
	return state == "degraded" || state == "alert"
}
