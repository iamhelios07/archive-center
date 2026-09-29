package store

import (
	"context"
	"errors"
	"fmt"
)

// D1 guidance plan state capability.
//
// guidance_plan_states is the K-2 per-session cache of the narrative plan
// snapshot: the story plan, the director, the build status, the turn the
// snapshot was built from, and any warnings the build produced. Three routes read
// or write it (buildL3GuidanceSnapshot, the narrative-control rebuild, and the
// director patch), and each of them asserts GuidancePlanStateStore directly. A
// route that finds the capability missing degrades instead of failing: the read
// routes return state_status "no_state" and the write routes skip the cache. That
// degradation is silent, so implementing the capability is what makes the D1
// provider behave like the canonical one rather than quietly rebuilding a plan on
// every request.
//
// The statements are translated from the MariaDB reference in mariadb_delete.go
// rather than re-derived, because four of their details are load bearing and easy
// to lose in a rewrite:
//
//   - The read is a bare equality on chat_session_id with no ORDER BY and no
//     LIMIT, because the table carries a UNIQUE constraint on that column. The
//     D1 schema declares the same constraint, so at most one row can match and
//     the read is single-row by construction rather than by a row limit that
//     happens to be big enough.
//   - The write is a true upsert, not an insert. MariaDB's ON DUPLICATE KEY UPDATE
//     becomes SQLite's ON CONFLICT (chat_session_id) DO UPDATE here. The conflict
//     branch deliberately omits created_at, so an existing row keeps its original
//     creation time while updated_at advances. That is the same reasoning as
//     SaveStatusCurrentValue: the session's plan cache has one lifetime, and a
//     rewrite is not a new row.
//   - Both created_at and updated_at are written from one resolved timestamp, and
//     the timestamp comes from item.UpdatedAt when the caller supplied one. The
//     narrative-control rebuild always sets UpdatedAt explicitly; the director
//     patch does not, so an unset timestamp is stamped with the current UTC time
//     rather than being persisted as the zero time. A zero time in this column
//     would sort the row as older than every real write on the
//     idx_guidance_plan_updated index.
//   - The text columns are passed through verbatim, not through d1NullableString.
//     MariaDB binds the caller's strings as-is, so an empty plan is stored as an
//     empty string and only a rollback leaves the column NULL. Mapping "" to NULL
//     here would make the two providers disagree at the column level for a
//     comparison that reads the table directly, even though the Go-level read
//     renders both as "".
//
// MariaDB's ON UPDATE CURRENT_TIMESTAMP(3) never fires on this table, because the
// upsert assigns updated_at explicitly. Writing the value from Go therefore
// reproduces the stored result exactly, and the D1 schema needs no trigger to
// match it.
//
// The rollback write is not here. DeleteGuidancePlanState is the rewind half of
// the same table and is implemented with the other RollbackStore mutations in
// d1_rollback_capabilities.go, where the invalidation UPDATE and its batched
// neighbours belong together.

var _ GuidancePlanStateStore = (*d1Store)(nil)

// d1GuidancePlanStateSelect is the MariaDB projection, unchanged. state_status is
// read as a nullable column even though both schemas declare it NOT NULL, because
// the reference reads it through a nullable scan and a future migration must not
// change the representation this method returns.
const d1GuidancePlanStateSelect = `
		SELECT id, chat_session_id, story_plan_json, director_json, state_status, last_turn,
		       warnings_json, created_at, updated_at
		FROM guidance_plan_states`

// GetGuidancePlanState returns the cached plan snapshot for one chat session.
//
// A session with no cached state reports ErrNotFound, exactly as the MariaDB path
// maps sql.ErrNoRows. That mapping is what the read routes branch on: they treat
// ErrNotFound as "rebuild on next request" and any other error as a read failure,
// so collapsing the two would either hide a transport problem or turn an ordinary
// cold cache into an error the operator sees.
//
// The nullable text columns are rendered as the empty string rather than left
// distinguishable. The rollback write NULLs the three JSON columns, and the read
// routes call json.Unmarshal only on a non-empty string, so a NULL and an empty
// string must arrive as the same value or a rolled-back session would report a
// different plan from a never-built one.
func (s *d1Store) GetGuidancePlanState(ctx context.Context, chatSessionID string) (*GuidancePlanState, error) {
	var item GuidancePlanState
	var storyPlanJSON, directorJSON, stateStatus, warningsJSON *string
	err := s.conn.QueryRow(ctx, d1GuidancePlanStateSelect+`
		WHERE chat_session_id = ?`, chatSessionID).Scan(
		&item.ID, &item.ChatSessionID, &storyPlanJSON, &directorJSON, &stateStatus,
		&item.LastTurn, &warningsJSON, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	item.StoryPlanJSON = d1DerefString(storyPlanJSON)
	item.DirectorJSON = d1DerefString(directorJSON)
	item.StateStatus = d1DerefString(stateStatus)
	item.WarningsJSON = d1DerefString(warningsJSON)
	return &item, nil
}

// UpsertGuidancePlanState writes the plan snapshot for one chat session, replacing
// whatever was cached there before.
//
// A nil item is rejected. The reference would dereference it and panic, so this
// is the one place the port is stricter than the statement it translates: a panic
// inside a route handler is a process failure, and every caller in this repository
// passes a literal struct, so the guard costs nothing and removes the only way
// this method can take the process down.
//
// A blank chat session id is deliberately NOT rejected. The reference has no such
// guard, the column is NOT NULL rather than non-empty, and every write path
// derives the id from a route parameter that has already been validated. Adding a
// check here would reject a write the MariaDB provider accepts, which is a
// behavioural divergence rather than a safety improvement.
//
// The caller's struct is not modified. The reference does not report the assigned
// id or the resolved timestamp back, and the write sites ignore the error anyway
// because a stale cache is always safe to rebuild, so returning them would imply a
// contract the two providers do not share.
func (s *d1Store) UpsertGuidancePlanState(ctx context.Context, item *GuidancePlanState) error {
	if item == nil {
		return fmt.Errorf("store: guidance plan state is required")
	}
	// d1TimeValue resolves a zero UpdatedAt to the current UTC time and renders
	// the RFC3339 form the D1 schema stores, which is the same substitution the
	// reference performs with its own zero check.
	now := d1TimeValue(item.UpdatedAt)
	_, err := s.conn.Exec(ctx, `
		INSERT INTO guidance_plan_states (
			chat_session_id, story_plan_json, director_json, state_status, last_turn, warnings_json,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (chat_session_id) DO UPDATE SET
			story_plan_json = excluded.story_plan_json,
			director_json   = excluded.director_json,
			state_status    = excluded.state_status,
			last_turn       = excluded.last_turn,
			warnings_json   = excluded.warnings_json,
			updated_at      = excluded.updated_at
	`, item.ChatSessionID, item.StoryPlanJSON, item.DirectorJSON, item.StateStatus,
		item.LastTurn, item.WarningsJSON, now, now)
	return err
}
