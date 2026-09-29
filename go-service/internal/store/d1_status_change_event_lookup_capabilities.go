package store

import (
	"context"
)

// D1 exact source-fenced status-event lookup capability.
//
// StatusChangeEventSourceLookupStore is the read half of the reversible status
// ledger, and its doc comment is the contract this file implements: the answer
// must be read out of the event evidence envelope (source_revision,
// current_projection) rather than approximated with a recent-row cap. That single
// sentence is why the two methods here look nothing alike, and why the asymmetry
// between them is deliberate rather than an inconsistency to be smoothed over:
//
//   - GetStatusChangeEventBySourceRevision is an EVIDENCE lookup. It answers
//     "which event did this accepted source produce", so it has to keep
//     answering after that source is rolled back. The reference therefore does
//     not join memory_source_revisions, and neither does this port: adding the
//     lifecycle fence here would make the lookup go silent in exactly the
//     situation a rollback audit is performed in.
//   - GetLatestCurrentProjectionStatusChangeEvent is a PROJECTION lookup. It
//     answers "what is the current state", so a rolled-back source is no longer
//     evidence of anything and the reference joins the revision table to exclude
//     it.
//
// ---------------------------------------------------------------------------
// Why this read is not allowed to approximate
// ---------------------------------------------------------------------------
//
// The consumer is turn_story_clock.go. After a non-canonical rollback,
// DeleteStatusCurrentValues has already removed the story clock's current row,
// and restoreStoryClockCurrentAfterRollback rebuilds it from
// GetLatestCurrentProjectionStatusChangeEvent. When a store does not implement
// the interface, that function returns (0, nil) at the type assertion and the
// story clock is never restored at all. The failure mode an approximation would
// introduce is not that obvious one: it returns a well-formed event carrying a
// plausible but wrong clock, and the consumer writes that row back as current
// truth with no error anywhere. A recent-row cap (the created_at DESC, id DESC
// list read this same package already exposes) answers such a lookup with the
// newest event regardless of whether the user has rolled back the source that
// produced it, so the tests below contrast the two directly instead of only
// asserting the right answer.
//
// Neither reference method opens a transaction or takes a row lock, so there is
// no FOR UPDATE guarantee to re-establish and no batch to reproduce: both are
// single statement reads. Both map the absence of a row to ErrNotFound, which is
// the identity the consumer branches on to distinguish "nothing to restore" from
// a real failure that must abort the rollback.

var _ StatusChangeEventSourceLookupStore = (*d1Store)(nil)

// d1StatusEventLookupBySourceRevision is GetStatusChangeEventBySourceRevision.
//
// The revision is reachable only through the evidence path, because
// status_change_events has no source_revision column; that is the whole reason
// the interface forbids approximating this read. The comparison is deliberately
// left WITHOUT the COALESCE that the transition idempotency probe in
// d1StatusTransitionEventLookup needs: there a blank lookup key and a blank
// stored revision are the same lookup, so a NULL has to match. Here the caller
// supplies a concrete revision, and a COALESCE would add a blank-revision branch
// the reference does not have, answering with an event that has no source at all
// when the caller passes the empty string.
//
// source_turn is compared as the COLUMN it is, not as a JSON path. Reading the
// turn out of the evidence as well would let a lookup be satisfied by an event
// whose recorded source_turn is a different turn entirely, which is the exact
// mismatch a caller comparing (revision, turn) to rebuild a turn is trying to
// avoid.
//
// The column list is the shared d1StatusChangeEventSelect, reused rather than
// copied: this query has no join, so the unaliased names are unambiguous and a
// second copy of the projection would be a second contract.
var d1StatusEventLookupBySourceRevision = d1StatusChangeEventSelect + `
	WHERE chat_session_id = ?
	  AND status_key = ?
	  AND source_turn = ?
	  AND json_extract(evidence_json, ` + d1JSONPath("source_revision") + `) = ?
	ORDER BY id DESC
	LIMIT 1`

// d1StatusEventLookupObservationTurn is d1StatusObservationTurnSQL("event"),
// substituted at package initialisation because the expression embeds a
// function call and therefore cannot be a constant expression.
var d1StatusEventLookupObservationTurn = d1StatusObservationTurnSQL("event")

// d1StatusEventLookupLatestCurrentProjection is
// GetLatestCurrentProjectionStatusChangeEvent.
//
// The lifecycle_state filter belongs on the LEFT JOIN, not in the WHERE, for the
// same reason it does in the transition file: moving it into the WHERE turns the
// outer join into an inner one and silently drops every slot whose source
// revision was rolled back, which would promote the NEXT oldest event into the
// answer instead of reporting that this status key no longer has a current
// projection. Eligibility is then decided by d1StatusProjectionSourceSQL, the
// same rule the current-value rebuild read uses, so this lookup and
// ListLatestReversibleCurrentProjectionEvents cannot disagree about which event
// the current projection is.
//
// d1CurrentProjectionPredicate is reused verbatim and accepts both the JSON
// boolean true that ApplyReversibleStatusTransition writes and the historic JSON
// string form. Testing json_extract(...) = 'true' alone would match only the
// string form, so the read would report ErrNotFound for the majority of real
// projection events and the consumer would silently restore nothing.
//
// The column list repeats d1StatusChangeEventSelect with every name qualified by
// the event alias, and the qualification is mandatory rather than stylistic. The
// join contributes memory_source_revisions, which has its own id, chat_session_id
// and created_at, so reusing the shared list unaliased makes SQLite reject the
// whole statement with "ambiguous column name: id" before it evaluates a single
// row. Nothing else differs from the shared list, which is why the columns are
// spelled out here instead of being mechanically derived from a name that is
// already bound to a different FROM.
//
// It is a var rather than a const because it embeds the dialect helper calls; the
// three expressions it composes are themselves vars, so the whole statement is
// resolved at package initialisation in dependency order.
var d1StatusEventLookupLatestCurrentProjection = `
	SELECT event.id, event.chat_session_id, event.registry_id, event.status_value_id,
	       event.status_key, event.owner_scope, event.owner_id,
	       event.event_kind, event.previous_value_json, event.new_value_json,
	       event.evidence_json, event.source_turn,
	       event.story_clock_json, event.event_state, event.created_at
	FROM status_change_events event
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = event.chat_session_id
	 AND source_revision.source_revision = json_extract(event.evidence_json, ` + d1JSONPath("source_revision") + `)
	 AND source_revision.lifecycle_state = 'active'
	WHERE event.chat_session_id = ?
	  AND ` + d1StatusProjectionSourceSQL("event", "source_revision") + `
	  AND event.status_key = ?
	  AND ` + d1CurrentProjectionPredicate("event.evidence_json") + `
	ORDER BY ` + d1StatusEventLookupObservationTurn + ` DESC, event.id DESC
	LIMIT 1`

// GetStatusChangeEventBySourceRevision resolves the event one accepted source and
// turn produced, newest first.
//
// "Newest first" is the load-bearing part of the ORDER BY id DESC: one revision
// and one turn can legitimately emit more than one event for the same status key
// (a change and the observation that follows it), and a caller rebuilding a turn
// must see the latest one rather than the first ever written.
//
// No input is normalised. The reference trims nothing here, and trimming the
// revision would make a lookup for a padded revision succeed where MariaDB
// reports ErrNotFound, so a caller that depended on the miss would instead be
// handed an event.
func (s *d1Store) GetStatusChangeEventBySourceRevision(ctx context.Context, chatSessionID, statusKey, sourceRevision string, sourceTurn int) (StatusChangeEvent, error) {
	return d1StatusTransitionScanEvent(s.conn.QueryRow(ctx,
		d1StatusEventLookupBySourceRevision, chatSessionID, statusKey, sourceTurn, sourceRevision))
}

// GetLatestCurrentProjectionStatusChangeEvent resolves the observation that
// currently owns a status key.
//
// The ORDER BY is the observation order, not the write order. Two things break
// if it is replaced by created_at or by id alone:
//
//   - A late-arriving source observed an earlier turn but was written later. The
//     projection writer refuses to install exactly that state
//     (ErrStatusProjectionStale), so a lookup ordered by recency would report
//     back a value the store itself would have refused to store.
//   - An explicit state repair corrects an old turn but records when the
//     correction entered the projection in repair_recorded_turn. Ordering by the
//     row's own source_turn would let the history it corrects outrank the
//     correction and hand the consumer a clock the user already fixed.
//
// event.id DESC is the tiebreak for two events sharing one observation turn, and
// it is the same tiebreak the anti-join in
// d1StatusTransitionLatestProjectionEvents uses, so the two reads always name the
// same winner.
//
// The LIMIT 1 here is the reference's "one winner", not a window: the whole
// eligible set is ordered first and then truncated, which is what makes this read
// unbounded by window in the sense the interface requires. Capping the scan
// before the ordering is the approximation the interface doc forbids.
func (s *d1Store) GetLatestCurrentProjectionStatusChangeEvent(ctx context.Context, chatSessionID, statusKey string) (StatusChangeEvent, error) {
	return d1StatusTransitionScanEvent(s.conn.QueryRow(ctx,
		d1StatusEventLookupLatestCurrentProjection, chatSessionID, statusKey))
}
