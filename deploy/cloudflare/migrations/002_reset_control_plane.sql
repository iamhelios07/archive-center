-- Archive Center Cloudflare D1 — reset control plane.
--
-- These tables own the durable state of the FK-safe, resumable administrative
-- reset described in the implementation plan ("D1 FK-safe resumable reset 설계
-- 확정"). They are CONTROL PLANE, not application data: the reset's delete
-- allowlist must never include them, so a reset can always report its own
-- progress and resume after a Container restart.
--
-- Why a dedicated state machine instead of MariaDB's ResetAll:
--   * MariaDB disables foreign keys and deletes 72 tables in one transaction.
--     D1 enforces foreign keys and bounds a single modification, so the reset
--     must be chunked, child-first, and resumable.
--   * The existing adminJobManager is process memory only and is lost when a
--     stateless Container restarts, so it cannot own reset truth.
--   * D1 deletion and Vectorize purge cannot commit atomically, so an epoch
--     fence plus persistent purge state is required.
--
-- sqlite_sequence is deliberately untouched by reset: MariaDB's reset uses
-- DELETE (not TRUNCATE), so AUTO_INCREMENT counters are preserved, and SQLite
-- preserves sqlite_sequence across DELETE as well. Clearing it would restart
-- ids at 1 and break MariaDB parity.

-- Global maintenance lease. A reset holds this lease with a monotonic fencing
-- token so a resumed or concurrent worker cannot act on a stale claim.
CREATE TABLE IF NOT EXISTS "d1_maintenance_lease" (
    "lease_name"    TEXT PRIMARY KEY NOT NULL,
    "holder"        TEXT,
    "fencing_token" INTEGER NOT NULL DEFAULT 0,
    "acquired_at"   TEXT,
    "expires_at"    TEXT,
    "note"          TEXT
);

-- Single-row epoch counter. current_epoch advances when a reset starts;
-- vector_purged_epoch advances only after the Vectorize purge for that epoch
-- completes. Stale vectors are ignored while the two differ.
CREATE TABLE IF NOT EXISTS "d1_reset_epoch" (
    "epoch_id"            INTEGER PRIMARY KEY CHECK ("epoch_id" = 1),
    "current_epoch"       INTEGER NOT NULL DEFAULT 0,
    "vector_purged_epoch" INTEGER NOT NULL DEFAULT 0,
    "updated_at"          TEXT NOT NULL
);

-- Durable reset run. table_index/table_name/last_key form the resumable cursor
-- over the child-first allowlist; each committed chunk advances them so a
-- restart continues instead of restarting or double-deleting.
CREATE TABLE IF NOT EXISTS "d1_reset_runs" (
    "reset_run_id"   TEXT PRIMARY KEY NOT NULL,
    "epoch"          INTEGER NOT NULL,
    "status"         TEXT NOT NULL CHECK ("status" IN ('running', 'purging_vectors', 'completed', 'failed')),
    "table_index"    INTEGER NOT NULL DEFAULT 0,
    "table_name"     TEXT,
    "last_key"       TEXT,
    "rows_deleted"   INTEGER NOT NULL DEFAULT 0,
    "tables_cleared" INTEGER NOT NULL DEFAULT 0,
    "started_at"     TEXT NOT NULL,
    "updated_at"     TEXT NOT NULL,
    "completed_at"   TEXT,
    "last_error"     TEXT,
    "retry_count"    INTEGER NOT NULL DEFAULT 0,
    "fencing_token"  INTEGER NOT NULL,
    "confirmation"   TEXT NOT NULL,
    "requested_by"   TEXT,
    -- active_marker collapses both in-flight statuses to one constant so a
    -- unique index can enforce a single active run. A unique index on "status"
    -- alone would allow one 'running' AND one 'purging_vectors' row at the same
    -- time, because they are different values.
    "active_marker"  TEXT GENERATED ALWAYS AS
        (CASE WHEN "status" IN ('running', 'purging_vectors') THEN 'active' END) VIRTUAL
);

CREATE INDEX IF NOT EXISTS "ix_d1_reset_runs_recent"
    ON "d1_reset_runs" ("started_at" DESC);

-- At most one reset may be in flight. Terminal runs keep active_marker NULL, and
-- SQLite treats NULLs as distinct, so completed and failed runs accumulate as
-- history while a second concurrent reset is rejected.
CREATE UNIQUE INDEX IF NOT EXISTS "uq_d1_reset_runs_active"
    ON "d1_reset_runs" ("active_marker");

-- Seed the single epoch row. INSERT OR IGNORE keeps the migration idempotent.
INSERT OR IGNORE INTO "d1_reset_epoch" ("epoch_id", "current_epoch", "vector_purged_epoch", "updated_at")
VALUES (1, 0, 0, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));

-- Seed the lease row so later CAS updates always have a row to update.
INSERT OR IGNORE INTO "d1_maintenance_lease" ("lease_name", "fencing_token", "note")
VALUES ('admin_reset', 0, 'reset maintenance lease');
