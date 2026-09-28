-- Archive Center Cloudflare D1 — administrative job snapshots.
--
-- The in-process job manager is memory only, and a Cloudflare Container is
-- replaced rather than repaired. A reindex or a rescan that was in flight when
-- the previous instance went away therefore disappeared with no trace: the
-- operator polled /admin/jobs, saw an empty list, and could not tell "nothing
-- was running" from "the record of what was running was lost".
--
-- Reset already had durable state of its own (d1_reset_runs, the maintenance
-- lease and the epoch fence) because a reset has a resumable chunk cursor.
-- These jobs do not. There is no cursor to resume from, so this table records
-- what happened and explicitly records that an in-flight job was INTERRUPTED,
-- rather than restoring it as still running. Restoring a dead job as running
-- would be the more comfortable answer and the false one: nothing is executing
-- it, and an operator watching a progress bar that no longer advances has no
-- way to know that.
--
-- The operator re-runs the job. That is a deliberate, visible cost, chosen over
-- a resume that would need per-job cursor semantics and would still be a guess
-- about what the previous instance had already committed.

CREATE TABLE IF NOT EXISTS "d1_admin_jobs" (
    "job_id"      TEXT PRIMARY KEY NOT NULL,
    "kind"        TEXT NOT NULL,
    -- '' for a job that is not scoped to a session.
    "session_id"  TEXT NOT NULL DEFAULT '',
    -- The status at the time of writing. 'interrupted' is written by the
    -- restore path, never by the running job.
    "status"      TEXT NOT NULL,
    "snapshot_json" TEXT NOT NULL,
    "started_at"  TEXT NOT NULL,
    "updated_at"  TEXT NOT NULL,
    -- True once a job has reached a state it cannot leave. Terminal snapshots
    -- are written on every transition and are never rewritten afterwards, which
    -- is what bounds how much this table can grow per job.
    "terminal"    INTEGER NOT NULL DEFAULT 0
);

-- The listing is "recent jobs newest first", and the restore is "everything not
-- yet terminal", so updated_at leads both.
CREATE INDEX IF NOT EXISTS "ix_d1_admin_jobs_recent"
    ON "d1_admin_jobs" ("updated_at" DESC);

CREATE INDEX IF NOT EXISTS "ix_d1_admin_jobs_open"
    ON "d1_admin_jobs" ("terminal", "updated_at" DESC);
