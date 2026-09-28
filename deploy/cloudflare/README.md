# Archive Center on Cloudflare

This directory holds the Cloudflare deployment option: D1 as the canonical store,
Vectorize as the vector accelerator, a Worker as the bridge, and a Container
running the Go backend.

The local runtime — MariaDB plus ChromaDB, driven by a managed launcher on the
user's own machine — remains the default and is not changed by anything here.
This is a second way to run the same product, and the sections below say plainly
where the two are the same and where they are not.

- [Topology](#topology)
- [What is durable, and what is not](#what-is-durable-and-what-is-not)
- [Operator authentication](#operator-authentication)
- [Deploying](#deploying)
- [Administrative recovery and migration](#administrative-recovery-and-migration)
- [Local compatibility](#local-compatibility)
- [Remote test boundary](#remote-test-boundary)
- [Running the Worker tests on Windows](#running-the-worker-tests-on-windows)

## Topology

```
RisuAI  ──HTTP──▶  Container (Go backend, stateless)
                        │
                        │  cloudflarebridge: versioned JSON envelopes over HTTP
                        ▼
                   Worker  ──┬──▶  D1        (canonical rows)
                            └──▶  Vectorize  (delete-and-rebuildable index)
```

Two properties of this shape are load-bearing and easy to undo by accident.

**The Container never calls the Cloudflare REST API.** It holds a bridge URL and a
token and calls the Worker. A container that could mint its own D1 or Vectorize
credentials would have a second, unaccountable path to the canonical data, and
the Worker is where authorization, audit and confirmation are enforced.

**The Container is stateless.** It is replaced, not repaired. A layer is
discarded on every deploy and a stopped instance keeps nothing. Every route
through the Worker is therefore the only route, and every piece of state that
outlives one is in D1.

## What is durable, and what is not

This is the table to check before concluding that something is lost, because a
Container restart looks identical to a state loss when nothing says otherwise.

| State | Where it lives | Survives a restart |
| --- | --- | --- |
| Canonical rows (turns, memories, evidence, graph, canon) | D1 | yes |
| Administrative reset progress and epoch fence | D1 (`d1_reset_runs`, `d1_reset_epoch`) | yes |
| Turn preparation settings (multi-agent roles, body tracking) | D1 (`d1_turn_preparation_settings`) | yes |
| Vector index | Vectorize, rebuilt from D1 | yes, by reindex |
| Operator jobs other than reset | process memory | **no** |
| Diagnostic logs | container filesystem | **no** |

`GET /ready` states the first and last rows directly, so an operator does not
have to know the table to tell a durable setting from a lost one:

```json
{
  "checks": {
    "cloudflare_profile": "bootstrap_only",
    "cloudflare_parity": "incomplete",
    "store_capabilities": "82/83",
    "cloudflare_bridge": "configured",
    "vector_accelerator": "vectorize",
    "vector_engine_policy": "vectorize_required",
    "vector_accelerator_reachable": "vectorize",
    "turn_preparation_settings": "durable"
  }
}
```

Two of those keys exist because the report used to be wrong in a way that cost
time. `vector_accelerator` names the engine a deployment is **built around**;
`vector_accelerator_reachable` names one that is **reachable right now**. They
differ in fallback mode, where a deployment is built around ChromaDB and has
nothing connected to it. A startup failure names the dependency that actually
failed — `cloudflare vectorize startup preflight failed (bridge=…)` — rather than
naming a ChromaDB server that exists nowhere in this deployment.

## Operator authentication

**The Cloudflare profile refuses to start without it.** `AC_ENFORCE_AUTH=true` and
a non-empty `AC_BEARER_TOKEN` are required.

This profile is the only deployment shape that is reachable from the internet:
the platform gives a Container a public address, which is the reason to run it
there. `authMiddleware` is a pass-through when auth is not enforced, which is
correct for a service on loopback and wrong here. Without the requirement,
`POST /admin/database-reset` answers anyone who asks.

The route's own confirmation token is not a substitute. It is a constant compiled
into the binary and readable in the source, so it prevents a mistyped reset and
authorises nobody. The bearer token is the authorization; the confirmation token
is a typo guard.

The token is supplied by the platform, never baked into the image. A token in a
Dockerfile is a token in a layer that gets pushed, cached and pulled.

## Deploying

### 1. Provision the account-scoped resources

Create the D1 database and the Vectorize index, and note their identifiers. These
are account-scoped and the repository deliberately does not carry them.

### 2. Apply the schema

```bash
cd deploy/cloudflare
npx wrangler d1 migrations apply DB --remote
```

Migrations are numbered and applied in order:

| File | Contents |
| --- | --- |
| `001_canonical_schema.sql` | Generated from the MariaDB migrations. Do not edit by hand; re-run `go run ./cmd/d1-schema-gen`. |
| `002_reset_control_plane.sql` | The durable, resumable reset: lease, epoch, runs. |
| `003_turn_preparation_settings.sql` | User settings that the local runtime keeps in files. |

### 3. Render the deploy config

The tracked `wrangler.template.toml` contains binding names and nothing
account-specific. Identifiers, routes and secrets are materialised from your
environment into `wrangler.toml`, which is gitignored.

```bash
export AC_CLOUDFLARE_D1_DATABASE_ID=...
export AC_CLOUDFLARE_VECTORIZE_INDEX_ID=...
# optional
export AC_CLOUDFLARE_WORKER_ROUTE=bridge.example.com

node deploy/cloudflare/scripts/render-wrangler-config.mjs
```

A missing required value is a hard failure, never a rendered blank. A config
with an empty `database_id` deploys cleanly and then fails on the first query,
which is a much worse place to discover a missing variable.

The script never prints a value it read; it names the missing variable and
nothing else. Deploy logs get pasted into issues.

### 4. Set the bridge token and deploy the Worker

```bash
cd deploy/cloudflare
npx wrangler secret put BRIDGE_TOKEN
npx wrangler deploy
```

### 5. Build and deploy the Container

```bash
docker build -f deploy/cloudflare/container/Dockerfile -t archive-center-backend .
```

The image is a distroless static base with the Go backend and nothing else. It
has no shell and no package manager, which is what makes "there is nothing to
clean up" verifiable rather than merely intended.

Container environment:

| Variable | Value |
| --- | --- |
| `AC_RUNTIME_PROFILE` | `cloudflare` |
| `AC_STORE_MODE` | `cloudflare_authority` |
| `AC_VECTOR_MODE` | `cloudflare` |
| `AC_CLOUDFLARE_BRIDGE_URL` | the Worker's address, reachable from the container |
| `AC_CLOUDFLARE_BRIDGE_TOKEN` | the same value as the Worker secret |
| `AC_ENFORCE_AUTH` | `true` |
| `AC_BEARER_TOKEN` | the operator token, distinct from the bridge token |
| `AC_BIND_ADDR` | `0.0.0.0:28080` — already the image default |

`AC_BIND_ADDR` must not be left at the local default of `127.0.0.1`. A loopback
listener is unreachable from the platform's proxy, and the symptom is a
connection refused that looks like a crashed container.

## Administrative recovery and migration

### Reset

The reset is the one operator action whose truth is durable. It holds a global
maintenance lease with a fencing token, walks a child-first delete allowlist in
committed chunks, and checkpoints its cursor, so an interrupted reset resumes
instead of restarting or double-deleting.

```bash
curl -X POST https://<your-backend>/admin/database-reset \
  -H "Authorization: Bearer $AC_BEARER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"debug":true,"confirm":"RESET_ARCHIVE_CENTER_DB"}'
```

**Settings survive a reset.** The reset allowlist is held identical to MariaDB's
by a test, and MariaDB has no row for turn preparation settings because the local
runtime keeps them in a file that the reset never touched. Both providers keep a
user's agent configuration across a reset; adding the D1 table to the allowlist
would make Cloudflare delete settings the local runtime keeps, which is exactly
the divergence this work exists to remove.

Vectors are not deleted atomically with the rows. The reset advances an epoch,
and `vector_purged_epoch` trails until the Vectorize purge for that epoch
completes; stale vectors stay fenced in the meantime.

### Job status

```bash
curl -H "Authorization: Bearer $AC_BEARER_TOKEN" https://<your-backend>/admin/jobs
```

Each job carries `durable` and `source`. Reset runs come from
`d1_reset_runs` and survive a restart. Reindex, rescan, session-normalize and
dedupe-cleanup are still process memory and are still lost on a replacement —
the flags say so rather than letting the list look equally trustworthy in both
cases.

A bookmarked `/admin/jobs/<id>` keeps working for a reset run across a restart.
The id is durable precisely because the process that created it is gone.

## Local compatibility

What is the same: the routes, the request and response shapes, the memory
selection policy, the reset's confirmation and lease semantics, and the recall
thresholds. Stage 4 established that the observable recall result is the same
because both engines are scored by cosine similarity and the same threshold
applies.

What differs, deliberately:

| | Local | Cloudflare |
| --- | --- | --- |
| Canonical store | MariaDB | D1 (SQLite) |
| Vector engine | ChromaDB | Vectorize |
| Concurrency | `FOR UPDATE`, transactions | D1 batching, one bounded modification |
| Vector repair | atomic index swap with a resumable journal | reindex; an atomic swap is not available |
| Settings | files beside the data directory | D1 |
| Admin jobs (non-reset) | process memory | process memory |
| Exposure | loopback or a LAN you choose | the public internet |

The Vectorize difference is not a gap to be closed later. Vectorize's entire
surface is `upsert`/`insert`/`deleteByIds`/`getByIds`/`query`/`queryById`/
`describe`: there is no transaction and no delete-by-filter, so a session delete
cannot be made atomic against the canonical store. Correctness does not depend on
it — the canonical session row is committed before the vector cleanup, and a
vector hit whose canonical row is absent is counted and dropped rather than
delivered — and the provider refuses to pretend otherwise by rejecting a
`Rebuild` outright instead of silently doing something that looks like one.

## Remote test boundary

**Everything in this directory has been verified locally. Nothing has been
verified against a deployed Cloudflare account, because this repository has no
account, no account id, no API token, no D1 database id, no Vectorize index id
and no deployed or preview Worker.**

What was verified, and how:

| | How |
| --- | --- |
| D1 store behaviour | `modernc.org/sqlite`, the same engine D1 runs, including the real `ON CONFLICT` upsert and composite keys |
| Vectorize bridge payloads | 19 tests in real `workerd` against the real Worker source |
| Vectorize error classification | stubbed at the bridge client, because Vectorize has no local simulator |
| Container image | built and run locally; a Cloudflare profile was started and observed to refuse an unreachable bridge by name |
| Deploy config | rendered and parsed with a real TOML parser; account-neutrality scanned |
| The image running on Cloudflare | **not verified** |

The gap that matters most is the last one. Whether Cloudflare starts this image,
and how it routes to the exposed port, is untested. The first authenticated
deployment is where that is found.

Vectorize additionally cannot be exercised locally at all: the runtime classifies
it as a remote-only binding, so the behaviour that governs the outbox's
retry-versus-permanent-park decision — which errors Vectorize raises — is owned by
a remote harness.

## Running the Worker tests on Windows

`npm test` goes through `deploy/cloudflare/worker/scripts/run-worker-tests.mjs`,
which exists for one reason.

This repository carries a **Low** mandatory integrity label, so `workerd.exe` —
which lives inside it — runs as a Low integrity process. The default `%TEMP%` is
**Medium**, and Windows Mandatory Integrity Control forbids a Low process from
writing into a Medium directory. Every disk-backed binding, D1 and KV alike,
therefore failed at startup with an access error naming a directory the runtime
creates. It reads exactly like a workerd bug and is not one: workerd runs fine
here, and the tests that need no disk storage always passed.

The script hands the runtime a Low-labelled scratch directory, which puts both
sides of that write on the same level. The alternative, relabelling `workerd.exe`
up to Medium, also works but weakens the security posture of a 96 MB third-party
binary and `npm install` restores the original label on every reinstall, so it
would silently break again.

The test runner is wired into CI. Installing dependencies uses `npm ci`: the
lockfile is committed and complete, and installing from it never resolves the
dependency graph, which is the step npm 10 crashes on.
