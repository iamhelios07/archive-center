# Deploying Archive Center on Cloudflare

This is the operator's guide: what to create, in what order, and how to check each
step. Follow it top to bottom.

**Read step 1 and the reset section before you start.** Both involve data that
cannot be recovered, and one of them cannot be undone.

- [Before you start](#before-you-start)
- [What you are building](#what-you-are-building)
- [What survives a restart](#what-survives-a-restart)
- [Step 1 — create the resources](#step-1--create-the-resources)
- [Step 2 — apply the schema](#step-2--apply-the-schema)
- [Step 3 — render the deploy config](#step-3--render-the-deploy-config)
- [Step 4 — deploy the Worker](#step-4--deploy-the-worker)
- [Step 5 — deploy the Container](#step-5--deploy-the-container)
- [Step 6 — check that it came up](#step-6--check-that-it-came-up)
- [Optional: validate against real D1 and Vectorize first](#optional-validate-against-real-d1-and-vectorize-first)
- [Operating it](#operating-it)
- [Differences from the local runtime](#differences-from-the-local-runtime)

## Before you start

**A Cloudflare account.** Note which one: everything below is account-scoped.

**Node 22 or newer, and wrangler.** The version this was built against is
`wrangler 4.124.0`; a newer one is usually fine.

**An authenticated wrangler.**

```bash
cd deploy/cloudflare/worker
npx wrangler whoami
```

If that reports no account, log in:

```bash
npx wrangler login
```

`wrangler login` is enough — including for Vectorize. The scope list it prints
does not contain a Vectorize entry, which makes it look like Vectorize needs an
API token, but that is wrong: an OAuth login can create and query indexes. Confirm
it yourself before deciding you need more:

```bash
npx wrangler vectorize list
```

An API token also works if you prefer one. Set `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ACCOUNT_ID` in your environment.

**Decide your embedding dimension.** This is the one thing you must know before
you create anything, and it is covered in step 1. It comes from your embedding
model, and it is your setting, not a default this project can supply.

**Use dedicated resources.** Create a new D1 database and a new Vectorize index.
Do not point this at anything that already holds data for another project. The
schema migrations add tables, and `/admin/database-reset` genuinely deletes every
application row — see [Operating it](#operating-it).

## What you are building

```
RisuAI  ──HTTP──▶  Container (Go backend)
                        │
                        │  versioned JSON envelopes over HTTP
                        ▼
                   Worker  ──┬──▶  D1        (canonical rows)
                            └──▶  Vectorize  (rebuildable index)
```

The Worker is not optional and is not a shortcut. The Container holds a bridge URL
and a token and calls the Worker; it never calls the Cloudflare REST API. The
Worker is where authorization, audit and confirmation are enforced, so a container
that could reach D1 or Vectorize directly would be a second, unaccountable path to
your data.

**The Container is stateless.** It is replaced rather than repaired: a layer is
discarded on every deploy and a stopped instance keeps nothing. Everything that
has to outlive one is in D1.

## What survives a restart

Check this table before concluding that something is lost, because a restart and a
state loss look identical when nothing tells you which happened.

| State | Where it lives | Survives a restart |
| --- | --- | --- |
| Canonical rows (turns, memories, evidence, graph, canon) | D1 | yes |
| Reset progress and epoch fence | D1 (`d1_reset_runs`, `d1_reset_epoch`) | yes |
| Turn preparation settings (multi-agent roles, body tracking) | D1 (`d1_turn_preparation_settings`) | yes |
| Vector index | Vectorize, rebuildable from D1 | yes, by reindex |
| Operator jobs other than reset | process memory | **no** |
| Diagnostic logs | container filesystem | **no** |

## Step 1 — create the resources

### The embedding dimension

**Your Vectorize index dimension is determined by your embedding model, and that
model is a per-user setting:**

> RisuAI → settings → 기억 색인 LLM (Embedding)

Different users choose different models, so different installations need
different dimensions. There is no value this repository can declare on your
behalf.

**Find your number from data, not from configuration.** `AC_EMBEDDER_MODEL` in
`.env` is declarative metadata for preflight and reporting; it can disagree with
what was actually embedded. The authoritative record is per memory row:

```sql
-- against your local MariaDB
SELECT embedding_model, JSON_LENGTH(embedding) AS dim, COUNT(*)
FROM memories
WHERE embedding IS NOT NULL AND embedding <> ''
GROUP BY embedding_model, dim;
```

Your local ChromaDB cannot answer this. Chroma does not embed anything — the Host
computes the vectors and the backend stores them. Locally the width is never
declared in advance: Chroma takes it from the first upsert and rejects a later
mismatch. Vectorize is the opposite, and an index cannot be created without it.

For reference only, not as a lookup table: `text-embedding-3-small` is 1536,
`text-embedding-3-large` is 3072, `@cf/baai/bge-base-en-v1.5` is 768.

### Create them

```bash
cd deploy/cloudflare/worker

npx wrangler d1 create archive-center-db
npx wrangler vectorize create archive-center-vectors --dimensions=<N> --metric=cosine
```

`cosine` is not a preference — it is what the recall path expects, and what the
local ChromaDB path effectively scores by.

If your model happens to be one wrangler knows, `--preset` fills in both the
dimension and the metric for you:

```bash
npx wrangler vectorize create archive-center-vectors --preset @cf/baai/bge-base-en-v1.5
```

The presets available are `@cf/baai/bge-small-en-v1.5`,
`@cf/baai/bge-base-en-v1.5`, `@cf/baai/bge-large-en-v1.5`,
`openai/text-embedding-ada-002` and `cohere/embed-multilingual-v2.0`. Note that
`text-embedding-3-small` is **not** among them, so the common case still needs an
explicit `--dimensions`.

**Record both identifiers.** The D1 output prints a `database_id`; use the index
name for Vectorize.

Check what now exists:

```bash
npx wrangler d1 list
npx wrangler vectorize list
```

If you picked the wrong dimension, delete the index and create it again. Nothing
is in it yet, and that is much cheaper now than later.

## Step 2 — apply the schema

```bash
cd deploy/cloudflare
npx wrangler d1 migrations apply DB --remote
```

| File | Contents |
| --- | --- |
| `001_canonical_schema.sql` | Generated from the MariaDB migrations. Do not edit by hand; re-run `go run ./cmd/d1-schema-gen`. |
| `002_reset_control_plane.sql` | The durable, resumable reset: lease, epoch, runs. |
| `003_turn_preparation_settings.sql` | User settings the local runtime keeps in files. |
| `004_admin_jobs.sql` | Operator job snapshots. |

Check:

```bash
npx wrangler d1 migrations list DB --remote
```

That lists migrations that are **not yet applied**, so after a successful run it
should report nothing left to apply. If it still names files, the apply did not
finish and the next step will fail against a missing table.

## Step 3 — render the deploy config

The tracked `wrangler.template.toml` holds binding names and nothing
account-specific. Your identifiers go into a generated `wrangler.toml`, which is
gitignored.

```bash
cd C:\Users\myoun\code\archive-center    # or wherever you cloned it

export AC_CLOUDFLARE_D1_DATABASE_ID=<database_id from step 1>
export AC_CLOUDFLARE_VECTORIZE_INDEX_ID=archive-center-vectors
# optional, if you want a custom route or domain
export AC_CLOUDFLARE_WORKER_ROUTE=bridge.example.com

node deploy/cloudflare/scripts/render-wrangler-config.mjs
```

A missing value is a hard failure, never a rendered blank. A config with an empty
`database_id` would deploy cleanly and then fail on the first query, which is a
much worse place to find out. The script names the missing variable and never
prints a value it read.

Check that a config was produced and parses:

```bash
python -c "import tomllib; d=tomllib.load(open('deploy/cloudflare/wrangler.toml','rb')); print(d['d1_databases'][0]['database_id'] is not None, d['vectorize']['index_name'])"
```

## Step 4 — deploy the Worker

The bridge token is a Worker secret and the same value goes to the Container:

```bash
cd deploy/cloudflare
npx wrangler secret put BRIDGE_TOKEN
npx wrangler deploy
```

Check:

```bash
npx wrangler deployments list
```

`wrangler deploy` prints the Worker's URL. Note it — the Container needs it in
step 5.

The Worker refuses requests without `BRIDGE_TOKEN`, so a URL that answers 401 or
403 is working as intended rather than broken.

## Step 5 — deploy the Container

Build the image from the repository root:

```bash
docker build -f deploy/cloudflare/container/Dockerfile -t archive-center-backend .
```

Deploy it with the platform's Container tooling, or push the image to your own
registry and reference it. Either way it needs these variables:

| Variable | Value |
| --- | --- |
| `AC_RUNTIME_PROFILE` | `cloudflare` |
| `AC_STORE_MODE` | `cloudflare_authority` |
| `AC_VECTOR_MODE` | `cloudflare` |
| `AC_CLOUDFLARE_BRIDGE_URL` | the Worker URL from step 4 |
| `AC_CLOUDFLARE_BRIDGE_TOKEN` | the same value as `BRIDGE_TOKEN` |
| `AC_ENFORCE_AUTH` | `true` |
| `AC_BEARER_TOKEN` | an operator token you choose — **not** the bridge token |
| `AC_BIND_ADDR` | `0.0.0.0:28080` — already the image default |

Three of those will stop the deployment if they are wrong, and the log says so:

- Missing or mismatched bridge credentials fail startup with
  `cloudflare vectorize startup preflight failed (bridge=…)`.
- Missing `AC_ENFORCE_AUTH` / `AC_BEARER_TOKEN` fails startup with
  `requires operator authentication`. This is deliberate — see
  [Why operator authentication is required](#why-operator-authentication-is-required).
- `AC_BIND_ADDR` left at the local default `127.0.0.1` makes the Container
  unreachable from the platform's proxy. The symptom is a connection refused that
  looks like a crashed container.

### Why operator authentication is required

This is the only deployment shape of this project that is reachable from the
internet: the platform gives the Container a public address, which is the point of
running it there. Without a bearer token, `POST /admin/database-reset` answers
anyone who asks.

The reset's own confirmation token is not a substitute. It is a constant compiled
into the binary and readable in the source, so it stops a mistyped reset and
authorises nobody. The bearer token is the authorization.

Never put the token in the image or in `wrangler.toml`. A token there ends up in a
layer that gets pushed, cached and pulled.

## Step 6 — check that it came up

```bash
curl -H "Authorization: Bearer $AC_BEARER_TOKEN" https://<your-container>/ready
```

**Expect HTTP 503 at this point, and that is not a failure.** Every key in the
body is the useful part:

```json
{
  "checks": {
    "cloudflare_profile": "bootstrap_only",
    "cloudflare_parity": "incomplete",
    "ready_blocker": "cloudflare_parity_incomplete",
    "cloudflare_bridge": "configured",
    "store_capabilities": "<N>/<N>",
    "vector_accelerator": "vectorize",
    "vector_engine_policy": "vectorize_required",
    "vector_accelerator_reachable": "vectorize",
    "turn_preparation_settings": "durable"
  }
}
```

What each one tells you:

| Key | What you want to see |
| --- | --- |
| `cloudflare_bridge` | `configured` — the Container has a bridge URL and token |
| `vector_accelerator` | `vectorize`. This names the engine the deployment is **built around** |
| `vector_accelerator_reachable` | `vectorize` — the one reachable **right now** |
| `turn_preparation_settings` | `durable` — settings are in D1, not in a layer that will be discarded |
| `store_capabilities` | both sides equal. If they differ, `store_capabilities_missing` names what is absent |
| `cloudflare_profile` | `bootstrap_only` |

`cloudflare_profile: bootstrap_only` with `ready_blocker:
cloudflare_parity_incomplete` means the profile is not yet declared a
functional-parity deployment. The service runs and serves; it refuses to report
itself ready. That is the current intended state, and the 503 is the same fact
expressed as a status code — so do not read it as a broken deployment.

If a startup message names a dependency, it names the real one:
`cloudflare vectorize startup preflight failed (bridge=…)` means the bridge, not a
ChromaDB server, which does not exist in this deployment at all.

## Optional: validate against real D1 and Vectorize first

You do not have to deploy to find out whether the Worker and your bindings agree.
You can point a local Worker at your real D1 and Vectorize:

Add `remote = true` to the bindings in the rendered `deploy/cloudflare/wrangler.toml`:

```toml
[[d1_databases]]
binding = "DB"
database_id = "..."
remote = true

[vectorize]
binding = "VECTORIZE"
index_name = "..."
remote = true
```

Then:

```bash
cd deploy/cloudflare
npx wrangler dev
```

Binding calls now go to the real services. This is worth doing before step 5
because it is the cheapest way to learn that your dimension or your schema is
wrong, and because Vectorize has no local simulator — a local test cannot tell you
anything about how the real index behaves.

`remote = true` makes `wrangler dev` write to your real D1. Use the dedicated
resources from step 1.

## Operating it

### Reset — this deletes data

The reset clears every application row. It keeps the schema, the migration
metadata, and its own progress record.

```bash
curl -X POST https://<your-container>/admin/database-reset \
  -H "Authorization: Bearer $AC_BEARER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"debug":true,"confirm":"RESET_ARCHIVE_CENTER_DB","reset_vector":false,"operator":"your-name"}'
```

- `confirm` must match exactly. `debug` must be true.
- `reset_vector` defaults to true; set it false to clear rows and leave the index.
- `operator` is an optional label recorded as who asked. If you omit it, the
  request's peer address is recorded instead. The bearer token is never recorded.

It is resumable: it holds a maintenance lease, deletes in committed chunks and
checkpoints its cursor, so a Container replacement in the middle of one resumes
rather than restarting or double-deleting.

**Your settings survive.** Both this deployment and the local runtime keep a
user's agent configuration across a reset.

Vectors are not deleted at the same instant as the rows. The reset advances an
epoch and the vector purge trails it, so stale vectors stay ignored in the
meantime.

### Job status

```bash
curl -H "Authorization: Bearer $AC_BEARER_TOKEN" https://<your-container>/admin/jobs
```

Each job carries `durable` and `source`:

- `source: d1_reset_runs` — a reset. Survives a restart; a bookmarked
  `/admin/jobs/<id>` keeps working.
- `source: process_memory` — reindex, rescan, session-normalize, dedupe-cleanup.
  **Lost when the Container is replaced.** A job that was in flight comes back as
  `interrupted` with `resumable: false`, and you re-run it. It is not resumed for
  you, because the work it was doing cannot be reconstructed.

## Differences from the local runtime

The routes, request and response shapes, memory selection policy and recall
thresholds are the same. These are not:

| | Local | Cloudflare |
| --- | --- | --- |
| Canonical store | MariaDB | D1 (SQLite) |
| Vector engine | ChromaDB | Vectorize |
| Embedding dimension | implicit — from the first upsert | **declared — you set it in step 1** |
| Vector repair | atomic index swap | reindex; an atomic swap is not available |
| Settings | files beside the data directory | D1 |
| Admin jobs (non-reset) | process memory | process memory |
| Exposure | loopback or a LAN you choose | the public internet |

The dimension row is the one that costs you time if you skip it, and it is a
consequence of the engines rather than of this project — step 1 covers it.

The vector repair difference is permanent rather than pending. Vectorize has no
transaction and no delete-by-filter, so a session delete cannot be made atomic
against the canonical rows, and a rebuild is a reindex rather than an index swap.
It does not affect what recall returns: the canonical row is committed before the
vector cleanup, and a vector hit whose canonical row is absent is dropped instead
of delivered.

---

Maintainer notes — why the Container is shaped this way, what has and has not been
verified against a live account, and the Windows integrity issue that affects the
Worker test runner — are in `mydocs/working/task_m470_1_stage5.md` and in the
header comments of `container/Dockerfile` and
`worker/scripts/run-worker-tests.mjs`.
