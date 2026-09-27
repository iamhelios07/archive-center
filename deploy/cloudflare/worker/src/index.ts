// Archive Center Cloudflare Worker — Container-to-Cloudflare bridge.
//
// Stage 2 contract: this Worker receives versioned envelope requests from the
// Archive Center Go backend running in a Cloudflare Container and dispatches
// them to D1/Vectorize bindings. Stage 2 fixes the authorization context and
// the request/response/error contract only: bridge.ping and bridge.health are
// implemented, and every data operation answers with a typed
// "not_implemented" envelope error until the parity stages land.
//
// Account neutrality: no account ID, database ID, index ID, route, or domain
// appears here. Bindings are referenced by stable names only (DB, VECTORIZE)
// and the shared bridge secret is injected as a Worker secret (BRIDGE_TOKEN).

export interface Env {
  /** Shared Container-to-Worker bridge token, set via `wrangler secret put BRIDGE_TOKEN`. */
  BRIDGE_TOKEN: string;
  /** D1 binding for the canonical Archive Center database. */
  DB: D1Database;
  /** Vectorize binding for the rebuildable semantic-search accelerator. */
  VECTORIZE: Vectorize;
}

/** Bridge envelope contract version; must match go-service EnvelopeVersion. */
const ENVELOPE_VERSION = 1;

interface BridgeRequest {
  version: number;
  id: string;
  operation: string;
  payload?: unknown;
}

interface BridgeResponse {
  version: number;
  id: string;
  ok: boolean;
  result?: unknown;
  error_code?: string;
  error_message?: string;
  retryable?: boolean;
}

function envelopeError(id: string, code: string, message: string, retryable = false): BridgeResponse {
  return {
    version: ENVELOPE_VERSION,
    id,
    ok: false,
    error_code: code,
    error_message: message,
    retryable,
  };
}

function jsonResponse(status: number, body: BridgeResponse): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Timing-safe bearer comparison: both sides are hashed before comparing. */
async function tokenMatches(provided: string, expected: string): Promise<boolean> {
  if (!provided || !expected) {
    return false;
  }
  const encoder = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(provided)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const left = new Uint8Array(a);
  const right = new Uint8Array(b);
  if (left.length !== right.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < left.length; i++) {
    diff |= left[i] ^ right[i];
  }
  return diff === 0;
}

function bindingState(binding: unknown): "bound" | "missing" {
  return binding === undefined || binding === null ? "missing" : "bound";
}

// ---------------------------------------------------------------------------
// D1 handlers
// ---------------------------------------------------------------------------

/** A malformed request, as opposed to a failing database operation. */
class D1RequestError extends Error {}

function asArgs(value: unknown): unknown[] {
  if (value === undefined || value === null) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw new D1RequestError("args must be an array");
  }
  return value;
}

function requireSQL(value: unknown, operation: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new D1RequestError(`${operation} requires a non-empty sql string`);
  }
  return value;
}

/**
 * d1.query runs one parameterised read and returns a column list plus row
 * arrays. Columns come from the first row so the Go client can scan by position;
 * an empty result carries no columns, which the client reports as "no rows".
 */
async function runD1Query(env: Env, payload: unknown): Promise<unknown> {
  const request = (payload ?? {}) as { sql?: unknown; args?: unknown };
  const sql = requireSQL(request.sql, "d1.query");
  const result = await env.DB.prepare(sql)
    .bind(...asArgs(request.args))
    .all();
  const rows = (result.results ?? []) as Record<string, unknown>[];
  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
  const values = rows.map((row) => columns.map((column) => (row[column] === undefined ? null : row[column])));
  return { columns, rows: values };
}

/**
 * d1.batch runs statements as one D1 transaction. D1 rolls the whole batch back
 * when any statement fails, which is the atomicity the canonical write and the
 * resumable reset both rely on. Only committed row counts are reported.
 */
async function runD1Batch(env: Env, payload: unknown): Promise<unknown> {
  const request = (payload ?? {}) as { statements?: unknown };
  if (!Array.isArray(request.statements) || request.statements.length === 0) {
    throw new D1RequestError("d1.batch requires a non-empty statements array");
  }
  const prepared = request.statements.map((raw) => {
    const statement = (raw ?? {}) as { sql?: unknown; args?: unknown };
    return env.DB.prepare(requireSQL(statement.sql, "d1.batch statement")).bind(...asArgs(statement.args));
  });
  const results = await env.DB.batch(prepared);
  return {
    changes: results.map((result) => {
      const changes = (result.meta as { changes?: unknown } | undefined)?.changes;
      return typeof changes === "number" ? changes : 0;
    }),
  };
}

async function dispatchD1(
  env: Env,
  request: BridgeRequest,
  run: (env: Env, payload: unknown) => Promise<unknown>,
): Promise<{ status: number; body: BridgeResponse }> {
  try {
    const result = await run(env, request.payload);
    return { status: 200, body: { version: ENVELOPE_VERSION, id: request.id, ok: true, result } };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (error instanceof D1RequestError) {
      return { status: 200, body: envelopeError(request.id, "d1_request_invalid", message, false) };
    }
    // A failing D1 operation is reported as retryable: reads are idempotent and
    // a batch is transactional, so a bounded retry cannot duplicate work.
    return { status: 200, body: envelopeError(request.id, "d1_execution_failed", message, true) };
  }
}

async function dispatch(env: Env, request: BridgeRequest): Promise<{ status: number; body: BridgeResponse }> {
  switch (request.operation) {
    case "bridge.ping":
      return {
        status: 200,
        body: {
          version: ENVELOPE_VERSION,
          id: request.id,
          ok: true,
          result: { pong: true },
        },
      };
    case "bridge.health":
      return {
        status: 200,
        body: {
          version: ENVELOPE_VERSION,
          id: request.id,
          ok: true,
          result: {
            d1: bindingState(env.DB),
            vectorize: bindingState(env.VECTORIZE),
            version: ENVELOPE_VERSION,
          },
        },
      };
    case "d1.query":
      return dispatchD1(env, request, runD1Query);
    case "d1.batch":
      return dispatchD1(env, request, runD1Batch);
    default:
      return {
        status: 200,
        body: envelopeError(
          request.id,
          "not_implemented",
          `operation ${JSON.stringify(request.operation)} is not implemented in this stage; canonical D1 query/batch are available, vector operations land in Stage 4, and operator jobs in Stage 5`,
          false,
        ),
      };
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.BRIDGE_TOKEN) {
      return jsonResponse(
        503,
        envelopeError("", "bridge_not_configured", "BRIDGE_TOKEN secret is not configured"),
      );
    }
    if (request.method !== "POST") {
      return jsonResponse(405, envelopeError("", "method_not_allowed", "bridge accepts POST envelopes only"));
    }
    const url = new URL(request.url);
    if (url.pathname !== "/" && url.pathname !== "/bridge") {
      return jsonResponse(404, envelopeError("", "route_not_found", "unknown bridge route"));
    }
    const authorization = request.headers.get("authorization") ?? "";
    const provided = authorization.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : "";
    if (!(await tokenMatches(provided, env.BRIDGE_TOKEN))) {
      return jsonResponse(401, envelopeError("", "unauthorized", "invalid bridge token"));
    }

    let raw: BridgeRequest;
    try {
      raw = (await request.json()) as BridgeRequest;
    } catch {
      return jsonResponse(400, envelopeError("", "malformed_envelope", "request body is not a JSON envelope"));
    }
    if (
      typeof raw !== "object" ||
      raw === null ||
      typeof raw.id !== "string" ||
      raw.id.length === 0 ||
      typeof raw.operation !== "string" ||
      raw.operation.length === 0
    ) {
      return jsonResponse(400, envelopeError("", "malformed_envelope", "envelope id and operation must be non-empty strings"));
    }
    if (raw.version !== ENVELOPE_VERSION) {
      return jsonResponse(
        400,
        envelopeError(raw.id, "version_mismatch", `envelope version ${raw.version} is not supported; this worker speaks version ${ENVELOPE_VERSION}`),
      );
    }

    const { status, body } = await dispatch(env, raw);
    return jsonResponse(status, body);
  },
};
