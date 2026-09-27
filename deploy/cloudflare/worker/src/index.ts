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
    default:
      return {
        status: 200,
        body: envelopeError(
          request.id,
          "not_implemented",
          `operation ${JSON.stringify(request.operation)} is not implemented in this stage; canonical D1 operations land in Stage 3, vector operations in Stage 4, and operator jobs in Stage 5`,
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
