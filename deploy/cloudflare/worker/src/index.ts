// Archive Center Cloudflare Worker — Container-to-Cloudflare bridge.
//
// Stage 4 contract: this Worker receives versioned envelope requests from the
// Archive Center Go backend running in a Cloudflare Container and dispatches
// them to the D1 and Vectorize bindings. bridge.ping and bridge.health report
// the binding state, d1.query and d1.batch reach canonical D1, and
// vector.upsert, vector.delete, vector.query, and vector.health reach
// Vectorize. Every operation outside that list answers with a typed
// "not_implemented" envelope error.
//
// Account neutrality: no account ID, database ID, index ID, route, or domain
// appears here. Vectorize is reached only through the VECTORIZE binding, never
// through a REST URL and never with an API token. Bindings are referenced by
// stable names only (DB, VECTORIZE) and the shared bridge secret is injected
// as a Worker secret (BRIDGE_TOKEN).

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

// ---------------------------------------------------------------------------
// Vectorize handlers
// ---------------------------------------------------------------------------

/** A malformed vector payload, as opposed to a failing Vectorize operation. */
class VectorRequestError extends Error {}

/** The narrow Vectorize surface these handlers use, so a stub can replace it. */
export interface VectorizePort {
  upsert(vectors: VectorizeVector[]): Promise<unknown>;
  deleteByIds(ids: string[]): Promise<unknown>;
  query(vector: number[], options?: VectorizeQueryOptions): Promise<VectorizeMatches>;
  getByIds(ids: string[]): Promise<VectorizeVector[]>;
  describe(): Promise<VectorizeIndexInfo>;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireNumberArray(value: unknown, operation: string): number[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new VectorRequestError(`${operation} requires a non-empty array of numbers`);
  }
  return value.map((item) => {
    if (typeof item !== "number" || !Number.isFinite(item)) {
      throw new VectorRequestError(`${operation} accepts finite numbers only`);
    }
    return item;
  });
}

function requirePositiveInteger(value: unknown, operation: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new VectorRequestError(`${operation} requires a positive integer topK`);
  }
  return value;
}

function requireBoolean(value: unknown, operation: string): boolean {
  if (typeof value !== "boolean") {
    throw new VectorRequestError(`${operation} requires a boolean`);
  }
  return value;
}

/** Scalar metadata leaf values. A record of these is the deepest Vectorize stores. */
function requireMetadataValue(value: unknown, operation: string): VectorizeVectorMetadataValue {
  if (typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new VectorRequestError(`${operation} metadata numbers must be finite`);
    }
    return value;
  }
  if (Array.isArray(value)) {
    if (!value.every((item) => typeof item === "string")) {
      throw new VectorRequestError(`${operation} metadata arrays may only hold strings`);
    }
    return value;
  }
  throw new VectorRequestError(`${operation} metadata values must be string, number, boolean, or string[]`);
}

/** One metadata field value: a leaf, or a record of leaves. Nothing deeper. */
function requireMetadataEntry(value: unknown, operation: string): VectorizeVectorMetadata {
  if (isPlainObject(value)) {
    return requireMetadataObject(value, operation);
  }
  return requireMetadataValue(value, operation);
}

function requireMetadataObject(value: Record<string, unknown>, operation: string): Record<string, VectorizeVectorMetadataValue> {
  const metadata: Record<string, VectorizeVectorMetadataValue> = {};
  for (const [key, raw] of Object.entries(value)) {
    if (key.trim() === "") {
      throw new VectorRequestError(`${operation} metadata keys must be non-empty`);
    }
    metadata[key] = requireMetadataValue(raw, operation);
  }
  return metadata;
}

function requireMetadata(value: unknown, operation: string): Record<string, VectorizeVectorMetadata> {
  if (!isPlainObject(value)) {
    throw new VectorRequestError(`${operation} metadata must be an object`);
  }
  const metadata: Record<string, VectorizeVectorMetadata> = {};
  for (const [key, raw] of Object.entries(value)) {
    if (key.trim() === "") {
      throw new VectorRequestError(`${operation} metadata keys must be non-empty`);
    }
    metadata[key] = requireMetadataEntry(raw, operation);
  }
  return metadata;
}

/**
 * The index dimension is fixed when the index is created, so a batch that mixes
 * lengths can never be stored. Rejecting it here reports a permanently bad
 * payload instead of a retryable Vectorize failure.
 */
function requireUniformDimensions(vectors: { id: string; values: number[] }[]): void {
  const expected = vectors[0].values.length;
  for (const vector of vectors) {
    if (vector.values.length !== expected) {
      throw new VectorRequestError(
        `vector.upsert mixes vector dimensions: id ${JSON.stringify(vector.id)} has ${vector.values.length} values, expected ${expected}`,
      );
    }
  }
}

/**
 * vector.upsert stores vectors by document id. Vectorize replaces a stored
 * vector with the same id, so the operation is idempotent and a bounded retry
 * cannot duplicate work. The binding accepts the whole batch as one async
 * mutation and reports only a mutation id, so the accepted count is the
 * validated batch size.
 */
export async function runVectorUpsert(port: VectorizePort, payload: unknown): Promise<unknown> {
  const request = (payload ?? {}) as { vectors?: unknown };
  if (!Array.isArray(request.vectors) || request.vectors.length === 0) {
    throw new VectorRequestError("vector.upsert requires a non-empty vectors array");
  }
  const batch = request.vectors.map((raw) => {
    const entry = (raw ?? {}) as { id?: unknown; values?: unknown; metadata?: unknown };
    if (typeof entry.id !== "string" || entry.id.trim() === "") {
      throw new VectorRequestError("vector.upsert requires every vector to carry a non-empty id");
    }
    const vector: { id: string; values: number[]; metadata?: Record<string, VectorizeVectorMetadata> } = {
      id: entry.id,
      values: requireNumberArray(entry.values, "vector.upsert values"),
    };
    if (entry.metadata !== undefined && entry.metadata !== null) {
      vector.metadata = requireMetadata(entry.metadata, "vector.upsert");
    }
    return vector;
  });
  requireUniformDimensions(batch);
  await port.upsert(batch);
  return { upserted: batch.length };
}

/**
 * vector.delete removes vectors by document id. Deleting an id that is already
 * gone is not a failure: the outbox replays a delete after a partial failure,
 * and a delete that reported "not found" would poison that retry forever. An
 * empty id list is a no-op and never reaches the binding.
 */
export async function runVectorDelete(port: VectorizePort, payload: unknown): Promise<unknown> {
  const request = (payload ?? {}) as { ids?: unknown };
  if (!Array.isArray(request.ids)) {
    throw new VectorRequestError("vector.delete requires an ids array");
  }
  const ids = request.ids.map((raw) => {
    if (typeof raw !== "string" || raw.trim() === "") {
      throw new VectorRequestError("vector.delete requires every id to be a non-empty string");
    }
    return raw;
  });
  if (ids.length === 0) {
    return { deleted: 0 };
  }
  await port.deleteByIds(ids);
  return { deleted: ids.length };
}

/**
 * Builds the Vectorize metadata filter. The binding filter is a flat object in
 * which every field is an exact match and every field is ANDed with the others,
 * so the Go side exact-match map maps onto it field for field. An absent filter
 * and an empty filter both mean no filter: the Go side never sends an empty
 * object, and sending one to the binding would assert a match nothing has.
 */
function buildQueryFilter(value: unknown): VectorizeVectorMetadataFilter | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isPlainObject(value)) {
    throw new VectorRequestError("vector.query filter must be an object of exact-match metadata fields");
  }
  const filter: VectorizeVectorMetadataFilter = {};
  for (const [field, expected] of Object.entries(value)) {
    if (field.trim() === "") {
      throw new VectorRequestError("vector.query filter field names must be non-empty");
    }
    if (typeof expected === "string" || typeof expected === "boolean") {
      filter[field] = expected;
    } else if (typeof expected === "number" && Number.isFinite(expected)) {
      filter[field] = expected;
    } else {
      throw new VectorRequestError(`vector.query filter field ${JSON.stringify(field)} must be a string, number, or boolean`);
    }
  }
  return Object.keys(filter).length === 0 ? undefined : filter;
}

/** Projects one Vectorize match onto the bridge result shape. */
function toQueryMatch(
  match: VectorizeMatch,
  includeValues: boolean,
): { id: string; score: number; values?: number[]; metadata: Record<string, unknown> } {
  const projected: { id: string; score: number; values?: number[]; metadata: Record<string, unknown> } = {
    id: match.id,
    // The score is the raw Vectorize cosine score over the same stored vectors
    // the local Chroma path recomputes cosine over, so the Go side sets
    // distance to 1 - score and keeps the cosine threshold of 0.30.
    score: match.score,
    metadata: match.metadata ?? {},
  };
  if (includeValues && match.values) {
    projected.values = Array.from(match.values);
  }
  return projected;
}

/**
 * vector.query runs one similarity search and reports the matches in the order
 * Vectorize ranked them. A search that matches nothing is a successful empty
 * result, not a failure: Vectorize is eventually consistent, a vector that was
 * just upserted may be invisible for a short window, and the Go provider owns
 * that retry and fallback policy. This side never turns a pending mutation into
 * a reported success and never turns an empty index into an error.
 */
export async function runVectorQuery(port: VectorizePort, payload: unknown): Promise<unknown> {
  const request = (payload ?? {}) as {
    vector?: unknown;
    topK?: unknown;
    filter?: unknown;
    includeValues?: unknown;
  };
  const vector = requireNumberArray(request.vector, "vector.query");
  const topK = requirePositiveInteger(request.topK, "vector.query");
  const includeValues =
    request.includeValues === undefined || request.includeValues === null
      ? false
      : requireBoolean(request.includeValues, "vector.query includeValues");
  // Full metadata is required by the established Go result contract. Vectorize
  // limits that payload shape to 50 results; callers that need a wider,
  // deliberately lossy diagnostic must ask for a separate shallow protocol.
  if (topK > 50) {
    throw new VectorRequestError("vector.query full metadata is limited to topK <= 50");
  }
  const options: VectorizeQueryOptions = {
    topK,
    // Full metadata, not the indexed level: the Go side builds tier, source
    // table, and document text out of it, and the indexed level truncates long
    // strings. Stored values are the expensive half of the payload, so they are
    // returned only when the caller asks for them.
    returnMetadata: "all",
    returnValues: includeValues,
  };
  const filter = buildQueryFilter(request.filter);
  if (filter) {
    options.filter = filter;
  }
  const result = await port.query(vector, options);
  const matches = result?.matches ?? [];
  return { matches: matches.map((match) => toQueryMatch(match, includeValues)) };
}

/**
 * vector.get reads exact documents by id.
 *
 * Vectorize's getByIds returns the raw unscored vectors, which is what the Go
 * side needs and what a filtered query cannot give it. A filtered query is
 * APPROXIMATE: the index may drop a matching candidate from an ANN walk, so
 * using one to answer "does this document exist" would let an exact read report
 * a document as absent while the index holds it. The outbox verifies a mutation
 * by reading the document back, and startup recovery reconciles a snapshot
 * against the index, so a false "absent" in either is a duplicate write or a
 * needless re-upsert. Ids that do not exist are simply absent from the result
 * rather than an error, so a caller can retry the whole batch.
 */
export async function runVectorGet(port: VectorizePort, payload: unknown): Promise<unknown> {
  const request = (payload ?? {}) as { ids?: unknown };
  if (!Array.isArray(request.ids)) {
    throw new VectorRequestError("vector.get requires an ids array");
  }
  if (request.ids.some((id) => typeof id !== "string" || id.length === 0)) {
    throw new VectorRequestError("vector.get accepts non-empty string ids only");
  }
  if (request.ids.length === 0) {
    return { vectors: [] };
  }
  const found = await port.getByIds(request.ids as string[]);
  return { vectors: (found ?? []).map(projectVector) };
}

/** One stored vector, in the shape the Go side decodes. */
function projectVector(vector: VectorizeVector): Record<string, unknown> {
  return {
    id: vector.id,
    values: (vector.values ?? []) as number[],
    metadata: (vector.metadata ?? {}) as Record<string, unknown>,
  };
}

/**
 * vector.health reports the binding state and the index geometry.
 *
 * An absent binding is reported as bound=false rather than as an error so the
 * health report renders as unhealthy instead of failing to render.
 *
 * The geometry is here because two of the Go provider's operations are otherwise
 * impossible on a cold process, and neither can work around it:
 *
 *  - The index dimension is fixed at creation and Vectorize rejects a query
 *    whose vector length differs from it. An exact or filtered READ therefore
 *    needs a correctly sized vector, and the only vector the Go side can have on
 *    a cold process is none. Without the dimension the first read of a freshly
 *    started Container fails, and Cloudflare Containers scale to zero, so that
 *    is the common case rather than an edge one.
 *  - A count for the whole index cannot be derived from a filtered query, and
 *    paging a filtered query to exhaustion to approximate one would be O(n) and
 *    would still be an approximation.
 *
 * No index identity is reported. The binding exposes no account-neutral name or
 * version of its own, and inventing one would mean inventing an index identity
 * that does not exist.
 */
export async function runVectorHealth(port: VectorizePort | undefined): Promise<unknown> {
  if (port === undefined || port === null) {
    return { bound: false };
  }
  // describe() is the only account-neutral index read the binding offers, and it
  // is what makes a cold read possible at all. A failure here must not take the
  // whole health report down: a bound-but-unreadable index is still worth
  // reporting, just without geometry.
  try {
    const info = await port.describe();
    return {
      bound: true,
      version: String(ENVELOPE_VERSION),
      dimensions: info.dimensions,
      vectorCount: info.vectorCount,
      processedUpToMutation: info.processedUpToMutation,
    };
  } catch {
    return { bound: true, version: String(ENVELOPE_VERSION), geometryError: true };
  }
}

/**
 * A wrong-length vector can never be stored: the index dimension is fixed at
 * creation, so retrying it would spin forever. The binding surfaces the
 * rejection as a bare Error message rather than a typed code, so the condition
 * is matched on text and reported as a non-retryable request error.
 */
function isDimensionMismatch(message: string): boolean {
  return message.toLowerCase().includes("dimension");
}

async function dispatchVector(
  env: Env,
  request: BridgeRequest,
  run: (port: VectorizePort, payload: unknown) => Promise<unknown>,
): Promise<{ status: number; body: BridgeResponse }> {
  if (bindingState(env.VECTORIZE) !== "bound") {
    return {
      status: 200,
      body: envelopeError(request.id, "vector_binding_missing", "VECTORIZE binding is not configured on this Worker", false),
    };
  }
  try {
    const result = await run(env.VECTORIZE, request.payload);
    return { status: 200, body: { version: ENVELOPE_VERSION, id: request.id, ok: true, result } };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (error instanceof VectorRequestError) {
      return { status: 200, body: envelopeError(request.id, "vector_request_invalid", message, false) };
    }
    if (isDimensionMismatch(message)) {
      return {
        status: 200,
        body: envelopeError(request.id, "vector_request_invalid", `permanent dimension mismatch: ${message}`, false),
      };
    }
    // A failing Vectorize operation is reported as retryable: an upsert
    // replaces by document id and a delete of an absent id is a no-op, so a
    // bounded retry cannot duplicate work or lose a mutation.
    return { status: 200, body: envelopeError(request.id, "vector_execution_failed", message, true) };
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
    case "vector.upsert":
      return dispatchVector(env, request, runVectorUpsert);
    case "vector.delete":
      return dispatchVector(env, request, runVectorDelete);
    case "vector.query":
      return dispatchVector(env, request, runVectorQuery);
    case "vector.get":
      return dispatchVector(env, request, runVectorGet);
    case "vector.health":
      // The one Vectorize operation that must not fail: an absent binding is a
      // health report, not an error.
      return {
        status: 200,
        body: {
          version: ENVELOPE_VERSION,
          id: request.id,
          ok: true,
          result: await runVectorHealth(env.VECTORIZE),
        },
      };
    default:
      return {
        status: 200,
        body: envelopeError(
          request.id,
          "not_implemented",
          `operation ${JSON.stringify(request.operation)} is not implemented in this stage; canonical D1 query/batch and vector upsert/delete/query/get/health are available, and operator jobs land in Stage 5`,
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
