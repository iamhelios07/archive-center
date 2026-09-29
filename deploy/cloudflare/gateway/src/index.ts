import { Container, getContainer } from "@cloudflare/containers";

/**
 * Bindings intentionally separate public ingress from the private D1/Vectorize
 * bridge. BRIDGE_URL is a generated non-secret var; both tokens are Worker
 * secrets and are only copied into the Container's runtime environment.
 */
export interface GatewayEnv {
  ARCHIVE_CENTER: DurableObjectNamespace<ArchiveCenterContainer>;
  BRIDGE_URL: string;
  BRIDGE_TOKEN: string;
  AC_BEARER_TOKEN: string;
}

const CONTAINER_INSTANCE = "archive-center";
const JSON_HEADERS = { "Content-Type": "application/json; charset=utf-8" };

function configured(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
}

/** Timing-safe bearer comparison: hash both values before comparing them. */
export async function tokenMatches(provided: string, expected: string): Promise<boolean> {
  if (!provided || !expected) {
    return false;
  }

  const encoder = new TextEncoder();
  const [providedHash, expectedHash] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(provided)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const left = new Uint8Array(providedHash);
  const right = new Uint8Array(expectedHash);
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left[index] ^ right[index];
  }
  return difference === 0;
}

export function gatewayConfigured(env: GatewayEnv): boolean {
  return configured(env.BRIDGE_URL) && configured(env.BRIDGE_TOKEN) && configured(env.AC_BEARER_TOKEN);
}

/**
 * Return an error response before a Container is woken, or null for an
 * authenticated request. The Go service repeats bearer enforcement in depth.
 */
export async function authorizeGatewayRequest(request: Request, env: GatewayEnv): Promise<Response | null> {
  if (!gatewayConfigured(env)) {
    return new Response(JSON.stringify({ error: "gateway is not configured" }), {
      status: 503,
      headers: JSON_HEADERS,
    });
  }

  const authorization = request.headers.get("Authorization");
  const match = authorization?.match(/^Bearer ([^\s]+)$/);
  if (!match || !(await tokenMatches(match[1], env.AC_BEARER_TOKEN))) {
    return new Response(JSON.stringify({ error: "unauthorized" }), {
      status: 401,
      headers: JSON_HEADERS,
    });
  }

  return null;
}

/**
 * A singleton Go backend process. Product state is never written to this
 * Durable Object: D1 remains the canonical store and the Container is replaceable.
 */
export class ArchiveCenterContainer extends Container<GatewayEnv> {
  defaultPort = 28080;
  sleepAfter = "10m";
  pingEndpoint = "localhost/ready";
  enableInternet = true;

  envVars = {
    AC_RUNTIME_PROFILE: "cloudflare",
    AC_STORE_MODE: "cloudflare_authority",
    AC_VECTOR_MODE: "cloudflare",
    AC_CLOUDFLARE_BRIDGE_URL: this.env.BRIDGE_URL,
    AC_CLOUDFLARE_BRIDGE_TOKEN: this.env.BRIDGE_TOKEN,
    AC_ENFORCE_AUTH: "true",
    AC_BEARER_TOKEN: this.env.AC_BEARER_TOKEN,
    AC_BIND_ADDR: "0.0.0.0:28080",
  };
}

export default {
  async fetch(request: Request, env: GatewayEnv): Promise<Response> {
    const rejected = await authorizeGatewayRequest(request, env);
    if (rejected) {
      return rejected;
    }

    return getContainer(env.ARCHIVE_CENTER, CONTAINER_INSTANCE).fetch(request);
  },
};
