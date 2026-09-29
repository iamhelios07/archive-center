import { describe, expect, it, vi } from "vitest";

// @cloudflare/containers imports the workerd-only `cloudflare:workers` module.
// Gateway authorization is platform-independent, so mock the forwarding helper
// here; real forwarding remains covered by Wrangler deployment validation.
vi.mock("@cloudflare/containers", () => ({
  Container: class {},
  getContainer: vi.fn(),
}));

import {
  authorizeGatewayRequest,
  corsPreflightResponse,
  gatewayConfigured,
  handleGatewayRequest,
  tokenMatches,
  withCorsHeaders,
  type GatewayEnv,
} from "./index";

const configuredEnv = {
  ARCHIVE_CENTER: {} as DurableObjectNamespace,
  BRIDGE_URL: "https://bridge.invalid",
  BRIDGE_TOKEN: "bridge-secret",
  AC_BEARER_TOKEN: "client-secret",
} as unknown as GatewayEnv;

describe("gateway authorization", () => {
  it("requires every Container runtime setting before waking a Container", async () => {
    const response = await authorizeGatewayRequest(
      new Request("https://gateway.invalid/ready"),
      { ...configuredEnv, BRIDGE_TOKEN: "" },
    );

    expect(gatewayConfigured({ ...configuredEnv, BRIDGE_TOKEN: "" })).toBe(false);
    expect(response?.status).toBe(503);
  });

  it("rejects missing, malformed, and incorrect bearer credentials", async () => {
    const missing = await authorizeGatewayRequest(new Request("https://gateway.invalid/ready"), configuredEnv);
    const malformed = await authorizeGatewayRequest(
      new Request("https://gateway.invalid/ready", { headers: { Authorization: "Basic client-secret" } }),
      configuredEnv,
    );
    const incorrect = await authorizeGatewayRequest(
      new Request("https://gateway.invalid/ready", { headers: { Authorization: "Bearer wrong-secret" } }),
      configuredEnv,
    );

    expect(missing?.status).toBe(401);
    expect(malformed?.status).toBe(401);
    expect(incorrect?.status).toBe(401);
  });

  it("accepts only an exact bearer credential", async () => {
    const accepted = await authorizeGatewayRequest(
      new Request("https://gateway.invalid/ready", { headers: { Authorization: "Bearer client-secret" } }),
      configuredEnv,
    );

    expect(await tokenMatches("client-secret", "client-secret")).toBe(true);
    expect(await tokenMatches("client-secret", "client-secret-suffix")).toBe(false);
    expect(accepted).toBeNull();
  });
});

describe("gateway CORS", () => {
  it("answers a preflight without requiring the credential the preflight cannot carry", async () => {
    const preflight = new Request("https://gateway.invalid/sessions", {
      method: "OPTIONS",
      headers: {
        Origin: "https://risu.example",
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "authorization,content-type",
      },
    });
    const forward = vi.fn();

    const response = await handleGatewayRequest(preflight, configuredEnv, forward);

    expect(response.status).toBe(204);
    expect(response.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(response.headers.get("Access-Control-Allow-Headers")).toContain("Authorization");
    expect(response.headers.get("Access-Control-Allow-Methods")).toContain("POST");
    expect(forward).not.toHaveBeenCalled();
  });

  it("makes a bearer rejection readable by a browser caller", async () => {
    const unauthorized = new Request("https://gateway.invalid/ready", {
      headers: { Origin: "https://risu.example" },
    });

    const response = await handleGatewayRequest(unauthorized, configuredEnv, vi.fn());

    expect(response.status).toBe(401);
    expect(response.headers.get("Access-Control-Allow-Origin")).toBe("*");
  });

  it("keeps the Container's own origin header instead of duplicating it", async () => {
    const containerResponse = new Response("{}", {
      status: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });

    const response = withCorsHeaders(containerResponse);

    expect(response.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(response.headers.get("Vary")).toBe("Origin");
    expect(response.status).toBe(200);
  });

  it("answers a preflight even when the Worker is not fully configured", () => {
    // Configuration errors must stay diagnosable from a browser; a preflight that
    // fails on a missing secret is indistinguishable from a broken deployment.
    const preflight = corsPreflightResponse();

    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("Access-Control-Allow-Origin")).toBe("*");
  });
});
