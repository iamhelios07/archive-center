import { describe, expect, it, vi } from "vitest";

// @cloudflare/containers imports the workerd-only `cloudflare:workers` module.
// Gateway authorization is platform-independent, so mock the forwarding helper
// here; real forwarding remains covered by Wrangler deployment validation.
vi.mock("@cloudflare/containers", () => ({
  Container: class {},
  getContainer: vi.fn(),
}));

import { authorizeGatewayRequest, gatewayConfigured, tokenMatches, type GatewayEnv } from "./index";

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
