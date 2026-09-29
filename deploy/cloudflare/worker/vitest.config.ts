import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

/**
 * Worker test configuration.
 *
 * Bindings are declared INLINE rather than through a wrangler config path. That
 * is what makes this runnable with no Cloudflare account at all: a wrangler
 * config path would drag in database_id and Vectorize index identifiers, which
 * are account-scoped and which this repository deliberately does not carry.
 */
export default defineConfig({
  plugins: [
    cloudflareTest({
      // The Worker entry point. Setting it explicitly is what lets SELF and the
      // static export analysis work without a wrangler config; with no config
      // path there is nothing else for it to default to.
      main: "./src/index.ts",
      miniflare: {
        // BRIDGE_TOKEN is a Worker SECRET in a real deployment and the fetch
        // handler refuses to serve without it, so supplying a value here is what
        // makes the envelope path testable end to end.
        bindings: { BRIDGE_TOKEN: "local-test-bridge-token" },

        // D1 has a real local simulator, so the canonical query and batch
        // handlers are exercised against the engine they meet in workerd. It is
        // a DISK-BACKED DURABLE OBJECT, which is why `npm test` runs through
        // scripts/run-worker-tests.mjs: that script hands the runtime a
        // Low-integrity scratch directory, without which Windows Mandatory
        // Integrity Control denies the write and the binding cannot start.
        d1Databases: ["DB"],

        // Vectorize is genuinely remote-only. The runtime classifies it as
        // remote ("vectorize: \"remote\"") and touching the local binding throws
        // "Binding VECTORIZE needs to be run remotely". There is no local
        // simulator, and no label, path or flag that would create one. So the
        // behaviour that actually matters — what error Vectorize raises, and
        // therefore how the outbox classifies a mutation as retryable or
        // terminal — is owned by the remote harness. A local proxy would only
        // throw on first use and teach us nothing.

        // The runtime's default cache directory is not writable here, and this
        // Worker uses neither the Cache API nor KV, so turning their on-disk
        // persistence off removes that failure without changing anything the
        // tests exercise.
        cache: false,
        cacheDir: "./.wrangler/test-cache",
      },
    }),
  ],
});
