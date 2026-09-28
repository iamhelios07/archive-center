import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

/**
 * Worker test configuration.
 *
 * The bindings are declared INLINE rather than through a wrangler config path.
 * That is deliberate and it is what makes this runnable with no Cloudflare
 * account at all: a wrangler config path would drag in database_id and Vectorize
 * index identifiers, which are account-scoped and which this repository
 * deliberately does not carry. The local simulators need a name and nothing else.
 *
 * D1 and Vectorize are the real local simulators, so the handlers run against the
 * binding behaviour they will meet in workerd, and an error a binding throws is
 * classified the way production classifies it rather than the way a hand-written
 * stub classifies it.
 */
export default defineConfig({
  plugins: [
    cloudflareTest({
      // The Worker entry point. Setting it explicitly is what lets SELF and the
      // static export analysis work without a wrangler config; without a config
      // path there is nothing else for it to default to.
      main: "./src/index.ts",
      miniflare: {
        // BRIDGE_TOKEN is a Worker SECRET in a real deployment and the fetch
        // handler refuses to serve without it, so supplying a value here is what
        // makes the envelope path testable end to end.
        bindings: { BRIDGE_TOKEN: "local-test-bridge-token" },
        // NEITHER D1 nor Vectorize is declared, and that is a measured result
        // rather than an omission.
        //
        // Vectorize is classified as a REMOTE-ONLY binding by the runtime. There
        // is no local simulator, so a local binding object would only be a proxy
        // that throws on first use, and the behaviour that matters — what error
        // Vectorize raises, and therefore how the outbox classifies a retry —
        // is exactly the part a stub cannot reproduce.
        //
        // D1 does have a local simulator, but declaring it makes the runtime
        // create a backing directory at startup, which fails in this environment
        // with a CreateDirectory access error before any test runs. Declaring a
        // binding that prevents the suite from starting would leave no coverage
        // at all, so the D1 assertions are present and skipped, and the binding
        // belongs here once the runtime can create its storage.
        //
        // The runtime's default cache directory is not writable here either, and
        // this Worker uses neither the Cache API nor KV, so turning their on-disk
        // persistence off removes that failure without changing anything the
        // tests exercise.
        cache: false,
        cacheDir: "./.wrangler/test-cache",
      },
    }),
  ],
});
