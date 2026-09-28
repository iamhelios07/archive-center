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

        // NEITHER D1 nor Vectorize is declared, and both omissions are measured
        // rather than assumed.
        //
        // VECTORIZE has no local simulator at all. The runtime classifies it as a
        // REMOTE-ONLY binding (miniflare: vectorize: "remote"), and touching the
        // local binding throws "Binding VECTORIZE needs to be run remotely". So
        // the behaviour that actually matters — what error Vectorize raises, and
        // therefore how the outbox classifies a mutation as retryable or terminal
        // — is owned by the remote harness and cannot be reached from here. A
        // local proxy would only throw on first use and teach us nothing.
        //
        // D1 does have a local simulator, but it is a DISK-BACKED DURABLE OBJECT:
        // workerd creates one directory per such class at startup. On this host
        // that call fails with ERROR_ACCESS_DENIED (Windows error #5) for the
        // directory it names "miniflare-D1DatabaseObject", and the runtime exits
        // before a single test runs. Declaring the binding therefore costs the
        // whole suite its coverage instead of gaining D1 coverage.
        //
        // What was ruled out, so the search is not repeated:
        //   - workerd itself works here. With no disk-backed binding the pool
        //     starts and the Worker serves, so the runtime binary is fine.
        //   - It is not the configuration form: the documented ephemeral array
        //     form and a record form both reproduce it.
        //   - It is not a filesystem policy or a blocked name: the identical
        //     directory name is created without error in the project directory
        //     and in %TEMP%, as is an arbitrary one.
        //   - It is not the working directory: workerd is spawned with no cwd and
        //     inherits, and pre-creating the directory where the project would
        //     place it changes nothing.
        //   - It is not this cacheDir: an absolute, pre-created one behaves the
        //     same.
        // The D1 assertions are kept in the test file and skipped, carrying the
        // same note. A deleted test is indistinguishable from one that was never
        // needed.
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
