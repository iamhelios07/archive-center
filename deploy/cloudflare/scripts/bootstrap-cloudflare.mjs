#!/usr/bin/env node
// First-run Cloudflare bootstrap. This intentionally creates a new, isolated
// deployment every time it reaches the mutation phase. It never accepts existing
// resource names, never resets data, and never attempts cleanup after a failure.

import { randomBytes } from "node:crypto";
import { existsSync, readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline/promises";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const cloudflareRoot = resolve(here, "..");
const repositoryRoot = resolve(cloudflareRoot, "..");
const workerRoot = resolve(cloudflareRoot, "worker");
const gatewayRoot = resolve(cloudflareRoot, "gateway");
const migrationsRoot = resolve(cloudflareRoot, "migrations");
const bridgeConfig = resolve(cloudflareRoot, "wrangler.toml");
const gatewayConfig = resolve(gatewayRoot, "wrangler.toml");
const bridgeRenderer = resolve(here, "render-wrangler-config.mjs");
const gatewayRenderer = resolve(gatewayRoot, "scripts", "render-wrangler-config.mjs");

function fail(message) {
  process.stderr.write(`bootstrap-cloudflare: ${message}\n`);
  process.exit(1);
}

function notice(message) {
  process.stdout.write(`bootstrap-cloudflare: ${message}\n`);
}

function parseArgs(argv) {
  const options = { check: false, dimension: undefined };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--check") options.check = true;
    else if (arg === "--dimension") {
      const value = argv[++i];
      if (!value) fail("--dimension requires a positive integer");
      options.dimension = value;
    } else if (arg === "--help" || arg === "-h") {
      process.stdout.write("Usage: node deploy/cloudflare/scripts/bootstrap-cloudflare.mjs [--dimension N] [--check]\n");
      process.exit(0);
    } else fail(`unknown argument ${JSON.stringify(arg)}`);
  }
  return options;
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    env: options.env ?? process.env,
    input: options.input,
    encoding: "utf8",
    stdio: options.inherit ? "inherit" : "pipe",
    maxBuffer: 16 * 1024 * 1024,
  });
  if (result.error || result.status !== 0) {
    // Child output can contain account identifiers, image references, URLs, or
    // service responses. Do not relay it to a terminal, CI log, or report.
    throw new Error(options.label ?? "child command failed");
  }
  return `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
}

function runStep(label, action) {
  try {
    const value = action();
    notice(`${label}: pass`);
    return value;
  } catch {
    fail(`${label}: failed. No existing resource was reused, reset, or deleted; inspect the local command environment before retrying.`);
  }
}

function npmCommand() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

function ensureNode() {
  const [major] = process.versions.node.split(".").map(Number);
  if (!Number.isSafeInteger(major) || major < 22) fail("Node.js 22 or newer is required");
}

function ensureDependencies() {
  // A clone has no node_modules and receives the exact locked graph. Do not run
  // npm ci over a pre-existing graph: it deletes the directory first, which can
  // fail on a locked file and is needless for an operator who already installed
  // this exact checkout. Wrangler and the gateway package are still checked below.
  if (!existsSync(resolve(workerRoot, "node_modules", "wrangler", "bin", "wrangler.js"))) {
    run(npmCommand(), ["ci"], { cwd: workerRoot, label: "worker dependency install" });
  }
  if (!existsSync(resolve(gatewayRoot, "node_modules", "@cloudflare", "containers"))) {
    run(npmCommand(), ["ci"], { cwd: gatewayRoot, label: "gateway dependency install" });
  }
}

function wranglerBin() {
  const path = resolve(workerRoot, "node_modules", "wrangler", "bin", "wrangler.js");
  if (!existsSync(path)) throw new Error("Wrangler is not installed");
  return path;
}

function wrangler(args, options = {}) {
  return run(process.execPath, [wranglerBin(), ...args], {
    ...options,
    label: options.label ?? "wrangler command",
  });
}

function checkDocker() {
  run("docker", ["info"], { label: "Docker engine check" });
}

function parseJSON(output, label) {
  const first = output.indexOf("{");
  const last = output.lastIndexOf("}");
  if (first < 0 || last < first) throw new Error(`${label} returned no JSON`);
  try {
    return JSON.parse(output.slice(first, last + 1));
  } catch {
    throw new Error(`${label} returned invalid JSON`);
  }
}

function getAccountID() {
  const specified = (process.env.CLOUDFLARE_ACCOUNT_ID ?? "").trim();
  if (specified !== "") return specified;
  const payload = parseJSON(wrangler(["whoami", "--json"], { label: "Wrangler authentication check" }), "whoami");
  const accounts = Array.isArray(payload.accounts) ? payload.accounts : [];
  const ids = [...new Set(accounts.map((account) => String(account?.id ?? "").trim()).filter(Boolean))];
  if (ids.length !== 1) {
    throw new Error("set CLOUDFLARE_ACCOUNT_ID when the login has zero or multiple accounts");
  }
  return ids[0];
}

function accountArgs(accountID, args) {
  return [...args, "--account-id", accountID];
}

function generatedSuffix() {
  // UTC timestamp aids local operator correlation; 24 random bits make a
  // generated name collision impractical without consulting or reusing existing
  // state. 16 characters keeps `archive-center-<suffix>` within the D1 database
  // name limit of 32 and the Worker name limit of 58.
  const timestamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 10).toLowerCase();
  return `${timestamp}${randomBytes(3).toString("hex")}`;
}

function generatedNames() {
  const suffix = generatedSuffix();
  return {
    database: `archive-center-${suffix}`,
    vectorize: `archive-center-${suffix}-vectors`,
    bridge: `archive-center-${suffix}-bridge`,
    gateway: `archive-center-${suffix}-gateway`,
    image: `archive-center-${suffix}:bootstrap`,
  };
}

function findString(object, keys) {
  if (object && typeof object === "object") {
    for (const key of keys) {
      if (typeof object[key] === "string" && object[key].trim() !== "") return object[key].trim();
    }
    for (const value of Object.values(object)) {
      const nested = findString(value, keys);
      if (nested) return nested;
    }
  }
  return "";
}

function requireID(output, label) {
  const value = findString(parseJSON(output, label), ["uuid", "database_id", "id"]);
  if (value === "") throw new Error(`${label} returned no resource identifier`);
  return value;
}

function migrationFiles() {
  return readdirSync(migrationsRoot)
    .filter((name) => /^\d+_.+\.sql$/.test(name))
    .sort()
    .map((name) => resolve(migrationsRoot, name));
}

function parseWorkersDevURL(output) {
  const urls = output.match(/https:\/\/[a-z0-9-]+\.[a-z0-9-]+\.workers\.dev(?:\/[^\s"']*)?/gi) ?? [];
  if (urls.length !== 1) throw new Error("deployment returned no unambiguous workers.dev URL");
  return urls[0].replace(/\/$/, "");
}

function generatedEnvironment(values) {
  return { ...process.env, ...values };
}

function secretPut(config, accountID, name, value) {
  wrangler(accountArgs(accountID, ["secret", "put", name, "--config", config]), {
    input: `${value}\n`,
    label: `${name} secret injection`,
  });
}

async function verifyReadiness(gatewayURL, bearerToken) {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    try {
      const unauthorized = await fetch(`${gatewayURL}/ready`, { redirect: "error" });
      if (unauthorized.status !== 401) throw new Error("gateway did not reject unauthenticated readiness");
      const authorized = await fetch(`${gatewayURL}/ready`, {
        headers: { Authorization: `Bearer ${bearerToken}` },
        redirect: "error",
      });
      if (authorized.ok) {
        notice("authenticated readiness: pass");
        return;
      }
    } catch {
      // Container pull and secret propagation are asynchronous. Suppress raw
      // endpoint/service responses because they can include operator data.
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 2_000));
  }
  fail("authenticated readiness: failed. Credentials were not displayed; inspect the deployment through Cloudflare before retrying.");
}

async function requestedDimension(options) {
  const supplied = options.dimension ?? "";
  let value = supplied;
  if (value === "") {
    if (!process.stdin.isTTY) fail("--dimension is required when stdin is not interactive");
    const prompt = createInterface({ input: process.stdin, output: process.stdout });
    value = await prompt.question("Embedding dimension (from your actual embedder): ");
    prompt.close();
  }
  if (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 1536) {
    fail("--dimension must be an integer from 1 through 1536; do not guess a model-independent default");
  }
  return value;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  ensureNode();
  runStep("Docker engine", checkDocker);
  runStep("dependencies", ensureDependencies);
  runStep("Wrangler CLI", () => wrangler(["--version"], { label: "Wrangler version check" }));
  runStep("bridge renderer", () => run(process.execPath, [bridgeRenderer, "--check"], { label: "bridge renderer check" }));
  runStep("gateway renderer", () => run(process.execPath, [gatewayRenderer, "--check"], { label: "gateway renderer check" }));
  if (options.check) {
    notice("check complete; no remote Cloudflare resource was changed");
    return;
  }

  const dimension = await requestedDimension(options);
  // Login must remain interactive only when no authenticated session exists.
  let accountID;
  try {
    accountID = getAccountID();
    notice("Wrangler authentication: pass");
  } catch {
    notice("Wrangler authentication: login required");
    run(process.execPath, [wranglerBin(), "login"], { inherit: true, label: "Wrangler login" });
    accountID = runStep("Wrangler authentication", getAccountID);
  }

  const names = generatedNames();
  const d1Create = runStep("dedicated D1 creation", () =>
    wrangler(accountArgs(accountID, ["d1", "create", names.database, "--json"]), { label: "D1 creation" }),
  );
  const databaseID = requireID(d1Create, "D1 creation");
  for (const migration of migrationFiles()) {
    runStep(`migration ${migration.split(/[\\/]/).pop()}`, () =>
      // --yes keeps the migration step non-interactive: the spawned process has
      // no TTY, and a confirmation prompt would abort it mid-bootstrap.
      wrangler(accountArgs(accountID, ["d1", "execute", names.database, "--remote", "--yes", "--file", migration]), {
        label: "D1 migration",
      }),
    );
  }
  runStep("dedicated Vectorize creation", () =>
    wrangler(accountArgs(accountID, ["vectorize", "create", names.vectorize, "--dimensions", dimension, "--metric", "cosine", "--json"]), {
      label: "Vectorize creation",
    }),
  );
  for (const property of ["chat_session_id", "tier", "source_table"]) {
    runStep(`Vectorize metadata index ${property}`, () =>
      wrangler(accountArgs(accountID, ["vectorize", "create-metadata-index", names.vectorize, "--property-name", property, "--type", "string"]), {
        label: "Vectorize metadata index creation",
      }),
    );
  }

  runStep("root-context Container build", () =>
    run("docker", ["build", "--platform", "linux/amd64", "--tag", names.image, "--file", resolve(cloudflareRoot, "container", "Dockerfile"), "."], {
      cwd: repositoryRoot,
      label: "Container image build",
    }),
  );
  const pushedImage = runStep("Cloudflare Registry push", () => {
    const output = wrangler(accountArgs(accountID, ["containers", "push", names.image]), { label: "Container image push" });
    const match = /Pushed image:\s*(\S+)/.exec(output);
    if (!match) throw new Error("Container image push returned no image URI");
    return match[1];
  });

  const bridgeEnv = generatedEnvironment({
    AC_CLOUDFLARE_BRIDGE_WORKER_NAME: names.bridge,
    AC_CLOUDFLARE_D1_DATABASE_NAME: names.database,
    AC_CLOUDFLARE_D1_DATABASE_ID: databaseID,
    AC_CLOUDFLARE_VECTORIZE_INDEX_NAME: names.vectorize,
  });
  runStep("bridge config render", () => run(process.execPath, [bridgeRenderer, "--out", bridgeConfig], { env: bridgeEnv, label: "bridge config render" }));
  const bridgeDeploy = runStep("private bridge deploy", () =>
    wrangler(accountArgs(accountID, ["deploy", "--config", bridgeConfig]), { label: "bridge deployment" }),
  );
  const bridgeURL = parseWorkersDevURL(bridgeDeploy);

  const bridgeToken = randomBytes(32).toString("base64url");
  const bearerToken = randomBytes(32).toString("base64url");
  runStep("bridge secret injection", () => secretPut(bridgeConfig, accountID, "BRIDGE_TOKEN", bridgeToken));
  const gatewayEnv = generatedEnvironment({
    AC_CLOUDFLARE_GATEWAY_WORKER_NAME: names.gateway,
    AC_CLOUDFLARE_BRIDGE_URL: bridgeURL,
    AC_CLOUDFLARE_CONTAINER_IMAGE: pushedImage,
  });
  runStep("gateway config render", () => run(process.execPath, [gatewayRenderer, "--out", gatewayConfig], { env: gatewayEnv, label: "gateway config render" }));
  const gatewayDeploy = runStep("public gateway deploy", () =>
    wrangler(accountArgs(accountID, ["deploy", "--config", gatewayConfig]), { label: "gateway deployment" }),
  );
  const gatewayURL = parseWorkersDevURL(gatewayDeploy);
  runStep("gateway bridge secret injection", () => secretPut(gatewayConfig, accountID, "BRIDGE_TOKEN", bridgeToken));
  runStep("gateway bearer secret injection", () => secretPut(gatewayConfig, accountID, "AC_BEARER_TOKEN", bearerToken));
  await verifyReadiness(gatewayURL, bearerToken);

  // This is the sole credential display. Nothing before this point writes either
  // value, so an incomplete deployment never leaves an operator with a token for
  // a service that has not passed its authenticated readiness boundary.
  process.stdout.write("bootstrap-cloudflare: deployment ready. Enter these once in RisuAI:\n");
  process.stdout.write(`  endpoint: ${gatewayURL}\n`);
  process.stdout.write(`  bearer token: ${bearerToken}\n`);
}

await main();
