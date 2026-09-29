#!/usr/bin/env node
// Renders the private bridge Worker config. Account-scoped values live only in
// the ignored output; diagnostic output names missing variables, never values.

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const deployRoot = resolve(here, "..");
const templatePath = resolve(deployRoot, "wrangler.template.toml");
const defaultOut = resolve(deployRoot, "wrangler.toml");

const REQUIRED = [
  {
    variable: "AC_CLOUDFLARE_D1_DATABASE_ID",
    key: "database_id",
    marker: "# database_id is account-specific and must be rendered at deploy time.",
    describe: "D1 database id",
    // The template has no database_id assignment; insert above the marker.
    insertBefore: true,
  },
  {
    // Vectorize Worker bindings identify an index by its API name. The old ID
    // variable is retained only for already-provisioned operators; a bootstrap
    // always supplies the name returned by `vectorize create`.
    variable: "AC_CLOUDFLARE_VECTORIZE_INDEX_NAME",
    legacyVariable: "AC_CLOUDFLARE_VECTORIZE_INDEX_ID",
    key: "index_name",
    marker: 'index_name = "archive-center-vectors"',
    describe: "Vectorize index name",
    // The template already assigns index_name; replace it or the rendered
    // document would redefine the key and TOML parsing would fail.
    insertBefore: false,
  },
];

const OPTIONAL = [
  {
    variable: "AC_CLOUDFLARE_BRIDGE_WORKER_NAME",
    key: "name",
    marker: 'name = "archive-center-bridge"',
  },
  {
    variable: "AC_CLOUDFLARE_D1_DATABASE_NAME",
    key: "database_name",
    marker: 'database_name = "archive-center-db"',
  },
];

function fail(message) {
  process.stderr.write(`render-wrangler-config: ${message}\n`);
  process.exit(1);
}

function parseArgs(argv) {
  const options = { check: false, out: defaultOut };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--check") options.check = true;
    else if (argv[i] === "--out") {
      const next = argv[++i];
      if (!next) fail("--out requires a path");
      options.out = resolve(process.cwd(), next);
    } else fail(`unknown argument ${JSON.stringify(argv[i])}`);
  }
  return options;
}

function envValue(field, allowCheckValue) {
  const names = [field.variable, field.legacyVariable].filter(Boolean);
  for (const name of names) {
    const value = (process.env[name] ?? "").trim();
    if (value !== "") {
      if (/[\r\n]/.test(value)) fail(`${name} must be a single line`);
      return value;
    }
  }
  if (allowCheckValue) return "check-only-value";
  return "";
}

function replaceAssignment(template, field, value) {
  if (!template.includes(field.marker)) {
    fail(`template marker for ${field.describe ?? field.variable} is missing; update this renderer with the template`);
  }
  const assignment = `${field.key} = ${JSON.stringify(value)}`;
  if (field.insertBefore) return template.replace(field.marker, `${assignment}\n${field.marker}`);
  return template.replace(field.marker, assignment);
}

function render(template, options) {
  let out = template;
  const missing = [];
  for (const field of REQUIRED) {
    const value = envValue(field, options.check);
    if (value === "") missing.push(field.variable);
    else out = replaceAssignment(out, field, value);
  }
  for (const field of OPTIONAL) {
    const value = envValue(field, false);
    if (value !== "") out = replaceAssignment(out, field, value);
  }
  if (missing.length > 0) {
    process.stderr.write(
      "render-wrangler-config: missing required environment variables:\n" +
        missing.map((name) => `  - ${name}\n`).join("") +
        "Values are never printed.\n",
    );
    process.exit(1);
  }
  const route = (process.env.AC_CLOUDFLARE_WORKER_ROUTE ?? "").trim();
  if (route !== "") {
    if (/[\r\n]/.test(route)) fail("AC_CLOUDFLARE_WORKER_ROUTE must be a single line");
    out += `\n# Rendered from AC_CLOUDFLARE_WORKER_ROUTE. Never committed.\nroutes = [\n  { pattern = ${JSON.stringify(route)}, custom_domain = true }\n]\n`;
  }
  return out;
}

const options = parseArgs(process.argv.slice(2));
if (!existsSync(templatePath)) fail(`template not found at ${templatePath}`);
const rendered = render(readFileSync(templatePath, "utf8"), options);
for (const field of REQUIRED) {
  const key = field.key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  if (!new RegExp(`^\\s*${key}\\s*=\\s*"[^"\\r\\n]+"`, "m").test(rendered)) {
    fail(`rendered config has no ${field.describe}; renderer and template have drifted`);
  }
}
if (options.check) {
  process.stdout.write("render-wrangler-config: template is renderable and complete\n");
} else {
  writeFileSync(options.out, rendered, "utf8");
  process.stdout.write("render-wrangler-config: wrote ignored output\n");
}
