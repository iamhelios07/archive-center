#!/usr/bin/env node
// Renders a deployable wrangler config from the tracked template.
//
// The repository never carries an account, database, index, route or domain.
// deploy/cloudflare/wrangler.template.toml holds the stable shape with those
// values left as comments, and this script materialises them from the operator's
// environment into a file that is deliberately gitignored.
//
// Two rules shape the design, and both are about not making a deploy-time
// mistake look like a success:
//
//   1. A missing required value is a hard failure, never a rendered blank. A
//      config with `database_id = ""` deploys cleanly and then fails at runtime
//      on the first query, which is a far worse place to discover a missing
//      variable than here.
//   2. Values are never echoed. The script reports WHICH variable was missing, by
//      name, and never prints what it read. Deploy logs are routinely pasted
//      into issues.
//
// Usage:
//   node deploy/cloudflare/scripts/render-wrangler-config.mjs            # render
//   node deploy/cloudflare/scripts/render-wrangler-config.mjs --check    # validate only
//   node deploy/cloudflare/scripts/render-wrangler-config.mjs --out path
//
// Environment:
//   AC_CLOUDFLARE_D1_DATABASE_ID   required
//   AC_CLOUDFLARE_VECTORIZE_INDEX_ID  required
//   AC_CLOUDFLARE_WORKER_ROUTE     optional; a route or domain to add

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const deployRoot = resolve(here, "..");
const templatePath = resolve(deployRoot, "wrangler.template.toml");
const defaultOut = resolve(deployRoot, "wrangler.toml");

/** Values the template marks as account-specific, and where each one goes. */
const REQUIRED = [
  {
    variable: "AC_CLOUDFLARE_D1_DATABASE_ID",
    // The template already carries the marker comment immediately above the
    // block, so the value is inserted at the documented place rather than
    // appended somewhere the operator will not look for it.
    anchor: "# database_id is account-specific and must be rendered at deploy time.",
    key: "database_id",
    render: (value) => `database_id = ${JSON.stringify(value)}`,
    describe: "D1 database id",
  },
  {
    variable: "AC_CLOUDFLARE_VECTORIZE_INDEX_ID",
    anchor: "# Vectorize index IDs are account-specific and must be rendered at deploy time.",
    key: "index_name",
    render: (value) => `index_name = ${JSON.stringify(value)}`,
    describe: "Vectorize index id",
  },
];

function parseArgs(argv) {
  const options = { check: false, out: defaultOut };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--check") {
      options.check = true;
    } else if (arg === "--out") {
      const next = argv[++i];
      if (!next) {
        fail("--out requires a path");
      }
      options.out = resolve(process.cwd(), next);
    } else {
      fail(`unknown argument ${JSON.stringify(arg)}`);
    }
  }
  return options;
}

function fail(message) {
  process.stderr.write(`render-wrangler-config: ${message}\n`);
  process.exit(1);
}

function readTemplate() {
  if (!existsSync(templatePath)) {
    fail(`template not found at ${templatePath}`);
  }
  return readFileSync(templatePath, "utf8");
}

function render(template) {
  const missing = [];
  let out = template;

  for (const field of REQUIRED) {
    if (!out.includes(field.anchor)) {
      // The anchor disappearing is a template edit, not an environment problem,
      // and it must not be reported as a missing variable: the operator would go
      // looking in the wrong place entirely.
      fail(
        `the template no longer carries the marker ${JSON.stringify(field.anchor)}. ` +
          `Re-add it or update this script; the renderer inserts ${field.describe} at that marker.`,
      );
    }
    const value = (process.env[field.variable] ?? "").trim();
    if (value === "") {
      missing.push(field.variable);
      continue;
    }

    // A key that already has a value is REPLACED, and a key that has none is
    // inserted at the marker. Adding a second assignment instead would produce a
    // duplicate TOML key, which wrangler rejects at deploy time — and a template
    // legitimately carries a placeholder value for one of these, so insertion
    // alone is wrong.
    const existing = new RegExp(`^(\\s*)${field.key}\\s*=.*$`, "m");
    if (existing.test(out)) {
      out = out.replace(existing, `$1${field.render(value)}`);
    } else {
      out = out.replace(field.anchor, `${field.render(value)}\n${field.anchor}`);
    }
  }

  const route = (process.env.AC_CLOUDFLARE_WORKER_ROUTE ?? "").trim();
  if (route !== "") {
    out += `\n# Rendered from AC_CLOUDFLARE_WORKER_ROUTE. Never committed.\nroutes = [\n  { pattern = ${JSON.stringify(route)}, custom_domain = true }\n]\n`;
  }

  if (missing.length > 0) {
    // Names only. Never the values, and never the file contents.
    process.stderr.write(
      `render-wrangler-config: missing required environment variables:\n` +
        missing.map((name) => `  - ${name}\n`).join("") +
        `Set them from the Cloudflare dashboard. Values are never printed here; ` +
        `deploy logs are routinely pasted into issues.\n`,
    );
    process.exit(1);
  }
  return out;
}

const options = parseArgs(process.argv.slice(2));
const rendered = render(readTemplate());

// A rendered config that carries a marker but no value for it means the renderer
// and the template have drifted, and deploying it would produce a config whose
// identifiers are still comments. This is checked on the KEY with a non-empty
// value, not by looking for a placeholder string, because the rendered file
// legitimately contains the operator's real value.
for (const field of REQUIRED) {
  const assigned = new RegExp(`^\\s*${field.key}\\s*=\\s*"[^"]+"`, "m");
  if (!assigned.test(rendered)) {
    fail(
      `the rendered config has no ${field.describe} value; renderer and template have drifted. ` +
        `The key ${field.key} must be assigned a non-empty quoted value.`,
    );
  }
}

if (options.check) {
  process.stdout.write("render-wrangler-config: template is renderable and complete\n");
} else {
  writeFileSync(options.out, rendered, "utf8");
  process.stdout.write(`render-wrangler-config: wrote ${options.out}\n`);
  process.stdout.write("render-wrangler-config: that path is gitignored; do not commit it.\n");
}
