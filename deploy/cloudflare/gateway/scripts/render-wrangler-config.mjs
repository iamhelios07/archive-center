#!/usr/bin/env node
// Renders the public gateway's account-specific configuration. Values are used
// only to write the ignored output and are never echoed by this program.

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const gatewayRoot = resolve(here, "..");
const templatePath = resolve(gatewayRoot, "wrangler.template.toml");
const defaultOut = resolve(gatewayRoot, "wrangler.toml");

const REQUIRED = [
  {
    variable: "AC_CLOUDFLARE_GATEWAY_WORKER_NAME",
    key: "name",
    marker: 'name = "archive-center-gateway"',
    describe: "gateway Worker name",
  },
  {
    variable: "AC_CLOUDFLARE_BRIDGE_URL",
    key: "BRIDGE_URL",
    marker: "# BRIDGE_URL is rendered from a protected deployment value. It is the only",
    describe: "bridge URL",
    var: true,
  },
  {
    variable: "AC_CLOUDFLARE_CONTAINER_IMAGE",
    key: "image",
    marker: 'image = "registry.cloudflare.com/archive-center/archive-center-go:render-me"',
    describe: "Container image URI",
  },
];

function fail(message) {
  process.stderr.write(`render-gateway-wrangler-config: ${message}\n`);
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

function valueFor(field, allowCheckValue) {
  const value = (process.env[field.variable] ?? "").trim();
  if (value === "") return allowCheckValue ? "check-only-value" : "";
  if (/[\r\n]/.test(value)) fail(`${field.variable} must be a single line`);
  return value;
}

function render(template, options) {
  let out = template;
  const missing = [];
  for (const field of REQUIRED) {
    if (!out.includes(field.marker)) {
      fail(`template marker for ${field.describe} is missing; update this renderer with the template`);
    }
    const value = valueFor(field, options.check);
    if (value === "") {
      missing.push(field.variable);
      continue;
    }
    const assignment = field.var
      ? `${field.key} = ${JSON.stringify(value)}`
      : `${field.key} = ${JSON.stringify(value)}`;
    if (field.var) out = out.replace(field.marker, `${assignment}\n${field.marker}`);
    else out = out.replace(field.marker, assignment);
  }
  if (missing.length > 0) {
    process.stderr.write(
      "render-gateway-wrangler-config: missing required environment variables:\n" +
        missing.map((name) => `  - ${name}\n`).join("") +
        "Values are never printed.\n",
    );
    process.exit(1);
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
  process.stdout.write("render-gateway-wrangler-config: template is renderable and complete\n");
} else {
  writeFileSync(options.out, rendered, "utf8");
  process.stdout.write("render-gateway-wrangler-config: wrote ignored output\n");
}
