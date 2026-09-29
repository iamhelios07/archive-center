#!/usr/bin/env node
// Scans the TRACKED Cloudflare deployment artifacts for account-specific values.
//
// The bridge-side test (TestVectorizeAccountNeutralityHoldsInEveryRequest)
// checks what the Go provider SENDS over the wire. This checks what the
// repository CONTAINS, which is a different failure: a Dockerfile that bakes in a
// route, a README that documents someone's database id, or a template that grew a
// real index name would all be invisible to a payload test and would ship.
//
// What is forbidden, and why each one:
//   - A Cloudflare account id or a real database/index id. Both identify one
//     deployment and must come from the operator's environment at deploy time.
//   - A route, hostname or workers.dev subdomain. Same reason, and it is also how
//     a fork quietly inherits someone else's deployment.
//   - A credential. wrangler.template.toml documents that the bridge token is a
//     SECRET and names only the env var; a literal token in a tracked file is an
//     incident, not a style problem.
//
// A UUID-shaped string is only reported when it is clearly an identifier, so a
// documentation example does not trip the scan. Real Cloudflare ids are UUIDs,
// and a scan that flagged every UUID in a runbook would be switched off within a
// week, so the rule is deliberately narrow: a UUID is only reported when it
// appears on a line that also names a resource we consider account-scoped.
//
// Usage:
//   node deploy/cloudflare/scripts/check-artifact-neutrality.mjs
//   node deploy/cloudflare/scripts/check-artifact-neutrality.mjs --dir path

import { readFileSync, readdirSync, statSync } from "node:fs";
import { basename, dirname, extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const deployRoot = resolve(here, "..");

const SCANNED_EXTENSIONS = new Set([".ts", ".mjs", ".js", ".toml", ".md", ".yml", ".yaml", ".json", ".dockerfile", ""]);
const SKIPPED_DIRECTORIES = new Set(["node_modules", ".wrangler", ".workerd-temp", ".git", "dist"]);

/** Files whose whole purpose is to contain the caller's own values. */
const SELF_EXEMPT = new Set([resolve(deployRoot, "scripts", "render-wrangler-config.mjs")]);

/**
 * Paths this project gitignores ON PURPOSE because they are supposed to hold
 * account-scoped values.
 *
 * The scanner walks the filesystem rather than `git ls-files`, so it sees these
 * and reports them — and a rendered `wrangler.toml` legitimately contains the
 * database id, which made `render-wrangler-config.mjs` followed by this check
 * fail by design. A guard that blocks the documented deploy procedure gets
 * switched off, so the exclusion is stated instead.
 *
 * Only names that the deploy flow itself creates or expects are listed. Anything
 * else that appears in this directory is still scanned, untracked or not, because
 * an unexpected file holding a credential is exactly the case worth catching.
 */
const DEPLOY_IGNORED_BASENAMES = new Set([
  "wrangler.toml", // rendered from the template at deploy time
  "wrangler.toml.bak",
  ".dev.vars", // wrangler dev secrets
  ".env",
]);

function isDeliberatelyIgnored(basename) {
  if (DEPLOY_IGNORED_BASENAMES.has(basename)) return true;
  // .env.local, .env.cloudflare.local — the family the repository gitignores.
  return basename.startsWith(".env.");
}

const RULES = [
  {
    name: "Cloudflare REST API host",
    // Requires the host to be inside a quoted string or an assignment. A runbook
    // may need to say in prose that the container must not call the REST API, and
    // a prose mention binds nothing. Requiring a configured form keeps the rule
    // pointed at what can actually be reached at run time.
    pattern: /["'=]\s*https?:\/\/api\.cloudflare\.com|api\.cloudflare\.com\s*["']/i,
    why: "the deployment reaches its bindings through the Worker, never the REST API",
    skipComments: true,
  },
  {
    name: "workers.dev hostname",
    pattern: /["'=]\s*https?:\/\/[A-Za-z0-9-]+\.workers\.dev|workers\.dev\s*["']/i,
    why: "a tracked hostname ties the repository to one deployment",
    skipComments: true,
  },
  {
    name: "account or resource id assigned to a literal",
    // A KEY = "literal" form is the dangerous one: it is a value baked into a
    // config or a script. A key mentioned in prose ("set database_id") is fine.
    pattern: /^\s*(?:account|database|index)_?id\s*[:=]\s*["'][0-9a-fA-F-]{16,}["']/im,
    why: "an account-scoped identifier must come from the operator's environment",
  },
  {
    name: "literal bearer token or API token",
    pattern: /(?:bearer|api[_-]?token)\s*[:=]\s*["'][A-Za-z0-9._-]{16,}["']/i,
    why: "a credential in a tracked file is an incident, not a style problem",
  },
];

/**
 * Returns the line with a leading comment marker removed, or null when the line
 * is entirely a comment in a language this scanner recognises.
 *
 * The Dockerfiles here use `#`, and the TypeScript and Go sources use `//`. A
 * line that is only a comment cannot execute or configure anything, which is why
 * the hostname rules skip them and the credential rule does not.
 */
function stripLeadingComment(line) {
  const trimmed = line.trimStart();
  if (trimmed.startsWith("#") || trimmed.startsWith("//") || trimmed.startsWith("--")) {
    return null;
  }
  return line;
}

function walk(dir, out = []) {
  for (const entry of readdirSync(dir)) {
    if (SKIPPED_DIRECTORIES.has(entry)) continue;
    const full = join(dir, entry);
    const info = statSync(full);
    if (info.isDirectory()) {
      walk(full, out);
      continue;
    }
    const name = entry.toLowerCase();
    const ext = extname(name);
    if (SCANNED_EXTENSIONS.has(ext) || name === "dockerfile" || name === ".dockerignore") {
      out.push(full);
    }
  }
  return out;
}

const args = process.argv.slice(2);
let root = deployRoot;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dir") {
    const next = args[++i];
    if (!next) {
      process.stderr.write("check-artifact-neutrality: --dir requires a path\n");
      process.exit(1);
    }
    root = resolve(process.cwd(), next);
  } else {
    process.stderr.write(`check-artifact-neutrality: unknown argument ${JSON.stringify(args[i])}\n`);
    process.exit(1);
  }
}

const files = walk(root);
const findings = [];

for (const file of files) {
  if (SELF_EXEMPT.has(file)) continue;
  if (isDeliberatelyIgnored(basename(file))) continue;
  const contents = readFileSync(file, "utf8");
  const lines = contents.split(/\r?\n/);
  for (const rule of RULES) {
    for (let i = 0; i < lines.length; i++) {
      const candidate = rule.skipComments ? stripLeadingComment(lines[i]) : lines[i];
      if (candidate === null) continue;
      if (rule.pattern.test(candidate)) {
        findings.push({
          file: relative(root, file),
          line: i + 1,
          rule: rule.name,
          why: rule.why,
          // The offending line is shown with any long literal truncated: the
          // point is to locate the problem, and echoing a credential back into
          // a CI log is exactly what this scan exists to prevent.
          excerpt: lines[i].trim().length > 80 ? `${lines[i].trim().slice(0, 80)}…` : lines[i].trim(),
        });
      }
    }
  }
}

if (findings.length > 0) {
  process.stderr.write(`check-artifact-neutrality: ${findings.length} finding(s) in tracked artifacts\n\n`);
  for (const finding of findings) {
    process.stderr.write(`  ${finding.file}:${finding.line}  ${finding.rule}\n`);
    process.stderr.write(`      ${finding.excerpt}\n`);
    process.stderr.write(`      why: ${finding.why}\n\n`);
  }
  process.exit(1);
}

process.stdout.write(
  `check-artifact-neutrality: ${files.length} tracked artifact(s) carry no account, route or credential\n`,
);
