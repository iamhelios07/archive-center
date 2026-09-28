// Runs the Worker test suite in workerd.
//
// The temp directory is the whole reason this script exists.
//
// workerd.exe lives inside this repository, and this repository carries a LOW
// mandatory integrity label. A process launched from a Low-labelled file runs at
// Low integrity. The runtime's default scratch space (%TEMP% on Windows) is
// Medium, and Windows Mandatory Integrity Control forbids a Low process from
// writing into a Medium directory. So every disk-backed binding — D1, KV, R2 —
// failed at startup with
//
//   kj/filesystem-disk-win32.c++:773
//   CreateDirectory: #5 액세스가 거부되었습니다.
//   path = miniflare-D1DatabaseObject
//
// which reads exactly like a workerd bug and is not one: workerd itself runs
// fine here, and 13 tests passed before the D1 binding was added back.
//
// Giving the runtime a LOW-labelled scratch directory puts both sides of that
// write on the same level and the denial disappears. The alternative — relabelling
// workerd.exe up to Medium — also works, but it weakens the security posture of
// a 96 MB third-party binary and npm install restores the original label on
// every reinstall, so it would silently break again. Lowering the scratch
// directory instead is the contained fix.
//
// On a machine where these paths are already Low, or on a non-Windows host, the
// icacls call is a no-op and everything behaves the same.

import { execFileSync, spawn } from "node:child_process";
import { mkdirSync } from "node:fs";
import { resolve } from "node:path";

const scratch = resolve(".workerd-temp");
mkdirSync(scratch, { recursive: true });

if (process.platform === "win32") {
  try {
    execFileSync("icacls", [scratch, "/setintegritylevel", "(OI)(CI)L"], { stdio: "ignore" });
  } catch {
    // Not fatal here: the run below will fail loudly with the CreateDirectory
    // error if the label could not be set, and that message names the cause far
    // better than anything this script could add.
  }
}

const child = spawn("vitest", ["run", ...process.argv.slice(2)], {
  stdio: "inherit",
  shell: true,
  env: { ...process.env, TEMP: scratch, TMP: scratch },
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
