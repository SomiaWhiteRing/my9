#!/usr/bin/env node

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

function run(command, args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: "inherit",
      shell: true,
      env,
    });

    child.on("exit", (code, signal) => {
      if (signal) {
        reject(new Error(`command exited via signal: ${signal}`));
        return;
      }
      resolve(code ?? 0);
    });

    child.on("error", reject);
  });
}

function syncBundlerRuntimeMarkers(sourceDistDir, compatDistDir) {
  const resolvedSourceDir = path.resolve(sourceDistDir);
  const resolvedCompatDir = path.resolve(compatDistDir);

  if (resolvedSourceDir === resolvedCompatDir) {
    return;
  }

  const runtimeFiles = [
    ["server", "webpack-runtime.js"],
    ["server", "chunks", "[turbopack]_runtime.js"],
    ["server", "chunks", "ssr", "[turbopack]_runtime.js"],
  ];

  for (const segments of runtimeFiles) {
    const sourcePath = path.join(resolvedSourceDir, ...segments);
    if (!fs.existsSync(sourcePath)) {
      continue;
    }

    const compatPath = path.join(resolvedCompatDir, ...segments);
    fs.mkdirSync(path.dirname(compatPath), { recursive: true });
    fs.copyFileSync(sourcePath, compatPath);
  }
}

async function main() {
  const exitCode = await run("next", ["build", "--webpack"], process.env);
  if (exitCode !== 0) {
    process.exitCode = exitCode;
    return;
  }

  const sourceDistDir = process.env.NEXT_DIST_DIR || ".next";
  syncBundlerRuntimeMarkers(sourceDistDir, ".next");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
