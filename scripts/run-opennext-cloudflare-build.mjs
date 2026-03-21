#!/usr/bin/env node

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const DEFAULT_SITE_URLS = {
  production: "https://my9.shatranj.space",
  test: "https://my9test.shatranj.space",
};
const CF_BUILD_OUTPUT_DIR = ".cf-build";
const SHELL_SITE_URL = process.env.SITE_URL;
const SHELL_PUBLIC_SITE_URL = process.env.NEXT_PUBLIC_SITE_URL;

function loadLocalEnvFiles() {
  for (const file of [".env.local", ".env"]) {
    try {
      process.loadEnvFile(file);
    } catch {
      // ignore missing env files
    }
  }
}

function readFlag(name) {
  const exact = `--${name}`;
  const prefix = `${exact}=`;
  for (let index = 0; index < process.argv.length; index += 1) {
    const arg = process.argv[index];
    if (arg === exact) {
      return process.argv[index + 1] ?? null;
    }
    if (arg.startsWith(prefix)) {
      return arg.slice(prefix.length) || null;
    }
  }
  return null;
}

function resolveTargetEnv() {
  const envName = readFlag("env");
  return envName === "test" ? "test" : "production";
}

function resolveSiteUrl(targetEnv) {
  return (
    readFlag("site-url") ??
    SHELL_SITE_URL ??
    SHELL_PUBLIC_SITE_URL ??
    DEFAULT_SITE_URLS[targetEnv]
  );
}

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

function syncDirectoryContents(sourceDir, targetDir) {
  if (!fs.existsSync(sourceDir)) {
    return;
  }

  fs.mkdirSync(targetDir, { recursive: true });

  for (const entry of fs.readdirSync(sourceDir, { withFileTypes: true })) {
    const sourcePath = path.join(sourceDir, entry.name);
    const targetPath = path.join(targetDir, entry.name);

    fs.rmSync(targetPath, { recursive: true, force: true });
    fs.cpSync(sourcePath, targetPath, { recursive: true });
  }
}

function syncNestedServerFunctionBuildOutputs() {
  const serverFunctionsDir = path.join(
    process.cwd(),
    CF_BUILD_OUTPUT_DIR,
    ".open-next",
    "server-functions"
  );

  if (!fs.existsSync(serverFunctionsDir)) {
    return;
  }

  for (const entry of fs.readdirSync(serverFunctionsDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const functionDir = path.join(serverFunctionsDir, entry.name);
    const nestedBuildDir = path.join(functionDir, CF_BUILD_OUTPUT_DIR);
    syncDirectoryContents(nestedBuildDir, functionDir);
  }
}

async function refreshShareCountSnapshot(targetEnv, env) {
  const args = ["scripts/write-share-count-snapshot.mjs", "--remote"];

  if (targetEnv === "test") {
    args.push("--test");
  }

  // OpenNext invokes Next directly, so npm prebuild hooks do not run here.
  const exitCode = await run("node", args, env);
  if (exitCode !== 0) {
    throw new Error(`share count snapshot refresh failed with exit code ${exitCode}`);
  }
}

async function main() {
  loadLocalEnvFiles();

  const targetEnv = resolveTargetEnv();
  const siteUrl = resolveSiteUrl(targetEnv);
  const args = ["build"];

  if (targetEnv === "test") {
    args.push("--env=test");
  }

  console.log(`[cf:build] target=${targetEnv} siteUrl=${siteUrl}`);

  const buildEnv = {
    ...process.env,
    SITE_URL: siteUrl,
    NEXT_PUBLIC_SITE_URL: siteUrl,
    NEXT_DIST_DIR: `${CF_BUILD_OUTPUT_DIR}/.next`,
    NODE_OPTIONS: process.env.NODE_OPTIONS ?? "--max-old-space-size=6144",
  };

  await refreshShareCountSnapshot(targetEnv, buildEnv);

  const exitCode = await run("opennextjs-cloudflare", args, buildEnv);

  if (exitCode === 0) {
    const sourceBuildDir = path.join(process.cwd(), CF_BUILD_OUTPUT_DIR, ".open-next", ".build");
    const compatBuildDir = path.join(process.cwd(), ".open-next", ".build");
    if (fs.existsSync(sourceBuildDir)) {
      fs.mkdirSync(path.dirname(compatBuildDir), { recursive: true });
      fs.rmSync(compatBuildDir, { recursive: true, force: true });
      fs.cpSync(sourceBuildDir, compatBuildDir, { recursive: true });
    }

    syncNestedServerFunctionBuildOutputs();
  }

  process.exitCode = exitCode;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
