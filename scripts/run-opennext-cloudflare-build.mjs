#!/usr/bin/env node

import { spawn } from "node:child_process";

const DEFAULT_SITE_URLS = {
  production: "https://my9.shatranj.space",
  test: "https://my9test.shatranj.space",
};
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

async function main() {
  loadLocalEnvFiles();

  const targetEnv = resolveTargetEnv();
  const siteUrl = resolveSiteUrl(targetEnv);
  const args = ["build"];

  if (targetEnv === "test") {
    args.push("--env=test");
  }

  console.log(`[cf:build] target=${targetEnv} siteUrl=${siteUrl}`);

  const exitCode = await run("opennextjs-cloudflare", args, {
    ...process.env,
    SITE_URL: siteUrl,
    NEXT_PUBLIC_SITE_URL: siteUrl,
  });

  process.exitCode = exitCode;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
