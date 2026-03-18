#!/usr/bin/env node

import { spawn } from "node:child_process";
import path from "node:path";

for (const file of [".env.local", ".env"]) {
  try {
    process.loadEnvFile(path.resolve(process.cwd(), file));
  } catch {
    // ignore missing env files
  }
}

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("usage: node scripts/run-wrangler-with-sql-token.mjs <wrangler args...>");
  process.exit(1);
}

const env = { ...process.env };
const sqlToken = env.MY9_SQL_API_TOKEN?.trim();
if (sqlToken) {
  env.CLOUDFLARE_API_TOKEN = sqlToken;
}

const child = spawn("npx", ["wrangler", ...args], {
  cwd: process.cwd(),
  env,
  stdio: "inherit",
  shell: true,
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});

child.on("error", (error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
