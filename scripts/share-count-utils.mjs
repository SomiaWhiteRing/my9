import { spawn } from "node:child_process";

const SHARES_V2_TABLE = "my9_share_registry_v2";
const DEFAULT_D1_DATABASE_NAMES = {
  production: "my9-prod",
  test: "my9-test",
};

function toNonNegativeInt(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.trunc(value));
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return Math.max(0, Math.trunc(parsed));
    }
  }
  return null;
}

export function parseShareCount(value) {
  return toNonNegativeInt(value);
}

function buildWranglerEnv() {
  const env = { ...process.env };
  const sqlToken = env.MY9_SQL_API_TOKEN?.trim();

  if (sqlToken) {
    env.CLOUDFLARE_API_TOKEN = sqlToken;
  }

  return env;
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    const commandLine = [command, ...args.map((arg) => (/[\s()*;]/.test(arg) ? `"${arg.replaceAll('"', '\\"')}"` : arg))].join(" ");
    const child = spawn(commandLine, {
      cwd: process.cwd(),
      env: buildWranglerEnv(),
      shell: true,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });

    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (signal) {
        reject(new Error(`command exited via signal: ${signal}`));
        return;
      }
      if ((code ?? 1) !== 0) {
        reject(new Error(stderr.trim() || stdout.trim() || `command failed: ${command}`));
        return;
      }
      resolve(stdout);
    });
  });
}

function parseWranglerJson(stdout) {
  const jsonStart = stdout.search(/[\[{]/);
  if (jsonStart === -1) {
    throw new Error(`wrangler returned no JSON payload: ${stdout}`.trim());
  }
  return JSON.parse(stdout.slice(jsonStart));
}

async function queryCountFromWranglerD1(targetEnv, mode) {
  const databaseName = DEFAULT_D1_DATABASE_NAMES[targetEnv === "test" ? "test" : "production"];
  const args = [
    "wrangler",
    "d1",
    "execute",
    databaseName,
    `--${mode}`,
    `--command=SELECT COUNT(*) AS total_count FROM ${SHARES_V2_TABLE}`,
    "--json",
  ];

  if (targetEnv === "test") {
    args.push("--env", "test");
  }

  const stdout = await run("npx", args);
  const payload = parseWranglerJson(stdout);
  const rows = payload?.[0]?.results ?? payload?.results ?? [];
  return toNonNegativeInt(rows?.[0]?.total_count) ?? 0;
}

export async function resolveShareCountFromD1Local(targetEnv = "production") {
  return await queryCountFromWranglerD1(targetEnv, "local");
}

export async function resolveShareCountFromD1Remote(targetEnv = "production") {
  return await queryCountFromWranglerD1(targetEnv, "remote");
}
