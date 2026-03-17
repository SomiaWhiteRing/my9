#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { resolve } from "node:path";

const SHARES_V2_TABLE = "my9_share_registry_v2";
const SHARES_V2_TIER_CREATED_IDX = `${SHARES_V2_TABLE}_tier_created_idx`;

function loadLocalEnvFiles() {
  for (const file of [".env.local", ".env"]) {
    try {
      process.loadEnvFile(resolve(process.cwd(), file));
    } catch {
      // ignore missing env files
    }
  }
}

function readEnv(...names) {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function buildDatabaseUrlFromNeonParts() {
  const host = readEnv("NEON_DATABASE_PGHOST_UNPOOLED", "NEON_DATABASE_PGHOST");
  const user = readEnv("NEON_DATABASE_PGUSER");
  const password = readEnv("NEON_DATABASE_PGPASSWORD", "NEON_DATABASE_POSTGRES_PASSWORD");
  const database = readEnv("NEON_DATABASE_PGDATABASE", "NEON_DATABASE_POSTGRES_DATABASE");
  if (!host || !user || !password || !database) return null;

  let hostWithPort = host;
  const port = readEnv("NEON_DATABASE_PGPORT");
  if (port && !host.includes(":")) {
    hostWithPort = `${host}:${port}`;
  }

  const sslMode = readEnv("NEON_DATABASE_PGSSLMODE") ?? "require";
  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(
    password
  )}@${hostWithPort}/${encodeURIComponent(database)}?sslmode=${encodeURIComponent(sslMode)}`;
}

function hasArg(name) {
  return process.argv.includes(`--${name}`);
}

async function main() {
  loadLocalEnvFiles();

  const databaseUrl =
    process.env.DATABASE_URL ??
    readEnv(
      "NEON_DATABASE_DATABASE_URL_UNPOOLED",
      "NEON_DATABASE_POSTGRES_URL_NON_POOLING",
      "NEON_DATABASE_POSTGRES_URL",
      "NEON_DATABASE_DATABASE_URL"
    ) ??
    buildDatabaseUrlFromNeonParts();

  if (!databaseUrl) {
    throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  }

  const dryRun = hasArg("dry-run");
  const sql = neon(databaseUrl);

  const rows = await sql.query(`
    SELECT
      COUNT(*) FILTER (WHERE hot_payload IS NULL)::BIGINT AS null_hot_payload_rows,
      COUNT(*) FILTER (WHERE storage_tier = 'cold')::BIGINT AS cold_rows
    FROM ${SHARES_V2_TABLE}
  `);
  const stats = rows[0] ?? {};
  const nullHotPayloadRows = BigInt(String(stats.null_hot_payload_rows ?? 0));
  const coldRows = BigInt(String(stats.cold_rows ?? 0));

  console.log(
    JSON.stringify(
      {
        dryRun,
        nullHotPayloadRows: nullHotPayloadRows.toString(),
        coldRows: coldRows.toString(),
      },
      null,
      2
    )
  );

  if (nullHotPayloadRows > 0n || coldRows > 0n) {
    throw new Error("Cannot drop cold-storage columns while rows still depend on them.");
  }

  if (dryRun) {
    console.log("[dry-run] Schema unchanged.");
    return;
  }

  await sql.query(`DROP INDEX IF EXISTS ${SHARES_V2_TIER_CREATED_IDX}`);
  await sql.query(`
    ALTER TABLE ${SHARES_V2_TABLE}
    DROP COLUMN IF EXISTS storage_tier,
    DROP COLUMN IF EXISTS cold_object_key
  `);
  await sql.query(`
    ALTER TABLE ${SHARES_V2_TABLE}
    ALTER COLUMN hot_payload SET NOT NULL
  `);

  console.log(`[done] Removed cold-storage columns from ${SHARES_V2_TABLE}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
