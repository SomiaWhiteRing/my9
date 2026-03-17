#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { resolve } from "node:path";

const TREND_COUNT_DAY_TABLE = "my9_trend_subject_kind_day_v3";
const TREND_COUNT_HOUR_TABLE = "my9_trend_subject_kind_hour_v3";
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;

function loadLocalEnvFiles() {
  for (const file of [".env.local", ".env"]) {
    try {
      process.loadEnvFile(resolve(process.cwd(), file));
    } catch {
      // ignore missing env file
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

function parsePositiveInt(value, fallback) {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
}

function parseArg(name, fallback) {
  const prefix = `--${name}=`;
  const withEquals = process.argv.find((arg) => arg.startsWith(prefix));
  if (withEquals) {
    return parsePositiveInt(withEquals.slice(prefix.length), fallback);
  }

  const index = process.argv.indexOf(`--${name}`);
  if (index === -1 || index + 1 >= process.argv.length) return fallback;
  return parsePositiveInt(process.argv[index + 1], fallback);
}

function toBeijingDayKey(timestampMs) {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

function toBeijingHourBucket(timestampMs) {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS);
}

function getBeijingDayStart(timestampMs) {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / DAY_MS) * DAY_MS - BEIJING_TZ_OFFSET_MS;
}

async function main() {
  loadLocalEnvFiles();

  const cleanupTrendDays = Math.max(
    180,
    parseArg("cleanup-trend-days", parsePositiveInt(readEnv("MY9_TREND_CLEANUP_DAYS"), 190))
  );

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

  const sql = neon(databaseUrl);
  const cleanupBeforeDayKey = toBeijingDayKey(Date.now() - cleanupTrendDays * DAY_MS);
  const dayRows = await sql.query(
    `
    DELETE FROM ${TREND_COUNT_DAY_TABLE}
    WHERE day_key < $1
    RETURNING 1
    `,
    [cleanupBeforeDayKey]
  );

  const cleanupBeforeHourBucket = toBeijingHourBucket(getBeijingDayStart(Date.now()) - DAY_MS);
  const hourRows = await sql.query(
    `
    DELETE FROM ${TREND_COUNT_HOUR_TABLE}
    WHERE hour_bucket < $1
    RETURNING 1
    `,
    [cleanupBeforeHourBucket]
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        cleanupTrendDays,
        cleanedTrendRows: dayRows.length + hourRows.length,
        cleanedDayRows: dayRows.length,
        cleanedHourRows: hourRows.length,
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
