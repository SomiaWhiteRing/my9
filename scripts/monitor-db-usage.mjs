#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { resolve } from "node:path";

const DEFAULT_MAX_MB = 512;
const DEFAULT_WARN_PERCENT = 70;
const DEFAULT_CRITICAL_PERCENT = 90;
const DEFAULT_TOP_LIMIT = 12;

const CORE_TABLES = [
  "my9_share_registry_v2",
  "my9_share_alias_v1",
  "my9_subject_dim_v1",
  "my9_trend_subject_kind_all_v3",
  "my9_trend_subject_kind_day_v3",
  "my9_trend_subject_kind_hour_v3",
  "my9_trends_cache_v1",
  "my9_trend_count_all_v1",
  "my9_trend_count_day_v1",
  "my9_shares_v1",
];

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

function hasArg(name) {
  return process.argv.includes(`--${name}`);
}

function parseNumberArg(name, fallback) {
  const prefix = `--${name}=`;
  const valueArg = process.argv.find((arg) => arg.startsWith(prefix));
  if (valueArg) {
    const parsed = Number(valueArg.slice(prefix.length));
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function toMb(bytes) {
  return Number(bytes) / (1024 * 1024);
}

function parseFailLevel() {
  const prefix = "--fail-on=";
  const valueArg = process.argv.find((arg) => arg.startsWith(prefix));
  if (!valueArg) return null;
  const value = valueArg.slice(prefix.length).trim().toLowerCase();
  if (value === "warn" || value === "critical") return value;
  return null;
}

async function tableExists(sql, tableName) {
  const rows = await sql.query("SELECT to_regclass($1) IS NOT NULL AS ok", [tableName]);
  return Boolean(rows[0]?.ok);
}

async function getTableStats(sql, tableName, exactCounts) {
  const exists = await tableExists(sql, tableName);
  if (!exists) return { table: tableName, exists: false };

  const rows = await sql.query(
    `
    SELECT
      COALESCE(s.n_live_tup::BIGINT, c.reltuples::BIGINT, 0::BIGINT) AS row_estimate,
      pg_total_relation_size($1::regclass)::BIGINT AS total_bytes,
      pg_relation_size($1::regclass)::BIGINT AS table_bytes,
      (pg_total_relation_size($1::regclass) - pg_relation_size($1::regclass))::BIGINT AS index_toast_bytes,
      pg_size_pretty(pg_total_relation_size($1::regclass)) AS total_pretty
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = 'public' AND c.relname = $2
    `,
    [tableName, tableName]
  );

  let rowCount = null;
  if (exactCounts) {
    const countRows = await sql.query(`SELECT COUNT(*)::BIGINT AS row_count FROM ${tableName}`);
    rowCount = countRows[0]?.row_count ?? null;
  }

  return {
    table: tableName,
    exists: true,
    ...rows[0],
    row_count: rowCount,
  };
}

function statusFromPercent(usedPercent, warnPercent, criticalPercent) {
  if (usedPercent >= criticalPercent) return "critical";
  if (usedPercent >= warnPercent) return "warn";
  return "ok";
}

function shouldFail(status, failOn) {
  if (!failOn) return false;
  if (failOn === "warn") return status === "warn" || status === "critical";
  if (failOn === "critical") return status === "critical";
  return false;
}

function printTextReport(report) {
  console.log(
    `[db] ${report.db_size_pretty} / ${report.max_limit_mb} MB (${report.used_percent.toFixed(2)}%) status=${report.status}`
  );
  console.log(`[headroom] ${report.remaining_mb.toFixed(2)} MB`);

  console.log(`[top_tables] top ${report.top_tables.length}`);
  for (const row of report.top_tables) {
    console.log(
      `- ${row.table}: ${row.total_pretty} (rows=${Number(row.row_estimate).toLocaleString("en-US")}, indexes+toast=${toMb(
        row.index_toast_bytes
      ).toFixed(2)} MB)`
    );
  }

  console.log("[core_tables]");
  for (const item of report.core_tables) {
    if (!item.exists) {
      console.log(`- ${item.table}: missing`);
      continue;
    }
    console.log(
      `- ${item.table}: ${item.total_pretty}, rows=${Number(item.row_count ?? item.row_estimate).toLocaleString(
        "en-US"
      )}, table=${toMb(
        item.table_bytes
      ).toFixed(2)} MB`
    );
  }

  if (report.cache_expired_rows !== null) {
    console.log(`[cache] expired_rows=${report.cache_expired_rows}`);
  }
}

async function main() {
  loadLocalEnvFiles();

  const maxMb = parseNumberArg("max-mb", DEFAULT_MAX_MB);
  const warnPercent = parseNumberArg("warn-percent", DEFAULT_WARN_PERCENT);
  const criticalPercent = parseNumberArg("critical-percent", DEFAULT_CRITICAL_PERCENT);
  const topLimit = Math.max(1, Math.trunc(parseNumberArg("top", DEFAULT_TOP_LIMIT)));
  const outputJson = hasArg("json");
  const exactCounts = hasArg("exact-counts");
  const failOn = parseFailLevel();

  const databaseUrl = process.env.DATABASE_URL || buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  }

  const sql = neon(databaseUrl);

  const dbRows = await sql.query(
    `
    SELECT
      current_database() AS database_name,
      pg_database_size(current_database())::BIGINT AS db_size_bytes,
      pg_size_pretty(pg_database_size(current_database())) AS db_size_pretty
    `
  );

  const topTables = await sql.query(
    `
    SELECT
      c.relname AS table,
      c.reltuples::BIGINT AS row_estimate,
      pg_total_relation_size(c.oid)::BIGINT AS total_bytes,
      pg_relation_size(c.oid)::BIGINT AS table_bytes,
      (pg_total_relation_size(c.oid) - pg_relation_size(c.oid))::BIGINT AS index_toast_bytes,
      pg_size_pretty(pg_total_relation_size(c.oid)) AS total_pretty
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
    ORDER BY pg_total_relation_size(c.oid) DESC
    LIMIT $1
    `,
    [topLimit]
  );

  const coreTableStats = [];
  for (const tableName of CORE_TABLES) {
    coreTableStats.push(await getTableStats(sql, tableName, exactCounts));
  }

  const cacheExists = coreTableStats.find((item) => item.table === "my9_trends_cache_v1" && item.exists);
  let cacheExpiredRows = null;
  if (cacheExists) {
    const rows = await sql.query(
      `
      SELECT COUNT(*) FILTER (WHERE expires_at < EXTRACT(EPOCH FROM now()) * 1000)::BIGINT AS expired_rows
      FROM my9_trends_cache_v1
      `
    );
    cacheExpiredRows = Number(rows[0]?.expired_rows ?? 0);
  }

  const dbInfo = dbRows[0];
  const dbSizeBytes = Number(dbInfo.db_size_bytes);
  const maxBytes = maxMb * 1024 * 1024;
  const usedPercent = maxBytes > 0 ? (dbSizeBytes / maxBytes) * 100 : 0;
  const remainingMb = Math.max(0, (maxBytes - dbSizeBytes) / (1024 * 1024));
  const status = statusFromPercent(usedPercent, warnPercent, criticalPercent);

  const report = {
    collected_at: new Date().toISOString(),
    database: dbInfo.database_name,
    db_size_bytes: dbSizeBytes,
    db_size_pretty: dbInfo.db_size_pretty,
    max_limit_mb: maxMb,
    used_percent: usedPercent,
    remaining_mb: remainingMb,
    warn_percent: warnPercent,
    critical_percent: criticalPercent,
    exact_counts: exactCounts,
    status,
    cache_expired_rows: cacheExpiredRows,
    top_tables: topTables,
    core_tables: coreTableStats,
  };

  if (outputJson) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    printTextReport(report);
  }

  if (shouldFail(status, failOn)) {
    process.exit(2);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
