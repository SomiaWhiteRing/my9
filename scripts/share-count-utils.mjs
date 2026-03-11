import { neon } from "@neondatabase/serverless";

const SHARES_V1_TABLE = "my9_shares_v1";
const SHARES_V2_TABLE = "my9_share_registry_v2";

function readEnv(...names) {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

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

export function buildDatabaseUrlFromNeonParts() {
  const host = readEnv("NEON_DATABASE_PGHOST_UNPOOLED", "NEON_DATABASE_PGHOST");
  const user = readEnv("NEON_DATABASE_PGUSER");
  const password = readEnv("NEON_DATABASE_PGPASSWORD", "NEON_DATABASE_POSTGRES_PASSWORD");
  const database = readEnv("NEON_DATABASE_PGDATABASE", "NEON_DATABASE_POSTGRES_DATABASE");

  if (!host || !user || !password || !database) {
    return null;
  }

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

async function tryCountFromTable(sql, tableName) {
  try {
    const rows = await sql.query(
      `
      SELECT COUNT(*)::BIGINT AS total_count
      FROM ${tableName}
      `
    );
    const nextCount = toNonNegativeInt(rows?.[0]?.total_count);
    return nextCount ?? 0;
  } catch {
    return null;
  }
}

export async function resolveShareCountFromDatabase(databaseUrl) {
  const sql = neon(databaseUrl);

  const v2Count = await tryCountFromTable(sql, SHARES_V2_TABLE);
  if (v2Count !== null && v2Count > 0) {
    return v2Count;
  }

  const v1Count = await tryCountFromTable(sql, SHARES_V1_TABLE);
  if (v1Count !== null) {
    return v1Count;
  }

  if (v2Count !== null) {
    return v2Count;
  }

  throw new Error("Failed to read share count from v1/v2 tables.");
}

export function parseShareCount(value) {
  return toNonNegativeInt(value);
}
