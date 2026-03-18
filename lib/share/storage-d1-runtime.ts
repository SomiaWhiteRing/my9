import path from "node:path";
import {
  MAX_BATCH_SIZE,
  SHARE_ALIAS_TABLE,
  SHARE_SUBJECT_SLOT_TABLE,
  SHARE_VIEW_TOTAL_TABLE,
  SHARES_V2_TABLE,
  SUBJECT_DIM_TABLE,
  SUBJECT_GENRE_DIM_TABLE,
  TREND_COUNT_ALL_TABLE,
  TREND_COUNT_DAY_TABLE,
  TREND_COUNT_HOUR_TABLE,
  TRENDS_CACHE_TABLE,
  chunkArray,
  readEnv,
} from "@/lib/share/storage-common";

export type D1Scalar = string | number | null;

export type D1PreparedStatementLike = {
  bind: (...values: D1Scalar[]) => D1PreparedStatementLike;
  all: <T = Record<string, unknown>>() => Promise<{ results?: T[] } | T[]>;
  run: () => Promise<{ meta?: { changes?: number } } | { changes?: number } | unknown>;
};

export type D1DatabaseLike = {
  prepare: (query: string) => D1PreparedStatementLike;
  batch: (statements: D1PreparedStatementLike[]) => Promise<unknown[]>;
  exec: (query: string) => Promise<unknown>;
};

export type StatementInput = {
  sql: string;
  params?: D1Scalar[];
};

const D1_SCHEMA_SQL = `
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS ${SHARES_V2_TABLE} (
  share_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  creator_name TEXT,
  content_hash TEXT NOT NULL UNIQUE,
  hot_payload TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_viewed_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ${SHARES_V2_TABLE}_kind_created_idx
ON ${SHARES_V2_TABLE} (kind, created_at DESC);
CREATE TABLE IF NOT EXISTS ${SHARE_ALIAS_TABLE} (
  share_id TEXT PRIMARY KEY,
  target_share_id TEXT NOT NULL REFERENCES ${SHARES_V2_TABLE}(share_id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ${SHARE_ALIAS_TABLE}_target_idx
ON ${SHARE_ALIAS_TABLE} (target_share_id);
CREATE TABLE IF NOT EXISTS ${SUBJECT_DIM_TABLE} (
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  name TEXT NOT NULL,
  localized_name TEXT,
  cover TEXT,
  release_year INTEGER,
  genres TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, subject_id)
);
CREATE INDEX IF NOT EXISTS ${SUBJECT_DIM_TABLE}_subject_idx
ON ${SUBJECT_DIM_TABLE} (subject_id);
CREATE TABLE IF NOT EXISTS ${SUBJECT_GENRE_DIM_TABLE} (
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  genre TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, subject_id, genre)
);
CREATE INDEX IF NOT EXISTS ${SUBJECT_GENRE_DIM_TABLE}_kind_genre_idx
ON ${SUBJECT_GENRE_DIM_TABLE} (kind, genre, subject_id);
CREATE TABLE IF NOT EXISTS ${SHARE_SUBJECT_SLOT_TABLE} (
  share_id TEXT NOT NULL REFERENCES ${SHARES_V2_TABLE}(share_id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  slot_index INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  day_key INTEGER NOT NULL,
  hour_bucket INTEGER NOT NULL,
  PRIMARY KEY (share_id, slot_index)
);
CREATE INDEX IF NOT EXISTS ${SHARE_SUBJECT_SLOT_TABLE}_kind_created_idx
ON ${SHARE_SUBJECT_SLOT_TABLE} (kind, created_at DESC);
CREATE INDEX IF NOT EXISTS ${SHARE_SUBJECT_SLOT_TABLE}_kind_day_subject_idx
ON ${SHARE_SUBJECT_SLOT_TABLE} (kind, day_key, subject_id);
CREATE INDEX IF NOT EXISTS ${SHARE_SUBJECT_SLOT_TABLE}_kind_hour_subject_idx
ON ${SHARE_SUBJECT_SLOT_TABLE} (kind, hour_bucket, subject_id);
CREATE TABLE IF NOT EXISTS ${TREND_COUNT_ALL_TABLE} (
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, subject_id)
);
CREATE INDEX IF NOT EXISTS ${TREND_COUNT_ALL_TABLE}_kind_count_idx
ON ${TREND_COUNT_ALL_TABLE} (kind, count DESC, subject_id);
CREATE TABLE IF NOT EXISTS ${TREND_COUNT_DAY_TABLE} (
  kind TEXT NOT NULL,
  day_key INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, day_key, subject_id)
);
CREATE INDEX IF NOT EXISTS ${TREND_COUNT_DAY_TABLE}_kind_day_count_idx
ON ${TREND_COUNT_DAY_TABLE} (kind, day_key, count DESC, subject_id);
CREATE TABLE IF NOT EXISTS ${TREND_COUNT_HOUR_TABLE} (
  kind TEXT NOT NULL,
  hour_bucket INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, hour_bucket, subject_id)
);
CREATE INDEX IF NOT EXISTS ${TREND_COUNT_HOUR_TABLE}_kind_hour_count_idx
ON ${TREND_COUNT_HOUR_TABLE} (kind, hour_bucket, count DESC, subject_id);
CREATE TABLE IF NOT EXISTS ${TRENDS_CACHE_TABLE} (
  cache_key TEXT PRIMARY KEY,
  period TEXT NOT NULL,
  view TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ${TRENDS_CACHE_TABLE}_expires_idx
ON ${TRENDS_CACHE_TABLE} (expires_at);
CREATE TABLE IF NOT EXISTS ${SHARE_VIEW_TOTAL_TABLE} (
  share_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  view_count INTEGER NOT NULL,
  last_aggregated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ${SHARE_VIEW_TOTAL_TABLE}_kind_count_idx
ON ${SHARE_VIEW_TOTAL_TABLE} (kind, view_count DESC, share_id);
`;

type LocalPlatformEnv = {
  MY9_DB?: D1DatabaseLike;
};

type GlobalRuntimeWithEnv = typeof globalThis & {
  __MY9_CF_ENV?: LocalPlatformEnv;
};

let localPlatformPromise: Promise<LocalPlatformEnv | null> | null = null;
let d1DatabasePromise: Promise<D1DatabaseLike | null> | null = null;
let d1SchemaReadyPromise: Promise<boolean> | null = null;

async function dynamicRuntimeImport<T = unknown>(specifier: string): Promise<T> {
  const importer = new Function("s", "return import(s);") as (s: string) => Promise<T>;
  return importer(specifier);
}

async function getCloudflareBoundD1(): Promise<D1DatabaseLike | null> {
  const globalEnv = (globalThis as GlobalRuntimeWithEnv).__MY9_CF_ENV;
  const globalDb = globalEnv?.MY9_DB;
  if (globalDb && typeof globalDb.prepare === "function") {
    return globalDb;
  }

  try {
    const { getCloudflareContext } = await import("@opennextjs/cloudflare");
    const context = (await getCloudflareContext({ async: true })) as { env?: LocalPlatformEnv };
    const db = context?.env?.MY9_DB;
    return db && typeof db.prepare === "function" ? db : null;
  } catch {
    return null;
  }
}

async function getLocalPlatformEnv(): Promise<LocalPlatformEnv | null> {
  if (!localPlatformPromise) {
    localPlatformPromise = (async () => {
      try {
        const { getPlatformProxy } = await dynamicRuntimeImport<typeof import("wrangler")>("wrangler");
        const environment = readEnv("MY9_DB_WRANGLER_ENV", "NEXT_DEV_WRANGLER_ENV") ?? undefined;
        const platform = await getPlatformProxy<LocalPlatformEnv>({
          configPath: path.resolve(process.cwd(), "wrangler.jsonc"),
          environment,
          persist: true,
          remoteBindings: false,
        });
        return platform.env ?? null;
      } catch {
        return null;
      }
    })();
  }
  return localPlatformPromise;
}

export async function getD1Database(): Promise<D1DatabaseLike | null> {
  if (!d1DatabasePromise) {
    d1DatabasePromise = (async () => {
      const bound = await getCloudflareBoundD1();
      if (bound) return bound;

      const localEnv = await getLocalPlatformEnv();
      const localDb = localEnv?.MY9_DB;
      return localDb && typeof localDb.prepare === "function" ? localDb : null;
    })();
  }
  return d1DatabasePromise;
}

export async function ensureD1Schema(): Promise<boolean> {
  const db = await getD1Database();
  if (!db) {
    return false;
  }

  if ((globalThis as GlobalRuntimeWithEnv).__MY9_CF_ENV?.MY9_DB) {
    return true;
  }

  if (!d1SchemaReadyPromise) {
    d1SchemaReadyPromise = (async () => {
      try {
        await db.exec(D1_SCHEMA_SQL);
        return true;
      } catch {
        d1SchemaReadyPromise = null;
        return false;
      }
    })();
  }

  return d1SchemaReadyPromise;
}

export async function isD1RuntimeAvailable(): Promise<boolean> {
  return (await getD1Database()) !== null;
}

export function buildPlaceholders(count: number): string {
  return Array.from({ length: count }, () => "?").join(", ");
}

export async function queryAll<T = Record<string, unknown>>(
  db: D1DatabaseLike,
  sql: string,
  params: D1Scalar[] = []
): Promise<T[]> {
  const prepared = db.prepare(sql).bind(...params.map((value) => (value === undefined ? null : value)));
  const result = await prepared.all<T>();
  if (Array.isArray(result)) {
    return result;
  }
  return Array.isArray(result.results) ? result.results : [];
}

export async function queryFirst<T = Record<string, unknown>>(
  db: D1DatabaseLike,
  sql: string,
  params: D1Scalar[] = []
): Promise<T | null> {
  const rows = await queryAll<T>(db, sql, params);
  return rows[0] ?? null;
}

export function readChangeCount(result: unknown): number {
  if (!result || typeof result !== "object") return 0;
  const record = result as { meta?: { changes?: number }; changes?: number };
  if (typeof record.meta?.changes === "number") return Math.trunc(record.meta.changes);
  if (typeof record.changes === "number") return Math.trunc(record.changes);
  return 0;
}

export async function execute(db: D1DatabaseLike, sql: string, params: D1Scalar[] = []): Promise<number> {
  const result = await db.prepare(sql).bind(...params.map((value) => (value === undefined ? null : value))).run();
  return readChangeCount(result);
}

export async function executeBatch(db: D1DatabaseLike, statements: StatementInput[]): Promise<number> {
  let changes = 0;
  for (const chunk of chunkArray(statements, MAX_BATCH_SIZE)) {
    const result = await db.batch(chunk.map((statement) => db.prepare(statement.sql).bind(...(statement.params ?? []))));
    for (const item of result) {
      changes += readChangeCount(item);
    }
  }
  return changes;
}
