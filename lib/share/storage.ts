import { neon } from "@neondatabase/serverless";
import {
  CompactSharePayload,
  SubjectSnapshot,
  compactPayloadToGames,
  createContentHash,
  normalizeCompactPayload,
  toCompactSharePayload,
} from "@/lib/share/compact";
import { getColdSharePayload, buildColdObjectKey, isColdStorageEnabled, putColdSharePayload } from "@/lib/share/cold-storage";
import {
  ShareSubject,
  StoredShareV1,
  TrendBucket,
  TrendGameItem,
  TrendPeriod,
  TrendResponse,
  TrendView,
  TrendYearPage,
} from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";

const TRENDS_CACHE_PREFIX = "trends:cache:";
const TRENDS_SAMPLE_CACHE_PREFIX = "trends:sample:";

const SHARES_V1_TABLE = "my9_shares_v1";
const SHARES_V2_TABLE = "my9_share_registry_v2";
const SHARE_ALIAS_TABLE = "my9_share_alias_v1";
const SUBJECT_DIM_TABLE = "my9_subject_dim_v1";
const TREND_COUNT_ALL_TABLE = "my9_trend_subject_kind_all_v3";
const TREND_COUNT_DAY_TABLE = "my9_trend_subject_kind_day_v3";
const TREND_COUNT_HOUR_TABLE = "my9_trend_subject_kind_hour_v3";
const TRENDS_CACHE_TABLE = "my9_trends_cache_v1";
const TRENDS_CACHE_VERSION = "v9";
const TRENDS_SAMPLE_CACHE_VERSION = "v5";
const SAMPLE_SUMMARY_CACHE_VIEW = "sample";
const OVERALL_TREND_PAGE_SIZE = 20;
const GROUPED_BUCKET_LIMIT = 20;
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const HALF_HOUR_MS = 30 * 60 * 1000;
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;
const TREND_CACHE_COMPAT_TTL_MS = 60 * 60 * 1000;
const SHARES_V2_KIND_CREATED_IDX = `${SHARES_V2_TABLE}_kind_created_idx`;
const SHARES_V2_TIER_CREATED_IDX = `${SHARES_V2_TABLE}_tier_created_idx`;
const SHARE_ALIAS_TARGET_IDX = `${SHARE_ALIAS_TABLE}_target_idx`;
const SUBJECT_DIM_SUBJECT_IDX = `${SUBJECT_DIM_TABLE}_subject_idx`;
const TREND_COUNT_ALL_KIND_COUNT_IDX = `${TREND_COUNT_ALL_TABLE}_kind_count_idx`;
const TRENDS_CACHE_EXPIRES_IDX = `${TRENDS_CACHE_TABLE}_expires_idx`;

function readEnv(...names: string[]): string | null {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function buildDatabaseUrlFromNeonParts(): string | null {
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

const DATABASE_URL = buildDatabaseUrlFromNeonParts();
const DATABASE_ENABLED = Boolean(DATABASE_URL);
const MEMORY_FALLBACK_ENABLED =
  readEnv("MY9_ALLOW_MEMORY_FALLBACK") === "1" ||
  (readEnv("MY9_ALLOW_MEMORY_FALLBACK") !== "0" && process.env.NODE_ENV !== "production");
const TREND_MEMORY_CACHE_ENABLED = MEMORY_FALLBACK_ENABLED && process.env.NODE_ENV !== "production";
const V1_FALLBACK_ENABLED = readEnv("MY9_ENABLE_V1_FALLBACK") !== "0";
const TREND_24H_SOURCE = readEnv("MY9_TRENDS_24H_SOURCE") === "hour" ? "hour" : "day";

type SqlClient = ReturnType<typeof neon>;

let sqlClient: SqlClient | null = null;
let schemaReadyPromise: Promise<void> | null = null;
let schemaLastError: Error | null = null;

function getSqlClient(): SqlClient | null {
  if (!DATABASE_ENABLED) {
    return null;
  }
  if (!sqlClient) {
    sqlClient = neon(DATABASE_URL!);
  }
  return sqlClient;
}

type MemoryStore = {
  shares: Map<string, StoredShareV1>;
  hashToShareId: Map<string, string>;
  trendCache: Map<string, { value: TrendResponse; expiresAt: number; updatedAt?: number }>;
  trendSampleCache: Map<string, { value: TrendSampleSummary; expiresAt: number; updatedAt?: number }>;
};

type ShareV1Row = {
  share_id: string;
  kind: string;
  creator_name: string | null;
  games: unknown;
  created_at: number | string;
  updated_at: number | string;
  last_viewed_at: number | string;
};

type ShareRegistryRow = {
  share_id: string;
  kind: string;
  creator_name: string | null;
  storage_tier: "hot" | "cold";
  hot_payload: unknown;
  cold_object_key: string | null;
  created_at: number | string;
  updated_at: number | string;
  last_viewed_at: number | string;
};

type SubjectDimRow = {
  subject_id: string;
  name: string;
  localized_name: string | null;
  cover: string | null;
  release_year: number | null;
  genres: unknown;
};

type TrendSubjectCountRow = {
  subject_id: string;
  count: number | string;
  name: string | null;
  localized_name: string | null;
  cover: string | null;
  release_year: number | null;
  genres: unknown;
  bucket_total?: number | string | null;
};

type TrendSampleRow = {
  sample_count: number | string;
  min_created: number | string | null;
  max_created: number | string | null;
};

type ShareCountRow = {
  total_count: number | string;
};

type TrendCacheRow = {
  payload: unknown;
  expires_at: number | string;
  updated_at?: number | string | null;
};

type TrendSampleSummary = {
  sampleCount: number;
  range: {
    from: number | null;
    to: number | null;
  };
};

function throwStorageError(context: string, cause?: unknown): never {
  if (cause instanceof Error) {
    throw new Error(`${context}: ${cause.message}`);
  }
  throw new Error(context);
}

function throwDatabaseNotReady(context: string): never {
  if (schemaLastError) {
    throwStorageError(context, schemaLastError);
  }
  throwStorageError(`${context}: database is not ready`);
}

function normalizeStoredShare(input: StoredShareV1): StoredShareV1 {
  return {
    ...input,
    kind: parseSubjectKind(input.kind) ?? DEFAULT_SUBJECT_KIND,
  };
}

function createEmptyGames(): Array<ShareSubject | null> {
  return Array.from({ length: 9 }, () => null);
}

function normalizeGames(value: unknown): Array<ShareSubject | null> {
  if (!Array.isArray(value)) {
    return createEmptyGames();
  }

  const next = createEmptyGames();
  for (let index = 0; index < 9; index += 1) {
    const item = value[index];
    next[index] = item && typeof item === "object" ? (item as ShareSubject) : null;
  }
  return next;
}

function toNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return Math.trunc(parsed);
    }
  }
  return fallback;
}

function rowToStoredShare(row: ShareV1Row): StoredShareV1 {
  return normalizeStoredShare({
    shareId: String(row.share_id),
    kind: (parseSubjectKind(row.kind) ?? DEFAULT_SUBJECT_KIND) as SubjectKind,
    creatorName: typeof row.creator_name === "string" ? row.creator_name : null,
    games: normalizeGames(row.games),
    createdAt: toNumber(row.created_at, Date.now()),
    updatedAt: toNumber(row.updated_at, Date.now()),
    lastViewedAt: toNumber(row.last_viewed_at, Date.now()),
  });
}

function parseTrendPayload(value: unknown): TrendResponse | null {
  if (!value || typeof value !== "object") return null;
  const data = value as Partial<TrendResponse>;
  if (!data.period || !data.view || !data.range || !Array.isArray(data.items)) {
    return null;
  }

  return {
    period: data.period,
    view: data.view,
    sampleCount: typeof data.sampleCount === "number" ? data.sampleCount : 0,
    range: {
      from: typeof data.range.from === "number" ? data.range.from : null,
      to: typeof data.range.to === "number" ? data.range.to : null,
    },
    lastUpdatedAt: typeof data.lastUpdatedAt === "number" ? data.lastUpdatedAt : Date.now(),
    items: data.items,
  };
}

function parseTrendSampleSummaryPayload(value: unknown): TrendSampleSummary | null {
  if (!value || typeof value !== "object") return null;
  const data = value as Partial<TrendSampleSummary>;
  if (!data.range || typeof data.range !== "object") {
    return null;
  }
  return {
    sampleCount: typeof data.sampleCount === "number" ? data.sampleCount : 0,
    range: {
      from: typeof data.range.from === "number" ? data.range.from : null,
      to: typeof data.range.to === "number" ? data.range.to : null,
    },
  };
}

function getMemoryStore(): MemoryStore {
  const g = globalThis as typeof globalThis & {
    __MY9_SHARE_MEMORY__?: MemoryStore;
  };

  if (!g.__MY9_SHARE_MEMORY__) {
    g.__MY9_SHARE_MEMORY__ = {
      shares: new Map<string, StoredShareV1>(),
      hashToShareId: new Map<string, string>(),
      trendCache: new Map<string, { value: TrendResponse; expiresAt: number; updatedAt?: number }>(),
      trendSampleCache: new Map<string, { value: TrendSampleSummary; expiresAt: number; updatedAt?: number }>(),
    };
  }
  if (!g.__MY9_SHARE_MEMORY__.trendSampleCache) {
    g.__MY9_SHARE_MEMORY__.trendSampleCache = new Map<
      string,
      { value: TrendSampleSummary; expiresAt: number; updatedAt?: number }
    >();
  }
  return g.__MY9_SHARE_MEMORY__;
}

function trendCacheKey(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  overallPage: number,
  yearPage: TrendYearPage
) {
  const sourcePart = period === "24h" ? `:src${TREND_24H_SOURCE}` : "";
  return `${TRENDS_CACHE_PREFIX}${TRENDS_CACHE_VERSION}:${period}:${view}:${kind}${sourcePart}:op${overallPage}:yp${yearPage}`;
}

function trendSampleCacheKey(period: TrendPeriod, kind: SubjectKind) {
  const sourcePart = period === "24h" ? `:src${TREND_24H_SOURCE}` : "";
  return `${TRENDS_SAMPLE_CACHE_PREFIX}${TRENDS_SAMPLE_CACHE_VERSION}:${period}:${kind}${sourcePart}`;
}

async function ensureSchema(): Promise<boolean> {
  const sql = getSqlClient();
  if (!sql) {
    return false;
  }

  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(SHARES_V2_TABLE)} (
          share_id TEXT PRIMARY KEY,
          kind TEXT NOT NULL,
          creator_name TEXT,
          content_hash TEXT NOT NULL UNIQUE,
          storage_tier TEXT NOT NULL DEFAULT 'hot',
          hot_payload JSONB,
          cold_object_key TEXT,
          created_at BIGINT NOT NULL,
          updated_at BIGINT NOT NULL,
          last_viewed_at BIGINT NOT NULL,
          CHECK (storage_tier IN ('hot', 'cold'))
        )
      `;
      await sql`
        CREATE INDEX IF NOT EXISTS ${sql.unsafe(SHARES_V2_KIND_CREATED_IDX)}
        ON ${sql.unsafe(SHARES_V2_TABLE)} (kind, created_at DESC)
      `;
      await sql`
        CREATE INDEX IF NOT EXISTS ${sql.unsafe(SHARES_V2_TIER_CREATED_IDX)}
        ON ${sql.unsafe(SHARES_V2_TABLE)} (storage_tier, created_at)
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(SHARE_ALIAS_TABLE)} (
          share_id TEXT PRIMARY KEY,
          target_share_id TEXT NOT NULL REFERENCES ${sql.unsafe(SHARES_V2_TABLE)}(share_id) ON DELETE CASCADE,
          created_at BIGINT NOT NULL
        )
      `;
      await sql`
        CREATE INDEX IF NOT EXISTS ${sql.unsafe(SHARE_ALIAS_TARGET_IDX)}
        ON ${sql.unsafe(SHARE_ALIAS_TABLE)} (target_share_id)
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(SUBJECT_DIM_TABLE)} (
          kind TEXT NOT NULL,
          subject_id TEXT NOT NULL,
          name TEXT NOT NULL,
          localized_name TEXT,
          cover TEXT,
          release_year INT,
          genres JSONB,
          updated_at BIGINT NOT NULL,
          PRIMARY KEY (kind, subject_id)
        )
      `;
      await sql`
        CREATE INDEX IF NOT EXISTS ${sql.unsafe(SUBJECT_DIM_SUBJECT_IDX)}
        ON ${sql.unsafe(SUBJECT_DIM_TABLE)} (subject_id)
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(TREND_COUNT_ALL_TABLE)} (
          kind TEXT NOT NULL,
          subject_id TEXT NOT NULL,
          count BIGINT NOT NULL,
          updated_at BIGINT NOT NULL,
          PRIMARY KEY (kind, subject_id)
        )
      `;
      await sql`
        CREATE INDEX IF NOT EXISTS ${sql.unsafe(TREND_COUNT_ALL_KIND_COUNT_IDX)}
        ON ${sql.unsafe(TREND_COUNT_ALL_TABLE)} (kind, count DESC, subject_id)
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(TREND_COUNT_DAY_TABLE)} (
          kind TEXT NOT NULL,
          day_key INT NOT NULL,
          subject_id TEXT NOT NULL,
          count BIGINT NOT NULL,
          updated_at BIGINT NOT NULL,
          PRIMARY KEY (kind, day_key, subject_id)
        )
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(TREND_COUNT_HOUR_TABLE)} (
          kind TEXT NOT NULL,
          hour_bucket BIGINT NOT NULL,
          subject_id TEXT NOT NULL,
          count BIGINT NOT NULL,
          updated_at BIGINT NOT NULL,
          PRIMARY KEY (kind, hour_bucket, subject_id)
        )
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS ${sql.unsafe(TRENDS_CACHE_TABLE)} (
          cache_key TEXT PRIMARY KEY,
          period TEXT NOT NULL,
          view TEXT NOT NULL,
          kind TEXT NOT NULL,
          payload JSONB NOT NULL,
          expires_at BIGINT NOT NULL,
          updated_at BIGINT NOT NULL
        )
      `;
      await sql`
        CREATE INDEX IF NOT EXISTS ${sql.unsafe(TRENDS_CACHE_EXPIRES_IDX)}
        ON ${sql.unsafe(TRENDS_CACHE_TABLE)} (expires_at)
      `;
    })();
  }

  try {
    await schemaReadyPromise;
    schemaLastError = null;
    return true;
  } catch (error) {
    schemaReadyPromise = null;
    schemaLastError =
      error instanceof Error ? error : new Error(typeof error === "string" ? error : "schema init failed");
    return false;
  }
}

function toBeijingHourBucket(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS);
}

function toBeijingTrendRefreshBucket(timestampMs: number): number {
  // Shift +30m so refresh boundaries move from xx:00 to xx:30 (Beijing time).
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS + HALF_HOUR_MS) / HOUR_MS);
}

function resolveTrendCacheUpdatedAt(expiresAt: number, updatedAt: unknown): number {
  if (typeof updatedAt === "number" && Number.isFinite(updatedAt) && updatedAt > 0) {
    return Math.trunc(updatedAt);
  }
  const inferred = expiresAt - TREND_CACHE_COMPAT_TTL_MS;
  if (Number.isFinite(inferred) && inferred > 0) {
    return Math.trunc(inferred);
  }
  return Date.now();
}

function isTrendCacheExpired(expiresAt: number, updatedAt: number, nowMs: number): boolean {
  if (nowMs > expiresAt) {
    return true;
  }
  return toBeijingTrendRefreshBucket(nowMs) !== toBeijingTrendRefreshBucket(updatedAt);
}

function getMemoryTrendCache(key: string, allowExpired = false): TrendResponse | null {
  const store = getMemoryStore();
  const item = store.trendCache.get(key);
  if (!item) return null;
  const nowMs = Date.now();
  const updatedAt = resolveTrendCacheUpdatedAt(item.expiresAt, item.updatedAt);
  if (isTrendCacheExpired(item.expiresAt, updatedAt, nowMs)) {
    if (!allowExpired) {
      store.trendCache.delete(key);
      return null;
    }
  }
  if (updatedAt !== item.updatedAt) {
    store.trendCache.set(key, { ...item, updatedAt });
  }
  return item.value;
}

function getMemoryTrendSampleSummaryCache(key: string, allowExpired = false): TrendSampleSummary | null {
  const store = getMemoryStore();
  const item = store.trendSampleCache.get(key);
  if (!item) return null;
  const nowMs = Date.now();
  const updatedAt = resolveTrendCacheUpdatedAt(item.expiresAt, item.updatedAt);
  if (isTrendCacheExpired(item.expiresAt, updatedAt, nowMs)) {
    if (!allowExpired) {
      store.trendSampleCache.delete(key);
      return null;
    }
  }
  if (updatedAt !== item.updatedAt) {
    store.trendSampleCache.set(key, { ...item, updatedAt });
  }
  return item.value;
}

function toBeijingDayKey(timestampMs: number): number {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

function getBeijingDayStart(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / DAY_MS) * DAY_MS - BEIJING_TZ_OFFSET_MS;
}

function getBeijingHourStart(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS) * HOUR_MS - BEIJING_TZ_OFFSET_MS;
}

function toSubjectSnapshot(row: SubjectDimRow): SubjectSnapshot {
  const genres = Array.isArray(row.genres)
    ? row.genres
        .map((item) => (typeof item === "string" ? item.trim() : ""))
        .filter((item) => Boolean(item))
    : undefined;

  return {
    subjectId: row.subject_id,
    name: row.name,
    localizedName: row.localized_name || undefined,
    cover: row.cover,
    releaseYear:
      typeof row.release_year === "number" && Number.isFinite(row.release_year)
        ? Math.trunc(row.release_year)
        : undefined,
    genres: genres && genres.length > 0 ? genres : undefined,
  };
}

async function fetchSubjectSnapshots(
  sql: SqlClient,
  kind: SubjectKind,
  subjectIds: string[]
): Promise<Map<string, SubjectSnapshot>> {
  const map = new Map<string, SubjectSnapshot>();
  if (subjectIds.length === 0) {
    return map;
  }

  const rows = (await sql.query(
    `
      SELECT subject_id, name, localized_name, cover, release_year, genres
      FROM ${SUBJECT_DIM_TABLE}
      WHERE kind = $1
        AND subject_id = ANY($2)
    `,
    [kind, subjectIds]
  )) as SubjectDimRow[];

  for (const row of rows) {
    map.set(row.subject_id, toSubjectSnapshot(row));
  }
  return map;
}

function collectSubjectIdsFromPayload(payload: CompactSharePayload): string[] {
  const unique = new Set<string>();
  for (const slot of payload) {
    if (!slot) continue;
    unique.add(slot.sid);
  }
  return Array.from(unique);
}

async function resolveCompactPayload(row: ShareRegistryRow): Promise<CompactSharePayload | null> {
  const hotPayload = normalizeCompactPayload(row.hot_payload);
  if (hotPayload) {
    return hotPayload;
  }

  if (row.storage_tier === "cold" && row.cold_object_key) {
    return getColdSharePayload(row.cold_object_key);
  }

  return null;
}

async function inflateShareFromRegistryRow(
  sql: SqlClient,
  row: ShareRegistryRow
): Promise<StoredShareV1 | null> {
  const kind = (parseSubjectKind(row.kind) ?? DEFAULT_SUBJECT_KIND) as SubjectKind;
  const payload = await resolveCompactPayload(row);
  if (!payload) {
    return null;
  }

  const subjectIds = collectSubjectIdsFromPayload(payload);
  const subjectSnapshots = await fetchSubjectSnapshots(sql, kind, subjectIds);
  const games = compactPayloadToGames({
    payload,
    subjectSnapshots,
  });

  return normalizeStoredShare({
    shareId: String(row.share_id),
    kind,
    creatorName: typeof row.creator_name === "string" ? row.creator_name : null,
    games,
    createdAt: toNumber(row.created_at, Date.now()),
    updatedAt: toNumber(row.updated_at, Date.now()),
    lastViewedAt: toNumber(row.last_viewed_at, Date.now()),
  });
}

async function tryGetShareFromV1(sql: SqlClient, shareId: string): Promise<StoredShareV1 | null> {
  if (!V1_FALLBACK_ENABLED) {
    return null;
  }

  try {
    const rows = (await sql.query(
      `
      SELECT share_id, kind, creator_name, games, created_at, updated_at, last_viewed_at
      FROM ${SHARES_V1_TABLE}
      WHERE share_id = $1
      LIMIT 1
      `,
      [shareId]
    )) as ShareV1Row[];

    if (rows.length === 0) {
      return null;
    }
    return rowToStoredShare(rows[0]);
  } catch {
    return null;
  }
}

async function tryListSharesFromV1(sql: SqlClient, from?: number): Promise<StoredShareV1[]> {
  if (!V1_FALLBACK_ENABLED) {
    return [];
  }

  try {
    if (typeof from === "number" && from > 0) {
      const rows = (await sql.query(
        `
        SELECT share_id, kind, creator_name, games, created_at, updated_at, last_viewed_at
        FROM ${SHARES_V1_TABLE}
        WHERE created_at >= $1
        `,
        [from]
      )) as ShareV1Row[];
      return rows.map((row) => rowToStoredShare(row));
    }

    const rows = (await sql.query(
      `
      SELECT share_id, kind, creator_name, games, created_at, updated_at, last_viewed_at
      FROM ${SHARES_V1_TABLE}
      `
    )) as ShareV1Row[];
    return rows.map((row) => rowToStoredShare(row));
  } catch {
    return [];
  }
}

async function tryCountSharesFromV1(sql: SqlClient): Promise<number | null> {
  if (!V1_FALLBACK_ENABLED) {
    return null;
  }

  try {
    const rows = (await sql.query(
      `
      SELECT COUNT(*)::BIGINT AS total_count
      FROM ${SHARES_V1_TABLE}
      `
    )) as ShareCountRow[];
    return toNumber(rows[0]?.total_count, 0);
  } catch {
    return null;
  }
}

type TrendIncrement = {
  dayKey: number;
  hourBucket: number;
  subjectId: string;
  count: number;
};

function buildTrendIncrements(params: {
  payload: CompactSharePayload;
  createdAt: number;
}): TrendIncrement[] {
  const dayKey = toBeijingDayKey(params.createdAt);
  const hourBucket = toBeijingHourBucket(params.createdAt);
  const countBySubject = new Map<string, number>();

  for (const slot of params.payload) {
    if (!slot) continue;
    countBySubject.set(slot.sid, (countBySubject.get(slot.sid) ?? 0) + 1);
  }

  return Array.from(countBySubject.entries()).map(([subjectId, count]) => ({
    dayKey,
    hourBucket,
    subjectId,
    count,
  }));
}

export async function saveShare(record: StoredShareV1): Promise<{ shareId: string; deduped: boolean }> {
  const normalizedRecord = normalizeStoredShare(record);
  const { payload, subjectSnapshots } = toCompactSharePayload(normalizedRecord.games);
  const contentHash = createContentHash({
    kind: normalizedRecord.kind,
    creatorName: normalizedRecord.creatorName,
    payload,
  });

  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwDatabaseNotReady("saveShare failed");
    }
    const memory = getMemoryStore();
    const dedupedShareId = memory.hashToShareId.get(contentHash);
    if (dedupedShareId) {
      return {
        shareId: dedupedShareId,
        deduped: true,
      };
    }
    memory.shares.set(normalizedRecord.shareId, normalizedRecord);
    memory.hashToShareId.set(contentHash, normalizedRecord.shareId);
    return {
      shareId: normalizedRecord.shareId,
      deduped: false,
    };
  }

  try {
    const increments = buildTrendIncrements({
      payload,
      createdAt: normalizedRecord.createdAt,
    });

    const subjectRowsPayload = Array.from(subjectSnapshots.values()).map((snapshot) => ({
      subject_id: snapshot.subjectId,
      name: snapshot.name,
      localized_name: snapshot.localizedName ?? null,
      cover: snapshot.cover,
      release_year: snapshot.releaseYear ?? null,
      genres: snapshot.genres ?? null,
    }));

    const incrementRowsPayload = increments.map((item) => ({
      day_key: item.dayKey,
      hour_bucket: item.hourBucket,
      subject_id: item.subjectId,
      count: item.count,
    }));

    const rows = (await sql.query(
      `
      WITH upsert_share AS (
        INSERT INTO ${SHARES_V2_TABLE} (
          share_id, kind, creator_name, content_hash, storage_tier, hot_payload, cold_object_key,
          created_at, updated_at, last_viewed_at
        )
        VALUES ($1, $2, $3, $4, 'hot', $5::jsonb, NULL, $6, $7, $8)
        ON CONFLICT (content_hash) DO UPDATE
        SET
          updated_at = GREATEST(${SHARES_V2_TABLE}.updated_at, EXCLUDED.updated_at),
          last_viewed_at = GREATEST(${SHARES_V2_TABLE}.last_viewed_at, EXCLUDED.last_viewed_at)
        RETURNING share_id, (xmax = 0) AS inserted
      ),
      subject_rows AS (
        SELECT
          $2::text AS kind,
          s.subject_id,
          s.name,
          s.localized_name,
          s.cover,
          s.release_year,
          s.genres,
          $7::bigint AS updated_at
        FROM jsonb_to_recordset(COALESCE($9::jsonb, '[]'::jsonb)) AS s(
          subject_id text,
          name text,
          localized_name text,
          cover text,
          release_year int,
          genres jsonb
        )
        CROSS JOIN upsert_share
        WHERE upsert_share.inserted
      ),
      subject_upsert AS (
        INSERT INTO ${SUBJECT_DIM_TABLE} (
          kind, subject_id, name, localized_name, cover, release_year, genres, updated_at
        )
        SELECT
          kind, subject_id, name, localized_name, cover, release_year, genres, updated_at
        FROM subject_rows
        ON CONFLICT (kind, subject_id) DO UPDATE SET
          name = EXCLUDED.name,
          localized_name = COALESCE(EXCLUDED.localized_name, ${SUBJECT_DIM_TABLE}.localized_name),
          cover = COALESCE(EXCLUDED.cover, ${SUBJECT_DIM_TABLE}.cover),
          release_year = COALESCE(EXCLUDED.release_year, ${SUBJECT_DIM_TABLE}.release_year),
          genres = COALESCE(EXCLUDED.genres, ${SUBJECT_DIM_TABLE}.genres),
          updated_at = EXCLUDED.updated_at
        RETURNING 1
      ),
      increment_rows AS (
        SELECT
          $2::text AS kind,
          i.day_key,
          i.hour_bucket,
          i.subject_id,
          i.count,
          $7::bigint AS updated_at
        FROM jsonb_to_recordset(COALESCE($10::jsonb, '[]'::jsonb)) AS i(
          day_key int,
          hour_bucket bigint,
          subject_id text,
          count bigint
        )
        CROSS JOIN upsert_share
        WHERE upsert_share.inserted
      ),
      trend_all_upsert AS (
        INSERT INTO ${TREND_COUNT_ALL_TABLE} (kind, subject_id, count, updated_at)
        SELECT kind, subject_id, count, updated_at
        FROM increment_rows
        ON CONFLICT (kind, subject_id) DO UPDATE SET
          count = ${TREND_COUNT_ALL_TABLE}.count + EXCLUDED.count,
          updated_at = EXCLUDED.updated_at
        RETURNING 1
      ),
      trend_day_upsert AS (
        INSERT INTO ${TREND_COUNT_DAY_TABLE} (kind, day_key, subject_id, count, updated_at)
        SELECT kind, day_key, subject_id, count, updated_at
        FROM increment_rows
        ON CONFLICT (kind, day_key, subject_id) DO UPDATE SET
          count = ${TREND_COUNT_DAY_TABLE}.count + EXCLUDED.count,
          updated_at = EXCLUDED.updated_at
        RETURNING 1
      ),
      trend_hour_upsert AS (
        INSERT INTO ${TREND_COUNT_HOUR_TABLE} (kind, hour_bucket, subject_id, count, updated_at)
        SELECT kind, hour_bucket, subject_id, count, updated_at
        FROM increment_rows
        ON CONFLICT (kind, hour_bucket, subject_id) DO UPDATE SET
          count = ${TREND_COUNT_HOUR_TABLE}.count + EXCLUDED.count,
          updated_at = EXCLUDED.updated_at
        RETURNING 1
      )
      SELECT share_id, inserted
      FROM upsert_share
      `,
      [
        normalizedRecord.shareId,
        normalizedRecord.kind,
        normalizedRecord.creatorName,
        contentHash,
        JSON.stringify(payload),
        normalizedRecord.createdAt,
        normalizedRecord.updatedAt,
        normalizedRecord.lastViewedAt,
        JSON.stringify(subjectRowsPayload),
        JSON.stringify(incrementRowsPayload),
      ]
    )) as Array<{ share_id: string; inserted: boolean }>;

    const storedShareId = rows[0]?.share_id;
    const inserted = Boolean(rows[0]?.inserted);
    if (!storedShareId) {
      throw new Error("saveShare failed: empty upsert result");
    }

    if (!inserted) {
      return {
        shareId: storedShareId,
        deduped: true,
      };
    }

    return {
      shareId: normalizedRecord.shareId,
      deduped: false,
    };
  } catch (error) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwStorageError("saveShare failed: database write error", error);
    }
    const memory = getMemoryStore();
    const dedupedShareId = memory.hashToShareId.get(contentHash);
    if (dedupedShareId) {
      return {
        shareId: dedupedShareId,
        deduped: true,
      };
    }
    memory.shares.set(normalizedRecord.shareId, normalizedRecord);
    memory.hashToShareId.set(contentHash, normalizedRecord.shareId);
    return {
      shareId: normalizedRecord.shareId,
      deduped: false,
    };
  }
}

export async function getShare(shareId: string): Promise<StoredShareV1 | null> {
  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwDatabaseNotReady("getShare failed");
    }
  } else {
    try {
      const rows = (await sql.query(
        `
        SELECT share_id, kind, creator_name, storage_tier, hot_payload, cold_object_key, created_at, updated_at, last_viewed_at
        FROM ${SHARES_V2_TABLE}
        WHERE share_id = $1
        LIMIT 1
        `,
        [shareId]
      )) as ShareRegistryRow[];
      if (rows.length > 0) {
        const inflated = await inflateShareFromRegistryRow(sql, rows[0]);
        if (inflated) {
          return inflated;
        }
      }

      const aliasRows = (await sql.query(
        `
        SELECT target_share_id
        FROM ${SHARE_ALIAS_TABLE}
        WHERE share_id = $1
        LIMIT 1
        `,
        [shareId]
      )) as Array<{ target_share_id: string }>;
      const aliasTarget = aliasRows[0]?.target_share_id;

      if (aliasTarget) {
        const targetRows = (await sql.query(
          `
          SELECT share_id, kind, creator_name, storage_tier, hot_payload, cold_object_key, created_at, updated_at, last_viewed_at
          FROM ${SHARES_V2_TABLE}
          WHERE share_id = $1
          LIMIT 1
          `,
          [aliasTarget]
        )) as ShareRegistryRow[];
        if (targetRows.length > 0) {
          const inflated = await inflateShareFromRegistryRow(sql, targetRows[0]);
          if (inflated) {
            return {
              ...inflated,
              shareId,
            };
          }
        }
      }

      const legacyShare = await tryGetShareFromV1(sql, shareId);
      if (legacyShare) {
        return legacyShare;
      }
    } catch (error) {
      if (!MEMORY_FALLBACK_ENABLED) {
        throwStorageError("getShare failed: database read error", error);
      }
    }
  }

  if (!MEMORY_FALLBACK_ENABLED) {
    return null;
  }

  const fromMemory = getMemoryStore().shares.get(shareId);
  return fromMemory ? normalizeStoredShare(fromMemory) : null;
}

export async function touchShare(shareId: string, now = Date.now()): Promise<boolean> {
  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwDatabaseNotReady("touchShare failed");
    }
  } else {
    try {
      const rows = (await sql.query(
        `
        WITH resolved AS (
          SELECT COALESCE(
            (SELECT target_share_id FROM ${SHARE_ALIAS_TABLE} WHERE share_id = $1),
            $1
          ) AS resolved_id
        )
        UPDATE ${SHARES_V2_TABLE}
        SET
          updated_at = $2,
          last_viewed_at = $2
        WHERE share_id = (SELECT resolved_id FROM resolved)
        RETURNING share_id
        `,
        [shareId, now]
      )) as Array<{ share_id: string }>;
      if (rows.length > 0) {
        return true;
      }

      if (V1_FALLBACK_ENABLED) {
        const legacyRows = (await sql.query(
          `
          UPDATE ${SHARES_V1_TABLE}
          SET
            updated_at = $2,
            last_viewed_at = $2
          WHERE share_id = $1
          RETURNING share_id
          `,
          [shareId, now]
        )) as Array<{ share_id: string }>;
        if (legacyRows.length > 0) {
          return true;
        }
      }
    } catch (error) {
      if (!MEMORY_FALLBACK_ENABLED) {
        throwStorageError("touchShare failed: database write error", error);
      }
    }
  }

  if (!MEMORY_FALLBACK_ENABLED) {
    return false;
  }

  const existing = getMemoryStore().shares.get(shareId);
  if (!existing) return false;
  getMemoryStore().shares.set(shareId, {
    ...normalizeStoredShare(existing),
    updatedAt: now,
    lastViewedAt: now,
  });
  return true;
}

export async function listAllShares(): Promise<StoredShareV1[]> {
  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwDatabaseNotReady("listAllShares failed");
    }
  } else {
    try {
      const rows = (await sql.query(
        `
        SELECT share_id, kind, creator_name, storage_tier, hot_payload, cold_object_key, created_at, updated_at, last_viewed_at
        FROM ${SHARES_V2_TABLE}
        ORDER BY created_at DESC
        `
      )) as ShareRegistryRow[];

      if (rows.length > 0) {
        const result: StoredShareV1[] = [];
        for (const row of rows) {
          const inflated = await inflateShareFromRegistryRow(sql, row);
          if (inflated) {
            result.push(inflated);
          }
        }
        return result;
      }

      const legacy = await tryListSharesFromV1(sql);
      if (legacy.length > 0) {
        return legacy;
      }
    } catch (error) {
      if (!MEMORY_FALLBACK_ENABLED) {
        throwStorageError("listAllShares failed: database read error", error);
      }
    }
  }

  if (!MEMORY_FALLBACK_ENABLED) {
    return [];
  }

  return Array.from(getMemoryStore().shares.values()).map((item) => normalizeStoredShare(item));
}

export async function countAllShares(): Promise<number> {
  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwDatabaseNotReady("countAllShares failed");
    }
  } else {
    try {
      const rows = (await sql.query(
        `
        SELECT COUNT(*)::BIGINT AS total_count
        FROM ${SHARES_V2_TABLE}
        `
      )) as ShareCountRow[];
      const totalCount = toNumber(rows[0]?.total_count, 0);
      if (totalCount > 0) {
        return totalCount;
      }

      const legacyCount = await tryCountSharesFromV1(sql);
      if (legacyCount !== null) {
        return legacyCount;
      }
    } catch (error) {
      if (!MEMORY_FALLBACK_ENABLED) {
        throwStorageError("countAllShares failed: database read error", error);
      }
    }
  }

  if (!MEMORY_FALLBACK_ENABLED) {
    return 0;
  }

  return getMemoryStore().shares.size;
}

function getPeriodStart(period: TrendPeriod, now = Date.now()): number {
  switch (period) {
    case "today":
      return getBeijingDayStart(now);
    case "24h":
      return getBeijingHourStart(now) - 23 * HOUR_MS;
    case "7d":
      return now - 7 * DAY_MS;
    case "30d":
      return now - 30 * DAY_MS;
    case "90d":
      return now - 90 * DAY_MS;
    case "180d":
      return now - 180 * DAY_MS;
    case "all":
    default:
      return 0;
  }
}

export async function listSharesByPeriod(period: TrendPeriod): Promise<StoredShareV1[]> {
  const sql = getSqlClient();
  const from = getPeriodStart(period);

  if (!sql || !(await ensureSchema())) {
    if (!MEMORY_FALLBACK_ENABLED) {
      throwDatabaseNotReady("listSharesByPeriod failed");
    }
  } else {
    try {
      const rows =
        from > 0
          ? ((await sql.query(
              `
            SELECT share_id, kind, creator_name, storage_tier, hot_payload, cold_object_key, created_at, updated_at, last_viewed_at
            FROM ${SHARES_V2_TABLE}
            WHERE created_at >= $1
            ORDER BY created_at DESC
            `,
              [from]
            )) as ShareRegistryRow[])
          : ((await sql.query(
              `
            SELECT share_id, kind, creator_name, storage_tier, hot_payload, cold_object_key, created_at, updated_at, last_viewed_at
            FROM ${SHARES_V2_TABLE}
            ORDER BY created_at DESC
            `
            )) as ShareRegistryRow[]);

      if (rows.length > 0) {
        const result: StoredShareV1[] = [];
        for (const row of rows) {
          const inflated = await inflateShareFromRegistryRow(sql, row);
          if (inflated) {
            result.push(inflated);
          }
        }
        return result;
      }

      const legacy = await tryListSharesFromV1(sql, from > 0 ? from : undefined);
      if (legacy.length > 0) {
        return legacy;
      }
    } catch (error) {
      if (!MEMORY_FALLBACK_ENABLED) {
        throwStorageError("listSharesByPeriod failed: database read error", error);
      }
    }
  }

  if (!MEMORY_FALLBACK_ENABLED) {
    return [];
  }

  const all = Array.from(getMemoryStore().shares.values()).map((item) => normalizeStoredShare(item));
  return all.filter((item) => item.createdAt >= from);
}

function sortByCount<T extends { count: number }>(items: T[]): T[] {
  return items.sort((a, b) => b.count - a.count);
}

function createTrendGameItem(row: TrendSubjectCountRow): TrendGameItem {
  const id = row.subject_id;
  const name = row.name || id;
  return {
    id,
    name,
    localizedName: row.localized_name || undefined,
    cover: row.cover,
    releaseYear:
      typeof row.release_year === "number" && Number.isFinite(row.release_year)
        ? Math.trunc(row.release_year)
        : undefined,
    count: toNumber(row.count, 0),
  };
}

function normalizeTrendGenres(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => (typeof item === "string" ? item.trim() : ""))
    .filter((item) => Boolean(item));
}

function buildTrendItemsFromCounts(view: TrendView, rows: TrendSubjectCountRow[]): TrendBucket[] {
  const groupedTopGamesLimit = 5;
  switch (view) {
    case "genre": {
      const bucketMap = new Map<string, TrendGameItem[]>();
      const bucketTotalMap = new Map<string, number>();
      for (const row of rows) {
        const game = createTrendGameItem(row);
        const rowBucketTotal = toNumber(row.bucket_total, 0);
        const genres = normalizeTrendGenres(row.genres);
        const buckets = genres.length > 0 ? genres : ["未分类"];
        for (const bucketKey of buckets) {
          const list = bucketMap.get(bucketKey) || [];
          list.push(game);
          bucketMap.set(bucketKey, list);
          if (rowBucketTotal > 0 && !bucketTotalMap.has(bucketKey)) {
            bucketTotalMap.set(bucketKey, rowBucketTotal);
          }
        }
      }
      const buckets: TrendBucket[] = [];
      for (const [bucket, games] of Array.from(bucketMap.entries())) {
        const sortedGames = sortByCount(games).slice(0, groupedTopGamesLimit);
        const total = bucketTotalMap.get(bucket) ?? games.reduce((sum, item) => sum + item.count, 0);
        buckets.push({
          key: bucket,
          label: bucket,
          count: total,
          games: sortedGames,
        });
      }
      return sortByCount(buckets).slice(0, GROUPED_BUCKET_LIMIT);
    }
    case "decade":
    case "year": {
      const bucketMap = new Map<string, TrendGameItem[]>();
      const bucketTotalMap = new Map<string, number>();
      for (const row of rows) {
        const releaseYear =
          typeof row.release_year === "number" && Number.isFinite(row.release_year)
            ? Math.trunc(row.release_year)
            : null;
        if (!releaseYear) continue;
        const bucketKey = view === "year" ? String(releaseYear) : `${Math.floor(releaseYear / 10) * 10}s`;
        const list = bucketMap.get(bucketKey) || [];
        list.push(createTrendGameItem(row));
        bucketMap.set(bucketKey, list);
        const rowBucketTotal = toNumber(row.bucket_total, 0);
        if (rowBucketTotal > 0 && !bucketTotalMap.has(bucketKey)) {
          bucketTotalMap.set(bucketKey, rowBucketTotal);
        }
      }
      const buckets: TrendBucket[] = [];
      for (const [bucket, games] of Array.from(bucketMap.entries())) {
        const sortedGames = sortByCount(games).slice(0, groupedTopGamesLimit);
        const total = bucketTotalMap.get(bucket) ?? games.reduce((sum, item) => sum + item.count, 0);
        buckets.push({
          key: bucket,
          label: bucket,
          count: total,
          games: sortedGames,
        });
      }
      return view === "decade"
        ? buckets.sort((a, b) => Number.parseInt(b.key, 10) - Number.parseInt(a.key, 10))
        : buckets.sort((a, b) => Number(b.key) - Number(a.key));
    }
    case "overall":
    default: {
      const sorted = sortByCount(rows.map((row) => createTrendGameItem(row))).slice(0, OVERALL_TREND_PAGE_SIZE);
      return sorted.map((game, index) => ({
        key: String(index + 1),
        label: `#${index + 1}`,
        count: game.count,
        games: [game],
      }));
    }
  }
}

export async function getAggregatedTrendResponse(params: {
  period: TrendPeriod;
  view: TrendView;
  kind: SubjectKind;
  overallPage: number;
  yearPage: TrendYearPage;
}): Promise<TrendResponse | null> {
  const { period, view, kind, overallPage, yearPage } = params;
  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    return null;
  }

  const fromTimestamp = getPeriodStart(period);
  const useHourCountsFor24h = period === "24h" && TREND_24H_SOURCE === "hour";
  const countSourceTable = useHourCountsFor24h ? TREND_COUNT_HOUR_TABLE : TREND_COUNT_DAY_TABLE;
  const countSourceTimeColumn = useHourCountsFor24h ? "hour_bucket" : "day_key";
  const fromCountSourceKey =
    fromTimestamp > 0
      ? useHourCountsFor24h
        ? toBeijingHourBucket(fromTimestamp)
        : toBeijingDayKey(fromTimestamp)
      : null;
  const overallOffset = Math.max(0, (overallPage - 1) * OVERALL_TREND_PAGE_SIZE);
  const yearFilterCondition = yearPage === "legacy" ? "d.release_year <= 2009" : "d.release_year >= 2010";
  const genreExcludeCondition =
    kind === "manga"
      ? "AND genre <> '漫画'"
      : kind === "lightnovel"
        ? "AND genre NOT IN ('轻小说', '小说')"
        : "";

  const sampleRows = (await sql.query(
    fromTimestamp > 0
      ? `
      SELECT
        COUNT(*)::BIGINT AS sample_count,
        MIN(created_at) AS min_created,
        MAX(created_at) AS max_created
      FROM ${SHARES_V2_TABLE}
      WHERE kind = $1
        AND created_at >= $2
      `
      : `
      SELECT
        COUNT(*)::BIGINT AS sample_count,
        MIN(created_at) AS min_created,
        MAX(created_at) AS max_created
      FROM ${SHARES_V2_TABLE}
      WHERE kind = $1
      `,
    fromTimestamp > 0 ? [kind, fromTimestamp] : [kind]
  )) as TrendSampleRow[];

  const sample = sampleRows[0];
  const sampleCount = toNumber(sample?.sample_count, 0);
  const rangeFrom = sample?.min_created === null ? null : toNumber(sample?.min_created, 0) || null;
  const rangeTo = sample?.max_created === null ? null : toNumber(sample?.max_created, 0) || null;

  if (sampleCount === 0) {
    return {
      period,
      view,
      sampleCount,
      range: { from: rangeFrom, to: rangeTo },
      lastUpdatedAt: Date.now(),
      items: [],
    };
  }

  let countRows: TrendSubjectCountRow[];

  if (period === "all") {
    if (view === "overall") {
      countRows = (await sql.query(
        `
        SELECT c.subject_id, c.count, d.name, d.localized_name, d.cover, d.release_year, NULL::jsonb AS genres
        FROM ${TREND_COUNT_ALL_TABLE} c
        JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
        WHERE c.kind = $1
        ORDER BY c.count DESC
        LIMIT ${OVERALL_TREND_PAGE_SIZE}
        OFFSET $2
        `,
        [kind, overallOffset]
      )) as TrendSubjectCountRow[];
    } else if (view === "genre") {
      countRows = (await sql.query(
        `
        WITH subject_counts AS (
          SELECT c.subject_id, c.count, d.name, d.localized_name, d.cover, d.release_year, d.genres
          FROM ${TREND_COUNT_ALL_TABLE} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE c.kind = $1
        ),
        expanded AS (
          SELECT
            sc.subject_id,
            sc.count,
            sc.name,
            sc.localized_name,
            sc.cover,
            sc.release_year,
            COALESCE(NULLIF(BTRIM(g.genre), ''), '未分类') AS genre
          FROM subject_counts sc
          LEFT JOIN LATERAL (
            SELECT jsonb_array_elements_text(
              CASE
                WHEN sc.genres IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof(sc.genres) = 'array' THEN sc.genres
                ELSE '[]'::jsonb
              END
            ) AS genre
          ) g ON true
        ),
        genre_totals AS (
          SELECT
            genre,
            SUM(count)::BIGINT AS total_count
          FROM expanded
          WHERE 1 = 1
            ${genreExcludeCondition}
          GROUP BY genre
          ORDER BY total_count DESC, genre ASC
          LIMIT ${GROUPED_BUCKET_LIMIT}
        ),
        ranked AS (
          SELECT
            e.subject_id,
            e.count,
            e.name,
            e.localized_name,
            e.cover,
            e.release_year,
            e.genre,
            gt.total_count,
            ROW_NUMBER() OVER (
              PARTITION BY e.genre
              ORDER BY e.count DESC, e.subject_id ASC
            ) AS genre_rank
          FROM expanded e
          JOIN genre_totals gt ON gt.genre = e.genre
        )
        SELECT
          r.subject_id,
          r.count,
          r.name,
          r.localized_name,
          r.cover,
          r.release_year,
          jsonb_build_array(r.genre) AS genres,
          r.total_count AS bucket_total
        FROM ranked r
        WHERE r.genre_rank <= 5
        ORDER BY r.total_count DESC, r.genre ASC, r.count DESC, r.subject_id ASC
        `,
        [kind]
      )) as TrendSubjectCountRow[];
    } else if (view === "year") {
      countRows = (await sql.query(
        `
        WITH subject_counts AS (
          SELECT c.subject_id, c.count
          FROM ${TREND_COUNT_ALL_TABLE} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE d.release_year IS NOT NULL
            AND c.kind = $1
            AND ${yearFilterCondition}
        ),
        ranked AS (
          SELECT
            d.release_year AS bucket_year,
            sc.subject_id,
            sc.count,
            d.name,
            d.localized_name,
            d.cover,
            d.release_year,
            SUM(sc.count) OVER (PARTITION BY d.release_year) AS bucket_total,
            ROW_NUMBER() OVER (
              PARTITION BY d.release_year
              ORDER BY sc.count DESC, sc.subject_id ASC
            ) AS bucket_rank
          FROM subject_counts sc
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = sc.subject_id AND d.kind = $1
        )
        SELECT
          r.subject_id,
          r.count,
          r.name,
          r.localized_name,
          r.cover,
          r.release_year,
          NULL::jsonb AS genres,
          r.bucket_total
        FROM ranked r
        WHERE r.bucket_rank <= 5
        ORDER BY r.bucket_year DESC, r.count DESC, r.subject_id ASC
        `,
        [kind]
      )) as TrendSubjectCountRow[];
    } else {
      countRows = (await sql.query(
        `
        WITH subject_counts AS (
          SELECT c.subject_id, c.count
          FROM ${TREND_COUNT_ALL_TABLE} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE d.release_year IS NOT NULL
            AND c.kind = $1
        ),
        ranked AS (
          SELECT
            ((d.release_year / 10) * 10) AS bucket_decade,
            sc.subject_id,
            sc.count,
            d.name,
            d.localized_name,
            d.cover,
            d.release_year,
            SUM(sc.count) OVER (PARTITION BY ((d.release_year / 10) * 10)) AS bucket_total,
            ROW_NUMBER() OVER (
              PARTITION BY ((d.release_year / 10) * 10)
              ORDER BY sc.count DESC, sc.subject_id ASC
            ) AS bucket_rank
          FROM subject_counts sc
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = sc.subject_id AND d.kind = $1
        )
        SELECT
          r.subject_id,
          r.count,
          r.name,
          r.localized_name,
          r.cover,
          r.release_year,
          NULL::jsonb AS genres,
          r.bucket_total
        FROM ranked r
        WHERE r.bucket_rank <= 5
        ORDER BY r.bucket_decade DESC, r.count DESC, r.subject_id ASC
        `,
        [kind]
      )) as TrendSubjectCountRow[];
    }
  } else {
    if (view === "overall") {
      countRows = (await sql.query(
        `
        WITH top_counts AS (
          SELECT
            c.subject_id,
            SUM(c.count)::BIGINT AS count
          FROM ${countSourceTable} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE c.${countSourceTimeColumn} >= $2
            AND c.kind = $1
          GROUP BY c.subject_id
          ORDER BY SUM(c.count) DESC
          LIMIT ${OVERALL_TREND_PAGE_SIZE}
          OFFSET $3
        )
        SELECT tc.subject_id, tc.count, d.name, d.localized_name, d.cover, d.release_year, NULL::jsonb AS genres
        FROM top_counts tc
        JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = tc.subject_id AND d.kind = $1
        ORDER BY tc.count DESC
        `,
        [kind, fromCountSourceKey, overallOffset]
      )) as TrendSubjectCountRow[];
    } else if (view === "genre") {
      countRows = (await sql.query(
        `
        WITH subject_counts AS (
          SELECT
            c.subject_id,
            SUM(c.count)::BIGINT AS count,
            d.name,
            d.localized_name,
            d.cover,
            d.release_year,
            d.genres
          FROM ${countSourceTable} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE c.${countSourceTimeColumn} >= $2
            AND c.kind = $1
          GROUP BY c.subject_id, d.name, d.localized_name, d.cover, d.release_year, d.genres
        ),
        expanded AS (
          SELECT
            sc.subject_id,
            sc.count,
            sc.name,
            sc.localized_name,
            sc.cover,
            sc.release_year,
            COALESCE(NULLIF(BTRIM(g.genre), ''), '未分类') AS genre
          FROM subject_counts sc
          LEFT JOIN LATERAL (
            SELECT jsonb_array_elements_text(
              CASE
                WHEN sc.genres IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof(sc.genres) = 'array' THEN sc.genres
                ELSE '[]'::jsonb
              END
            ) AS genre
          ) g ON true
        ),
        genre_totals AS (
          SELECT
            genre,
            SUM(count)::BIGINT AS total_count
          FROM expanded
          WHERE 1 = 1
            ${genreExcludeCondition}
          GROUP BY genre
          ORDER BY total_count DESC, genre ASC
          LIMIT ${GROUPED_BUCKET_LIMIT}
        ),
        ranked AS (
          SELECT
            e.subject_id,
            e.count,
            e.name,
            e.localized_name,
            e.cover,
            e.release_year,
            e.genre,
            gt.total_count,
            ROW_NUMBER() OVER (
              PARTITION BY e.genre
              ORDER BY e.count DESC, e.subject_id ASC
            ) AS genre_rank
          FROM expanded e
          JOIN genre_totals gt ON gt.genre = e.genre
        )
        SELECT
          r.subject_id,
          r.count,
          r.name,
          r.localized_name,
          r.cover,
          r.release_year,
          jsonb_build_array(r.genre) AS genres,
          r.total_count AS bucket_total
        FROM ranked r
        WHERE r.genre_rank <= 5
        ORDER BY r.total_count DESC, r.genre ASC, r.count DESC, r.subject_id ASC
        `,
        [kind, fromCountSourceKey]
      )) as TrendSubjectCountRow[];
    } else if (view === "year") {
      countRows = (await sql.query(
        `
        WITH subject_counts AS (
          SELECT
            c.subject_id,
            SUM(c.count)::BIGINT AS count
          FROM ${countSourceTable} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE c.${countSourceTimeColumn} >= $2
            AND c.kind = $1
            AND d.release_year IS NOT NULL
            AND ${yearFilterCondition}
          GROUP BY c.subject_id
        ),
        ranked AS (
          SELECT
            d.release_year AS bucket_year,
            sc.subject_id,
            sc.count,
            d.name,
            d.localized_name,
            d.cover,
            d.release_year,
            SUM(sc.count) OVER (PARTITION BY d.release_year) AS bucket_total,
            ROW_NUMBER() OVER (
              PARTITION BY d.release_year
              ORDER BY sc.count DESC, sc.subject_id ASC
            ) AS bucket_rank
          FROM subject_counts sc
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = sc.subject_id AND d.kind = $1
        )
        SELECT
          r.subject_id,
          r.count,
          r.name,
          r.localized_name,
          r.cover,
          r.release_year,
          NULL::jsonb AS genres,
          r.bucket_total
        FROM ranked r
        WHERE r.bucket_rank <= 5
        ORDER BY r.bucket_year DESC, r.count DESC, r.subject_id ASC
        `,
        [kind, fromCountSourceKey]
      )) as TrendSubjectCountRow[];
    } else {
      countRows = (await sql.query(
        `
        WITH subject_counts AS (
          SELECT
            c.subject_id,
            SUM(c.count)::BIGINT AS count
          FROM ${countSourceTable} c
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
          WHERE c.${countSourceTimeColumn} >= $2
            AND c.kind = $1
            AND d.release_year IS NOT NULL
          GROUP BY c.subject_id
        ),
        ranked AS (
          SELECT
            ((d.release_year / 10) * 10) AS bucket_decade,
            sc.subject_id,
            sc.count,
            d.name,
            d.localized_name,
            d.cover,
            d.release_year,
            SUM(sc.count) OVER (PARTITION BY ((d.release_year / 10) * 10)) AS bucket_total,
            ROW_NUMBER() OVER (
              PARTITION BY ((d.release_year / 10) * 10)
              ORDER BY sc.count DESC, sc.subject_id ASC
            ) AS bucket_rank
          FROM subject_counts sc
          JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = sc.subject_id AND d.kind = $1
        )
        SELECT
          r.subject_id,
          r.count,
          r.name,
          r.localized_name,
          r.cover,
          r.release_year,
          NULL::jsonb AS genres,
          r.bucket_total
        FROM ranked r
        WHERE r.bucket_rank <= 5
        ORDER BY r.bucket_decade DESC, r.count DESC, r.subject_id ASC
        `,
        [kind, fromCountSourceKey]
      )) as TrendSubjectCountRow[];
    }
  }

  return {
    period,
    view,
    sampleCount,
    range: { from: rangeFrom, to: rangeTo },
    lastUpdatedAt: Date.now(),
    items: buildTrendItemsFromCounts(view, countRows),
  };
}

export async function getTrendSampleSummary(period: TrendPeriod, kind: SubjectKind): Promise<TrendSampleSummary | null> {
  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    return null;
  }

  const fromTimestamp = getPeriodStart(period);
  const sampleRows = (await sql.query(
    fromTimestamp > 0
      ? `
      SELECT
        COUNT(*)::BIGINT AS sample_count,
        MIN(created_at) AS min_created,
        MAX(created_at) AS max_created
      FROM ${SHARES_V2_TABLE}
      WHERE kind = $1
        AND created_at >= $2
      `
      : `
      SELECT
        COUNT(*)::BIGINT AS sample_count,
        MIN(created_at) AS min_created,
        MAX(created_at) AS max_created
      FROM ${SHARES_V2_TABLE}
      WHERE kind = $1
      `,
    fromTimestamp > 0 ? [kind, fromTimestamp] : [kind]
  )) as TrendSampleRow[];

  const sample = sampleRows[0];
  const sampleCount = toNumber(sample?.sample_count, 0);
  const rangeFrom = sample?.min_created === null ? null : toNumber(sample?.min_created, 0) || null;
  const rangeTo = sample?.max_created === null ? null : toNumber(sample?.max_created, 0) || null;

  return {
    sampleCount,
    range: { from: rangeFrom, to: rangeTo },
  };
}

export async function getTrendSampleSummaryCache(
  period: TrendPeriod,
  kind: SubjectKind,
  options?: { allowExpired?: boolean }
): Promise<TrendSampleSummary | null> {
  const allowExpired = options?.allowExpired === true;
  const key = trendSampleCacheKey(period, kind);
  if (TREND_MEMORY_CACHE_ENABLED) {
    const fromMemory = getMemoryTrendSampleSummaryCache(key, allowExpired);
    if (fromMemory) return fromMemory;
  }

  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!TREND_MEMORY_CACHE_ENABLED) {
      throwDatabaseNotReady("getTrendSampleSummaryCache failed");
    }
  } else {
    try {
      const rows = (await sql`
        SELECT payload, expires_at, updated_at
        FROM ${sql.unsafe(TRENDS_CACHE_TABLE)}
        WHERE cache_key = ${key}
        LIMIT 1
      `) as TrendCacheRow[];

      if (rows.length > 0) {
        const row = rows[0];
        const expiresAt = toNumber(row.expires_at, 0);
        const updatedAt = resolveTrendCacheUpdatedAt(expiresAt, row.updated_at);
        if (isTrendCacheExpired(expiresAt, updatedAt, Date.now())) {
          if (!allowExpired) {
            await sql`
              DELETE FROM ${sql.unsafe(TRENDS_CACHE_TABLE)}
              WHERE cache_key = ${key}
            `;
            return null;
          }
        }

        const payload = parseTrendSampleSummaryPayload(row.payload);
        if (payload) {
          if (TREND_MEMORY_CACHE_ENABLED) {
            getMemoryStore().trendSampleCache.set(key, {
              value: payload,
              expiresAt,
              updatedAt,
            });
          }
          return payload;
        }
      }
    } catch (error) {
      if (!TREND_MEMORY_CACHE_ENABLED) {
        throwStorageError("getTrendSampleSummaryCache failed: database read error", error);
      }
    }
  }

  return null;
}

export async function setTrendSampleSummaryCache(
  period: TrendPeriod,
  kind: SubjectKind,
  value: TrendSampleSummary,
  ttlSeconds = 3600
): Promise<void> {
  const key = trendSampleCacheKey(period, kind);
  const updatedAt = Date.now();
  const expiresAt = updatedAt + ttlSeconds * 1000;

  if (TREND_MEMORY_CACHE_ENABLED) {
    getMemoryStore().trendSampleCache.set(key, {
      value,
      expiresAt,
      updatedAt,
    });
  }

  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!TREND_MEMORY_CACHE_ENABLED) {
      throwDatabaseNotReady("setTrendSampleSummaryCache failed");
    }
    return;
  }

  try {
    await sql`
      INSERT INTO ${sql.unsafe(TRENDS_CACHE_TABLE)} (
        cache_key,
        period,
        view,
        kind,
        payload,
        expires_at,
        updated_at
      )
      VALUES (
        ${key},
        ${period},
        ${SAMPLE_SUMMARY_CACHE_VIEW},
        ${kind},
        ${JSON.stringify(value)}::jsonb,
        ${expiresAt},
        ${updatedAt}
      )
      ON CONFLICT (cache_key) DO UPDATE SET
        period = EXCLUDED.period,
        view = EXCLUDED.view,
        kind = EXCLUDED.kind,
        payload = EXCLUDED.payload,
        expires_at = EXCLUDED.expires_at,
        updated_at = EXCLUDED.updated_at
    `;
  } catch (error) {
    if (!TREND_MEMORY_CACHE_ENABLED) {
      throwStorageError("setTrendSampleSummaryCache failed: database write error", error);
    }
  }
}

export async function getTrendsCache(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  overallPage: number,
  yearPage: TrendYearPage,
  options?: { allowExpired?: boolean }
): Promise<TrendResponse | null> {
  const allowExpired = options?.allowExpired === true;
  const key = trendCacheKey(period, view, kind, overallPage, yearPage);
  if (TREND_MEMORY_CACHE_ENABLED) {
    const fromMemory = getMemoryTrendCache(key, allowExpired);
    if (fromMemory) return fromMemory;
  }

  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!TREND_MEMORY_CACHE_ENABLED) {
      throwDatabaseNotReady("getTrendsCache failed");
    }
  } else {
    try {
      const rows = (await sql`
        SELECT payload, expires_at, updated_at
        FROM ${sql.unsafe(TRENDS_CACHE_TABLE)}
        WHERE cache_key = ${key}
        LIMIT 1
      `) as TrendCacheRow[];

      if (rows.length > 0) {
        const row = rows[0];
        const expiresAt = toNumber(row.expires_at, 0);
        const updatedAt = resolveTrendCacheUpdatedAt(expiresAt, row.updated_at);
        if (isTrendCacheExpired(expiresAt, updatedAt, Date.now())) {
          if (!allowExpired) {
            await sql`
              DELETE FROM ${sql.unsafe(TRENDS_CACHE_TABLE)}
              WHERE cache_key = ${key}
            `;
            return null;
          }
        }

        const payload = parseTrendPayload(row.payload);
        if (payload) {
          if (TREND_MEMORY_CACHE_ENABLED) {
            getMemoryStore().trendCache.set(key, {
              value: payload,
              expiresAt,
              updatedAt,
            });
          }
          return payload;
        }
      }
    } catch (error) {
      if (!TREND_MEMORY_CACHE_ENABLED) {
        throwStorageError("getTrendsCache failed: database read error", error);
      }
    }
  }

  return null;
}

export async function setTrendsCache(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  overallPage: number,
  yearPage: TrendYearPage,
  value: TrendResponse,
  ttlSeconds = 3600
): Promise<void> {
  const key = trendCacheKey(period, view, kind, overallPage, yearPage);
  const updatedAt = Date.now();
  const expiresAt = updatedAt + ttlSeconds * 1000;
  if (TREND_MEMORY_CACHE_ENABLED) {
    getMemoryStore().trendCache.set(key, {
      value,
      expiresAt,
      updatedAt,
    });
  }

  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    if (!TREND_MEMORY_CACHE_ENABLED) {
      throwDatabaseNotReady("setTrendsCache failed");
    }
    return;
  }

  try {
    await sql`
      INSERT INTO ${sql.unsafe(TRENDS_CACHE_TABLE)} (
        cache_key,
        period,
        view,
        kind,
        payload,
        expires_at,
        updated_at
      )
      VALUES (
        ${key},
        ${period},
        ${view},
        ${kind},
        ${JSON.stringify(value)}::jsonb,
        ${expiresAt},
        ${updatedAt}
      )
      ON CONFLICT (cache_key) DO UPDATE SET
        period = EXCLUDED.period,
        view = EXCLUDED.view,
        kind = EXCLUDED.kind,
        payload = EXCLUDED.payload,
        expires_at = EXCLUDED.expires_at,
        updated_at = EXCLUDED.updated_at
    `;
  } catch (error) {
    if (!TREND_MEMORY_CACHE_ENABLED) {
      throwStorageError("setTrendsCache failed: database write error", error);
    }
  }
}

export async function archiveHotSharesToColdStorage(params?: {
  olderThanDays?: number;
  batchSize?: number;
  cleanupTrendDays?: number;
}): Promise<{ processed: number; archived: number; skipped: number; cleanedTrendRows: number }> {
  const olderThanDays = params?.olderThanDays ?? 30;
  const batchSize = params?.batchSize ?? 500;
  const cleanupTrendDays = params?.cleanupTrendDays ?? 190;

  const sql = getSqlClient();
  if (!sql || !(await ensureSchema())) {
    return {
      processed: 0,
      archived: 0,
      skipped: 0,
      cleanedTrendRows: 0,
    };
  }

  const cutoff = Date.now() - olderThanDays * 24 * 60 * 60 * 1000;
  const rows = (await sql.query(
    `
    SELECT share_id, kind, creator_name, storage_tier, hot_payload, cold_object_key, created_at, updated_at, last_viewed_at
    FROM ${SHARES_V2_TABLE}
    WHERE storage_tier = 'hot'
      AND hot_payload IS NOT NULL
      AND created_at < $1
    ORDER BY created_at ASC
    LIMIT $2
    `,
    [cutoff, batchSize]
  )) as ShareRegistryRow[];

  let archived = 0;
  let skipped = 0;

  for (const row of rows) {
    const payload = normalizeCompactPayload(row.hot_payload);
    if (!payload || !isColdStorageEnabled()) {
      skipped += 1;
      continue;
    }

    const objectKey = buildColdObjectKey(row.share_id);
    const uploaded = await putColdSharePayload(objectKey, payload);
    if (!uploaded) {
      skipped += 1;
      continue;
    }

    await sql.query(
      `
      UPDATE ${SHARES_V2_TABLE}
      SET
        storage_tier = 'cold',
        cold_object_key = $2,
        hot_payload = NULL,
        updated_at = $3
      WHERE share_id = $1
      `,
      [row.share_id, objectKey, Date.now()]
    );
    archived += 1;
  }

  const cleanupBeforeDayKey = toBeijingDayKey(Date.now() - cleanupTrendDays * DAY_MS);
  const cleanedDayRows = (await sql.query(
    `
    DELETE FROM ${TREND_COUNT_DAY_TABLE}
    WHERE day_key < $1
    RETURNING 1
    `,
    [cleanupBeforeDayKey]
  )) as Array<{ "?column?": number }>;

  const cleanupBeforeHourBucket = toBeijingHourBucket(getBeijingDayStart(Date.now()) - DAY_MS);
  const cleanedHourRows = (await sql.query(
    `
    DELETE FROM ${TREND_COUNT_HOUR_TABLE}
    WHERE hour_bucket < $1
    RETURNING 1
    `,
    [cleanupBeforeHourBucket]
  )) as Array<{ "?column?": number }>;

  return {
    processed: rows.length,
    archived,
    skipped,
    cleanedTrendRows: cleanedDayRows.length + cleanedHourRows.length,
  };
}
