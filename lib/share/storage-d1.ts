import {
  compactPayloadToGames,
  createContentHash,
  normalizeCompactPayload,
  toCompactSharePayload,
} from "@/lib/share/compact";
import type { StorageBackend, TrendRollupCheckpoint, TrendRollupResult } from "@/lib/share/storage-contract";
import type { StoredShareV1, TrendBucket, TrendPeriod, TrendView, TrendYearPage } from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, type SubjectKind, parseSubjectKind } from "@/lib/subject-kind";
import {
  GROUPED_BUCKET_LIMIT,
  OVERALL_TREND_PAGE_SIZE,
  SAMPLE_SUMMARY_CACHE_VIEW,
  SHARE_ALIAS_TABLE,
  SHARE_SUBJECT_SLOT_TABLE,
  SHARE_VIEW_TOTAL_TABLE,
  SHARES_V2_TABLE,
  SYSTEM_CHECKPOINT_TABLE,
  SUBJECT_DIM_TABLE,
  SUBJECT_GENRE_DIM_TABLE,
  TREND_COUNT_ALL_TABLE,
  TREND_COUNT_DAY_TABLE,
  TREND_COUNT_HOUR_TABLE,
  TRENDS_CACHE_TABLE,
  TREND_24H_SOURCE,
  type ShareRegistryRow,
  type SubjectDimRow,
  type TrendSampleRow,
  buildTrendIncrements,
  chunkArray,
  collectSubjectIdsFromPayload,
  getPeriodStart,
  getBeijingDayStart,
  isTrendCacheExpired,
  normalizeStoredShare,
  parseStringArray,
  parseJsonValue,
  parsePositiveInt,
  parseTrendPayload,
  parseTrendSampleSummaryPayload,
  readEnv,
  resolveTrendCacheUpdatedAt,
  throwStorageError,
  toBeijingDayKey,
  toBeijingHourBucket,
  toNumber,
  toSubjectSnapshot,
  trendCacheKey,
  trendSampleCacheKey,
} from "@/lib/share/storage-common";
import {
  type D1DatabaseLike,
  type StatementInput,
  buildPlaceholders,
  ensureD1Schema,
  execute,
  executeBatch,
  getD1Database,
  queryAll,
  queryFirst,
} from "@/lib/share/storage-d1-runtime";

type ShareCountRow = {
  total_count: number | string;
};

type TrendCacheRow = {
  payload: unknown;
  expires_at: number | string;
  updated_at?: number | string | null;
};

type TrendSubjectQueryRow = {
  subject_id: string;
  count: number | string;
  name: string;
  localized_name: string | null;
  cover: string | null;
  release_year: number | string | null;
};

type RankedTrendBucketRow = TrendSubjectQueryRow & {
  bucket_key: string;
  bucket_label: string;
  bucket_total: number | string;
};

type SubjectGenreRow = {
  subject_id: string;
  genre: string;
};

type ShareViewRollupCheckpointRow = {
  payload: unknown;
};

type ShareViewAggregationRow = {
  last_aggregated_at: number | string | null;
};

type TrendUpdatedAtRow = {
  last_updated_at: number | string | null;
};

type SystemCheckpointRow = {
  payload: unknown;
  updated_at: number | string;
};

type TrendRollupSourceRow = {
  share_id: string;
  kind: string;
  slot_index: number | string;
  subject_id: string;
  created_at: number | string;
  day_key: number | string;
  hour_bucket: number | string;
};

const GROUPED_TOP_GAMES_LIMIT = 5;
const SHARE_VIEW_ROLLUP_CHECKPOINT_KEY = "system:share-view-rollup:v1";
const TREND_ROLLUP_CHECKPOINT_KEY = "system:trend-rollup:v1";
const DEFAULT_TREND_ROLLUP_BATCH_SIZE = 512;
const TREND_ROLLUP_CHECKPOINT_CACHE_TTL_MS = 60 * 1000;
// D1 starts rejecting statements once bound variables exceed 100.
const D1_SAFE_SQL_VARIABLES = 96;
const D1_IN_CLAUSE_CHUNK_SIZE = D1_SAFE_SQL_VARIABLES;
type TrendUpdateMode = "auto" | "realtime" | "rollup";
let trendRollupCheckpointCache:
  | {
      expiresAt: number;
      hasCheckpoint: boolean;
    }
  | null = null;

function resolveCompactPayload(row: ShareRegistryRow) {
  return normalizeCompactPayload(parseJsonValue<unknown>(row.hot_payload));
}

function toOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return Math.trunc(parsed);
    }
  }
  return null;
}

function parseTrendRollupCheckpoint(value: unknown): TrendRollupCheckpoint | null {
  const parsed = parseJsonValue<Partial<TrendRollupCheckpoint>>(value);
  const createdAt = toOptionalNumber(parsed?.createdAt);
  const slotIndex = toOptionalNumber(parsed?.slotIndex);
  const shareId = typeof parsed?.shareId === "string" ? parsed.shareId.trim().toLowerCase() : "";

  if (createdAt === null || slotIndex === null || !shareId) {
    return null;
  }

  return {
    createdAt,
    shareId,
    slotIndex,
  };
}

function readTrendUpdateMode(): TrendUpdateMode {
  const raw = readEnv("MY9_TRENDS_UPDATE_MODE");
  if (raw === "realtime" || raw === "rollup") {
    return raw;
  }
  return "auto";
}

async function getSystemCheckpoint<T>(
  db: D1DatabaseLike,
  checkpointKey: string,
  parsePayload: (value: unknown) => T | null
): Promise<T | null> {
  const row = await queryFirst<SystemCheckpointRow>(
    db,
    `
    SELECT payload, updated_at
    FROM ${SYSTEM_CHECKPOINT_TABLE}
    WHERE checkpoint_key = ?
    LIMIT 1
    `,
    [checkpointKey]
  );
  return parsePayload(row?.payload);
}

async function getSystemCheckpointRecord<T>(
  db: D1DatabaseLike,
  checkpointKey: string,
  parsePayload: (value: unknown) => T | null
): Promise<{ value: T; updatedAt: number } | null> {
  const row = await queryFirst<SystemCheckpointRow>(
    db,
    `
    SELECT payload, updated_at
    FROM ${SYSTEM_CHECKPOINT_TABLE}
    WHERE checkpoint_key = ?
    LIMIT 1
    `,
    [checkpointKey]
  );
  const value = parsePayload(row?.payload);
  if (!value) {
    return null;
  }
  return {
    value,
    updatedAt: toOptionalNumber(row?.updated_at) ?? 0,
  };
}

async function setSystemCheckpoint(
  db: D1DatabaseLike,
  checkpointKey: string,
  payload: unknown,
  updatedAt = Date.now()
): Promise<void> {
  await execute(
    db,
    `
    INSERT INTO ${SYSTEM_CHECKPOINT_TABLE} (
      checkpoint_key, payload, updated_at
    ) VALUES (?, ?, ?)
    ON CONFLICT (checkpoint_key) DO UPDATE SET
      payload = excluded.payload,
      updated_at = excluded.updated_at
    `,
    [checkpointKey, JSON.stringify(payload), updatedAt]
  );
}

async function getTrendRollupCheckpoint(db: D1DatabaseLike): Promise<TrendRollupCheckpoint | null> {
  return await getSystemCheckpoint(db, TREND_ROLLUP_CHECKPOINT_KEY, parseTrendRollupCheckpoint);
}

async function setTrendRollupCheckpoint(db: D1DatabaseLike, checkpoint: TrendRollupCheckpoint): Promise<void> {
  await setSystemCheckpoint(db, TREND_ROLLUP_CHECKPOINT_KEY, checkpoint);
  trendRollupCheckpointCache = {
    expiresAt: Date.now() + TREND_ROLLUP_CHECKPOINT_CACHE_TTL_MS,
    hasCheckpoint: true,
  };
}

async function hasTrendRollupCheckpoint(db: D1DatabaseLike): Promise<boolean> {
  const now = Date.now();
  if (trendRollupCheckpointCache && trendRollupCheckpointCache.expiresAt > now) {
    return trendRollupCheckpointCache.hasCheckpoint;
  }

  const checkpoint = await getTrendRollupCheckpoint(db);
  const hasCheckpoint = Boolean(checkpoint);
  trendRollupCheckpointCache = {
    expiresAt: now + TREND_ROLLUP_CHECKPOINT_CACHE_TTL_MS,
    hasCheckpoint,
  };
  return hasCheckpoint;
}

async function shouldUseRealtimeTrendWrites(db: D1DatabaseLike): Promise<boolean> {
  const mode = readTrendUpdateMode();
  if (mode === "realtime") {
    return true;
  }
  if (mode === "rollup") {
    return false;
  }
  return !(await hasTrendRollupCheckpoint(db));
}

function normalizeGenreList(genres: string[] | undefined): string[] {
  return Array.from(new Set((genres ?? []).map((genre) => genre.trim()).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  );
}

function areStringArraysEqual(left: string[], right: string[]) {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((value, index) => value === right[index]);
}

async function fetchExistingSubjectDimRows(db: D1DatabaseLike, kind: SubjectKind, subjectIds: string[]) {
  const rowsById = new Map<string, SubjectDimRow>();
  for (const ids of chunkArray(subjectIds, D1_IN_CLAUSE_CHUNK_SIZE)) {
    if (ids.length === 0) continue;
    const rows = await queryAll<SubjectDimRow>(
      db,
      `
      SELECT subject_id, name, localized_name, cover, release_year, genres
      FROM ${SUBJECT_DIM_TABLE}
      WHERE kind = ?
        AND subject_id IN (${buildPlaceholders(ids.length)})
      `,
      [kind, ...ids]
    );
    for (const row of rows) {
      rowsById.set(row.subject_id, row);
    }
  }
  return rowsById;
}

async function fetchExistingSubjectGenres(db: D1DatabaseLike, kind: SubjectKind, subjectIds: string[]) {
  const rowsById = new Map<string, string[]>();
  for (const ids of chunkArray(subjectIds, D1_IN_CLAUSE_CHUNK_SIZE)) {
    if (ids.length === 0) continue;
    const rows = await queryAll<SubjectGenreRow>(
      db,
      `
      SELECT subject_id, genre
      FROM ${SUBJECT_GENRE_DIM_TABLE}
      WHERE kind = ?
        AND subject_id IN (${buildPlaceholders(ids.length)})
      `,
      [kind, ...ids]
    );
    for (const row of rows) {
      const current = rowsById.get(row.subject_id);
      if (current) {
        current.push(row.genre);
      } else {
        rowsById.set(row.subject_id, [row.genre]);
      }
    }
  }
  for (const [subjectId, genres] of rowsById) {
    rowsById.set(subjectId, normalizeGenreList(genres));
  }
  return rowsById;
}

function buildSubjectDimStatement(params: {
  kind: SubjectKind;
  snapshot: ReturnType<typeof toSubjectSnapshot>;
  existingRow?: SubjectDimRow;
  updatedAt: number;
}): StatementInput | null {
  const existingRow = params.existingRow;
  const desiredGenres = normalizeGenreList(params.snapshot.genres);
  const existingGenres = normalizeGenreList(existingRow ? parseStringArray(existingRow.genres) : undefined);
  const desiredLocalizedName = params.snapshot.localizedName ?? existingRow?.localized_name ?? null;
  const desiredCover = params.snapshot.cover ?? existingRow?.cover ?? null;
  const desiredReleaseYear = params.snapshot.releaseYear ?? toOptionalNumber(existingRow?.release_year);
  const mergedGenres = params.snapshot.genres ? desiredGenres : existingGenres;

  if (
    existingRow &&
    existingRow.name === params.snapshot.name &&
    (existingRow.localized_name ?? null) === desiredLocalizedName &&
    (existingRow.cover ?? null) === desiredCover &&
    toOptionalNumber(existingRow.release_year) === desiredReleaseYear &&
    areStringArraysEqual(existingGenres, mergedGenres)
  ) {
    return null;
  }

  return {
    sql: `
    INSERT INTO ${SUBJECT_DIM_TABLE} (
      kind, subject_id, name, localized_name, cover, release_year, genres, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (kind, subject_id) DO UPDATE SET
      name = excluded.name,
      localized_name = excluded.localized_name,
      cover = excluded.cover,
      release_year = excluded.release_year,
      genres = excluded.genres,
      updated_at = excluded.updated_at
    `,
    params: [
      params.kind,
      params.snapshot.subjectId,
      params.snapshot.name,
      desiredLocalizedName,
      desiredCover,
      desiredReleaseYear,
      mergedGenres.length > 0 ? JSON.stringify(mergedGenres) : null,
      params.updatedAt,
    ],
  };
}

function buildSubjectGenreStatements(params: {
  kind: SubjectKind;
  snapshot: ReturnType<typeof toSubjectSnapshot>;
  existingGenres?: string[];
  updatedAt: number;
}): StatementInput[] {
  if (!params.snapshot.genres) {
    return [];
  }

  const desiredGenres = normalizeGenreList(params.snapshot.genres);
  const existingGenres = normalizeGenreList(params.existingGenres);
  const existingGenreSet = new Set(existingGenres);
  const desiredGenreSet = new Set(desiredGenres);
  const toDelete = existingGenres.filter((genre) => !desiredGenreSet.has(genre));
  const toInsert = desiredGenres.filter((genre) => !existingGenreSet.has(genre));

  if (toDelete.length === 0 && toInsert.length === 0) {
    return [];
  }

  return [
    ...(toDelete.length > 0
      ? [
          {
            sql: `
            DELETE FROM ${SUBJECT_GENRE_DIM_TABLE}
            WHERE kind = ?
              AND subject_id = ?
              AND genre IN (${buildPlaceholders(toDelete.length)})
            `,
            params: [params.kind, params.snapshot.subjectId, ...toDelete],
          } satisfies StatementInput,
        ]
      : []),
    ...toInsert.map<StatementInput>((genre) => ({
      sql: `
      INSERT OR IGNORE INTO ${SUBJECT_GENRE_DIM_TABLE} (
        kind, subject_id, genre, updated_at
      ) VALUES (?, ?, ?, ?)
      `,
      params: [params.kind, params.snapshot.subjectId, genre, params.updatedAt],
    })),
  ];
}

async function fetchSubjectSnapshots(db: D1DatabaseLike, kind: SubjectKind, subjectIds: string[]) {
  const snapshots = new Map<string, ReturnType<typeof toSubjectSnapshot>>();
  for (const ids of chunkArray(subjectIds, D1_IN_CLAUSE_CHUNK_SIZE)) {
    if (ids.length === 0) continue;
    const rows = await queryAll<SubjectDimRow>(
      db,
      `
      SELECT subject_id, name, localized_name, cover, release_year, genres
      FROM ${SUBJECT_DIM_TABLE}
      WHERE kind = ?
        AND subject_id IN (${buildPlaceholders(ids.length)})
      `,
      [kind, ...ids]
    );
    for (const row of rows) {
      snapshots.set(row.subject_id, toSubjectSnapshot(row));
    }
  }
  return snapshots;
}

async function inflateShareFromRegistryRow(db: D1DatabaseLike, row: ShareRegistryRow): Promise<StoredShareV1 | null> {
  const kind = parseSubjectKind(row.kind) ?? DEFAULT_SUBJECT_KIND;
  const payload = resolveCompactPayload(row);
  if (!payload) return null;

  const subjectSnapshots = await fetchSubjectSnapshots(db, kind, collectSubjectIdsFromPayload(payload));
  return normalizeStoredShare({
    shareId: String(row.share_id),
    kind,
    creatorName: typeof row.creator_name === "string" ? row.creator_name : null,
    games: compactPayloadToGames({ payload, subjectSnapshots }),
    createdAt: toNumber(row.created_at, Date.now()),
    updatedAt: toNumber(row.updated_at, Date.now()),
    lastViewedAt: toNumber(row.last_viewed_at, Date.now()),
  });
}

async function resolveExistingShareIdByHash(db: D1DatabaseLike, contentHash: string) {
  const row = await queryFirst<{ share_id: string }>(
    db,
    `
    SELECT share_id
    FROM ${SHARES_V2_TABLE}
    WHERE content_hash = ?
    LIMIT 1
    `,
    [contentHash]
  );
  return row?.share_id ?? null;
}

function buildTrendCountSourceQuery(period: TrendPeriod, kind: SubjectKind) {
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

  return period === "all"
    ? {
        sql: `
        SELECT kind, subject_id, count
        FROM ${TREND_COUNT_ALL_TABLE}
        WHERE kind = ?
        `,
        params: [kind],
      }
    : {
        sql: `
        SELECT kind, subject_id, SUM(count) AS count
        FROM ${countSourceTable}
        WHERE kind = ?
          AND ${countSourceTimeColumn} >= ?
        GROUP BY kind, subject_id
        `,
        params: [kind, fromCountSourceKey],
      };
}

async function resolveTrendLastUpdatedAt(db: D1DatabaseLike, period: TrendPeriod, kind: SubjectKind): Promise<number> {
  const rollupCheckpoint = await getSystemCheckpointRecord(db, TREND_ROLLUP_CHECKPOINT_KEY, parseTrendRollupCheckpoint);
  if (rollupCheckpoint) {
    return rollupCheckpoint.updatedAt;
  }

  const fromTimestamp = getPeriodStart(period);
  const useHourCountsFor24h = period === "24h" && TREND_24H_SOURCE === "hour";
  const updatedAtRow = await queryFirst<TrendUpdatedAtRow>(
    db,
    period === "all"
      ? `
        SELECT MAX(updated_at) AS last_updated_at
        FROM ${TREND_COUNT_ALL_TABLE}
        WHERE kind = ?
        `
      : `
        SELECT MAX(updated_at) AS last_updated_at
        FROM ${useHourCountsFor24h ? TREND_COUNT_HOUR_TABLE : TREND_COUNT_DAY_TABLE}
        WHERE kind = ?
          AND ${useHourCountsFor24h ? "hour_bucket" : "day_key"} >= ?
        `,
    period === "all"
      ? [kind]
      : [kind, useHourCountsFor24h ? toBeijingHourBucket(fromTimestamp) : toBeijingDayKey(fromTimestamp)]
  );

  return toOptionalNumber(updatedAtRow?.last_updated_at) ?? 0;
}

function toTrendRollupCheckpoint(row: TrendRollupSourceRow): TrendRollupCheckpoint {
  return {
    createdAt: toNumber(row.created_at, 0),
    shareId: String(row.share_id),
    slotIndex: toNumber(row.slot_index, 0),
  };
}

function buildTrendSampleQuery(
  period: TrendPeriod,
  hasCheckpoint: boolean
): {
  sql: string;
  bindValues: (kind: SubjectKind, fromTimestamp: number, checkpoint: TrendRollupCheckpoint | null) => Array<string | number>;
} {
  if (period === "all") {
    return hasCheckpoint
      ? {
          sql: `
          SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
          FROM ${SHARES_V2_TABLE}
          WHERE kind = ?
            AND created_at <= ?
          `,
          bindValues: (kind, _fromTimestamp, checkpoint) => [kind, checkpoint?.createdAt ?? 0],
        }
      : {
          sql: `
          SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
          FROM ${SHARES_V2_TABLE}
          WHERE kind = ?
          `,
          bindValues: (kind) => [kind],
        };
  }

  return hasCheckpoint
    ? {
        sql: `
        SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
        FROM ${SHARES_V2_TABLE}
        WHERE kind = ?
          AND created_at >= ?
          AND created_at <= ?
        `,
        bindValues: (kind, fromTimestamp, checkpoint) => [kind, fromTimestamp, checkpoint?.createdAt ?? 0],
      }
    : {
        sql: `
        SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
        FROM ${SHARES_V2_TABLE}
        WHERE kind = ?
          AND created_at >= ?
        `,
        bindValues: (kind, fromTimestamp) => [kind, fromTimestamp],
      };
}

async function resolveTrendRollupHead(db: D1DatabaseLike): Promise<TrendRollupCheckpoint | null> {
  const row = await queryFirst<TrendRollupSourceRow>(
    db,
    `
    SELECT share_id, slot_index, created_at, kind, subject_id, day_key, hour_bucket
    FROM ${SHARE_SUBJECT_SLOT_TABLE}
    ORDER BY created_at DESC, share_id DESC, slot_index DESC
    LIMIT 1
    `
  );
  return row ? toTrendRollupCheckpoint(row) : null;
}

async function hasExistingTrendAggregates(db: D1DatabaseLike): Promise<boolean> {
  const row = await queryFirst<{ has_rows: number | string }>(
    db,
    `
    SELECT 1 AS has_rows
    FROM ${TREND_COUNT_ALL_TABLE}
    LIMIT 1
    `
  );
  return toNumber(row?.has_rows, 0) === 1;
}

async function fetchTrendRollupSourceRows(
  db: D1DatabaseLike,
  checkpoint: TrendRollupCheckpoint | null,
  limit: number
): Promise<TrendRollupSourceRow[]> {
  if (!checkpoint) {
    return await queryAll<TrendRollupSourceRow>(
      db,
      `
      SELECT share_id, kind, slot_index, subject_id, created_at, day_key, hour_bucket
      FROM ${SHARE_SUBJECT_SLOT_TABLE}
      ORDER BY created_at ASC, share_id ASC, slot_index ASC
      LIMIT ?
      `,
      [limit]
    );
  }

  return await queryAll<TrendRollupSourceRow>(
    db,
    `
    SELECT share_id, kind, slot_index, subject_id, created_at, day_key, hour_bucket
    FROM ${SHARE_SUBJECT_SLOT_TABLE}
    WHERE created_at > ?
      OR (created_at = ? AND share_id > ?)
      OR (created_at = ? AND share_id = ? AND slot_index > ?)
    ORDER BY created_at ASC, share_id ASC, slot_index ASC
    LIMIT ?
    `,
    [
      checkpoint.createdAt,
      checkpoint.createdAt,
      checkpoint.shareId,
      checkpoint.createdAt,
      checkpoint.shareId,
      checkpoint.slotIndex,
      limit,
    ]
  );
}

function buildTrendRollupRangeWhereClause(start: TrendRollupCheckpoint | null, end: TrendRollupCheckpoint) {
  const endParams = [end.createdAt, end.createdAt, end.shareId, end.createdAt, end.shareId, end.slotIndex];

  if (!start) {
    return {
      sql: `
      (
        created_at < ?
        OR (created_at = ? AND share_id < ?)
        OR (created_at = ? AND share_id = ? AND slot_index <= ?)
      )
      `,
      params: endParams,
    };
  }

  return {
    sql: `
    (
      created_at > ?
      OR (created_at = ? AND share_id > ?)
      OR (created_at = ? AND share_id = ? AND slot_index > ?)
    )
    AND (
      created_at < ?
      OR (created_at = ? AND share_id < ?)
      OR (created_at = ? AND share_id = ? AND slot_index <= ?)
    )
    `,
    params: [
      start.createdAt,
      start.createdAt,
      start.shareId,
      start.createdAt,
      start.shareId,
      start.slotIndex,
      ...endParams,
    ],
  };
}

function buildTrendRollupStatements(
  start: TrendRollupCheckpoint | null,
  end: TrendRollupCheckpoint,
  updatedAt: number
): StatementInput[] {
  const range = buildTrendRollupRangeWhereClause(start, end);
  const checkpointPayload = JSON.stringify(end);

  return [
    {
      sql: `
      INSERT INTO ${TREND_COUNT_ALL_TABLE} (kind, subject_id, count, updated_at)
      SELECT kind, subject_id, COUNT(*) AS count, ? AS updated_at
      FROM ${SHARE_SUBJECT_SLOT_TABLE}
      WHERE ${range.sql}
      GROUP BY kind, subject_id
      ON CONFLICT (kind, subject_id) DO UPDATE SET
        count = ${TREND_COUNT_ALL_TABLE}.count + excluded.count,
        updated_at = excluded.updated_at
      `,
      params: [updatedAt, ...range.params],
    },
    {
      sql: `
      INSERT INTO ${TREND_COUNT_DAY_TABLE} (kind, day_key, subject_id, count, updated_at)
      SELECT kind, day_key, subject_id, COUNT(*) AS count, ? AS updated_at
      FROM ${SHARE_SUBJECT_SLOT_TABLE}
      WHERE ${range.sql}
      GROUP BY kind, day_key, subject_id
      ON CONFLICT (kind, day_key, subject_id) DO UPDATE SET
        count = ${TREND_COUNT_DAY_TABLE}.count + excluded.count,
        updated_at = excluded.updated_at
      `,
      params: [updatedAt, ...range.params],
    },
    {
      sql: `
      INSERT INTO ${TREND_COUNT_HOUR_TABLE} (kind, hour_bucket, subject_id, count, updated_at)
      SELECT kind, hour_bucket, subject_id, COUNT(*) AS count, ? AS updated_at
      FROM ${SHARE_SUBJECT_SLOT_TABLE}
      WHERE ${range.sql}
      GROUP BY kind, hour_bucket, subject_id
      ON CONFLICT (kind, hour_bucket, subject_id) DO UPDATE SET
        count = ${TREND_COUNT_HOUR_TABLE}.count + excluded.count,
        updated_at = excluded.updated_at
      `,
      params: [updatedAt, ...range.params],
    },
    {
      sql: `
      INSERT INTO ${SYSTEM_CHECKPOINT_TABLE} (
        checkpoint_key, payload, updated_at
      ) VALUES (?, ?, ?)
      ON CONFLICT (checkpoint_key) DO UPDATE SET
        payload = excluded.payload,
        updated_at = excluded.updated_at
      `,
      params: [TREND_ROLLUP_CHECKPOINT_KEY, checkpointPayload, updatedAt],
    },
  ];
}

function toTrendGameItem(row: TrendSubjectQueryRow) {
  return {
    id: row.subject_id,
    name: row.name || row.subject_id,
    localizedName: row.localized_name || undefined,
    cover: row.cover,
    releaseYear:
      row.release_year === null || row.release_year === undefined ? undefined : toNumber(row.release_year, 0) || undefined,
    count: toNumber(row.count, 0),
  };
}

function buildBucketsFromRankedRows(rows: RankedTrendBucketRow[]): TrendBucket[] {
  const buckets = new Map<string, TrendBucket>();

  for (const row of rows) {
    const existing = buckets.get(row.bucket_key);
    if (existing) {
      existing.games.push(toTrendGameItem(row));
      continue;
    }

    buckets.set(row.bucket_key, {
      key: row.bucket_key,
      label: row.bucket_label,
      count: toNumber(row.bucket_total, 0),
      games: [toTrendGameItem(row)],
    });
  }

  return Array.from(buckets.values());
}

async function loadOverallTrendBuckets(
  db: D1DatabaseLike,
  period: TrendPeriod,
  kind: SubjectKind,
  overallPage: number
): Promise<TrendBucket[]> {
  const countSource = buildTrendCountSourceQuery(period, kind);
  const rows = await queryAll<TrendSubjectQueryRow>(
    db,
    `
    WITH subject_counts AS (
      ${countSource.sql}
    )
    SELECT c.subject_id, c.count, d.name, d.localized_name, d.cover, d.release_year
    FROM subject_counts c
    JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = c.subject_id AND d.kind = c.kind
    ORDER BY c.count DESC, c.subject_id ASC
    LIMIT ?
    OFFSET ?
    `,
    [...countSource.params, OVERALL_TREND_PAGE_SIZE, Math.max(0, (overallPage - 1) * OVERALL_TREND_PAGE_SIZE)]
  );

  return rows.map((row, index) => ({
    key: String(index + 1),
    label: `#${index + 1}`,
    count: toNumber(row.count, 0),
    games: [toTrendGameItem(row)],
  }));
}

async function loadGenreTrendBuckets(
  db: D1DatabaseLike,
  period: TrendPeriod,
  kind: SubjectKind
): Promise<TrendBucket[]> {
  const countSource = buildTrendCountSourceQuery(period, kind);
  const genreFilterSql =
    kind === "manga"
      ? "g.genre <> '漫画'"
      : kind === "lightnovel"
        ? "g.genre NOT IN ('轻小说', '小说')"
        : null;

  const rows = await queryAll<RankedTrendBucketRow>(
    db,
    `
    WITH subject_counts AS (
      ${countSource.sql}
    ),
    subject_rows AS (
      SELECT sc.kind, sc.subject_id, sc.count, d.name, d.localized_name, d.cover, d.release_year
      FROM subject_counts sc
      JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = sc.subject_id AND d.kind = sc.kind
    ),
    bucketed AS (
      SELECT
        g.genre AS bucket_key,
        g.genre AS bucket_label,
        sr.subject_id,
        sr.count,
        sr.name,
        sr.localized_name,
        sr.cover,
        sr.release_year
      FROM subject_rows sr
      JOIN ${SUBJECT_GENRE_DIM_TABLE} g ON g.kind = sr.kind AND g.subject_id = sr.subject_id
      ${genreFilterSql ? `WHERE ${genreFilterSql}` : ""}

      UNION ALL

      SELECT
        '未分类' AS bucket_key,
        '未分类' AS bucket_label,
        sr.subject_id,
        sr.count,
        sr.name,
        sr.localized_name,
        sr.cover,
        sr.release_year
      FROM subject_rows sr
      WHERE NOT EXISTS (
        SELECT 1
        FROM ${SUBJECT_GENRE_DIM_TABLE} g
        WHERE g.kind = sr.kind
          AND g.subject_id = sr.subject_id
      )
    ),
    bucket_totals AS (
      SELECT
        bucket_key,
        bucket_label,
        subject_id,
        count,
        name,
        localized_name,
        cover,
        release_year,
        SUM(count) OVER (PARTITION BY bucket_key) AS bucket_total,
        ROW_NUMBER() OVER (PARTITION BY bucket_key ORDER BY count DESC, subject_id ASC) AS row_number
      FROM bucketed
    ),
    ranked AS (
      SELECT
        bucket_key,
        bucket_label,
        subject_id,
        count,
        name,
        localized_name,
        cover,
        release_year,
        bucket_total,
        DENSE_RANK() OVER (ORDER BY bucket_total DESC, bucket_label ASC) AS bucket_rank,
        row_number
      FROM bucket_totals
    )
    SELECT bucket_key, bucket_label, bucket_total, subject_id, count, name, localized_name, cover, release_year
    FROM ranked
    WHERE row_number <= ${GROUPED_TOP_GAMES_LIMIT}
      AND bucket_rank <= ${GROUPED_BUCKET_LIMIT}
    ORDER BY bucket_rank ASC, row_number ASC
    `,
    countSource.params
  );

  return buildBucketsFromRankedRows(rows);
}

async function loadTemporalTrendBuckets(
  db: D1DatabaseLike,
  period: TrendPeriod,
  kind: SubjectKind,
  view: Extract<TrendView, "year" | "decade">,
  yearPage: TrendYearPage
): Promise<TrendBucket[]> {
  const countSource = buildTrendCountSourceQuery(period, kind);
  const bucketKeySql =
    view === "year"
      ? "CAST(d.release_year AS TEXT)"
      : "CAST(CAST(d.release_year / 10 AS INTEGER) * 10 AS TEXT) || 's'";
  const sortKeySql = view === "year" ? "d.release_year" : "CAST(d.release_year / 10 AS INTEGER) * 10";
  const yearFilterSql = yearPage === "legacy" ? "AND d.release_year <= 2009" : "AND d.release_year >= 2010";

  const rows = await queryAll<RankedTrendBucketRow>(
    db,
    `
    WITH subject_counts AS (
      ${countSource.sql}
    ),
    bucketed AS (
      SELECT
        ${bucketKeySql} AS bucket_key,
        ${bucketKeySql} AS bucket_label,
        ${sortKeySql} AS sort_key,
        sc.subject_id,
        sc.count,
        d.name,
        d.localized_name,
        d.cover,
        d.release_year
      FROM subject_counts sc
      JOIN ${SUBJECT_DIM_TABLE} d ON d.subject_id = sc.subject_id AND d.kind = sc.kind
      WHERE d.release_year IS NOT NULL
      ${view === "year" ? yearFilterSql : ""}
    ),
    ranked AS (
      SELECT
        bucket_key,
        bucket_label,
        subject_id,
        count,
        name,
        localized_name,
        cover,
        release_year,
        sort_key,
        SUM(count) OVER (PARTITION BY bucket_key) AS bucket_total,
        ROW_NUMBER() OVER (PARTITION BY bucket_key ORDER BY count DESC, subject_id ASC) AS row_number
      FROM bucketed
    )
    SELECT bucket_key, bucket_label, bucket_total, subject_id, count, name, localized_name, cover, release_year
    FROM ranked
    WHERE row_number <= ${GROUPED_TOP_GAMES_LIMIT}
    ORDER BY sort_key DESC, row_number ASC
    `,
    countSource.params
  );

  return buildBucketsFromRankedRows(rows);
}

const d1StorageBackend: StorageBackend = {
  name: "d1",

  async saveShare(record) {
    const normalizedRecord = normalizeStoredShare(record);
    const { payload, subjectSnapshots } = toCompactSharePayload(normalizedRecord.games);
    const contentHash = createContentHash({
      kind: normalizedRecord.kind,
      creatorName: normalizedRecord.creatorName,
      payload,
    });

    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("saveShare failed: d1 is not ready");
    }

    const existingShareId = await resolveExistingShareIdByHash(db, contentHash);
    if (existingShareId) {
      return { shareId: existingShareId, deduped: true };
    }

    const useRealtimeTrendWrites = await shouldUseRealtimeTrendWrites(db);
    const increments = useRealtimeTrendWrites
      ? buildTrendIncrements({
          payload,
          createdAt: normalizedRecord.createdAt,
        })
      : [];
    const subjectRows = Array.from(subjectSnapshots.values());
    const existingSubjectRows = await fetchExistingSubjectDimRows(
      db,
      normalizedRecord.kind,
      subjectRows.map((snapshot) => snapshot.subjectId)
    );
    const existingSubjectGenres = await fetchExistingSubjectGenres(
      db,
      normalizedRecord.kind,
      subjectRows
        .filter((snapshot) => Boolean(snapshot.genres))
        .map((snapshot) => snapshot.subjectId)
    );
    const subjectDimStatements = subjectRows
      .map((snapshot) =>
        buildSubjectDimStatement({
          kind: normalizedRecord.kind,
          snapshot,
          existingRow: existingSubjectRows.get(snapshot.subjectId),
          updatedAt: normalizedRecord.updatedAt,
        })
      )
      .filter((statement): statement is StatementInput => Boolean(statement));
    const subjectGenreStatements = subjectRows.flatMap((snapshot) =>
      buildSubjectGenreStatements({
        kind: normalizedRecord.kind,
        snapshot,
        existingGenres: existingSubjectGenres.get(snapshot.subjectId),
        updatedAt: normalizedRecord.updatedAt,
      })
    );
    const slotRows = payload.flatMap((slot, slotIndex) =>
      slot
        ? [
            {
              slotIndex,
              subjectId: slot.sid,
            },
          ]
        : []
    );
    const trendStatements = useRealtimeTrendWrites
      ? [
          ...increments.map<StatementInput>((row) => ({
            sql: `
            INSERT INTO ${TREND_COUNT_ALL_TABLE} (kind, subject_id, count, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (kind, subject_id) DO UPDATE SET
              count = ${TREND_COUNT_ALL_TABLE}.count + excluded.count,
              updated_at = excluded.updated_at
            `,
            params: [normalizedRecord.kind, row.subjectId, row.count, normalizedRecord.updatedAt],
          })),
          ...increments.map<StatementInput>((row) => ({
            sql: `
            INSERT INTO ${TREND_COUNT_DAY_TABLE} (kind, day_key, subject_id, count, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (kind, day_key, subject_id) DO UPDATE SET
              count = ${TREND_COUNT_DAY_TABLE}.count + excluded.count,
              updated_at = excluded.updated_at
            `,
            params: [normalizedRecord.kind, row.dayKey, row.subjectId, row.count, normalizedRecord.updatedAt],
          })),
          ...increments.map<StatementInput>((row) => ({
            sql: `
            INSERT INTO ${TREND_COUNT_HOUR_TABLE} (kind, hour_bucket, subject_id, count, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (kind, hour_bucket, subject_id) DO UPDATE SET
              count = ${TREND_COUNT_HOUR_TABLE}.count + excluded.count,
              updated_at = excluded.updated_at
            `,
            params: [normalizedRecord.kind, row.hourBucket, row.subjectId, row.count, normalizedRecord.updatedAt],
          })),
        ]
      : [];

    const statements: StatementInput[] = [
      {
        sql: `
        INSERT INTO ${SHARES_V2_TABLE} (
          share_id, kind, creator_name, content_hash, hot_payload, created_at, updated_at, last_viewed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `,
        params: [
          normalizedRecord.shareId,
          normalizedRecord.kind,
          normalizedRecord.creatorName,
          contentHash,
          JSON.stringify(payload),
          normalizedRecord.createdAt,
          normalizedRecord.updatedAt,
          normalizedRecord.lastViewedAt,
        ],
      },
      ...subjectDimStatements,
      ...subjectGenreStatements,
      ...slotRows.map<StatementInput>((row) => ({
        sql: `
        INSERT INTO ${SHARE_SUBJECT_SLOT_TABLE} (
          share_id, kind, slot_index, subject_id, created_at, day_key, hour_bucket
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        params: [
          normalizedRecord.shareId,
          normalizedRecord.kind,
          row.slotIndex,
          row.subjectId,
          normalizedRecord.createdAt,
          toBeijingDayKey(normalizedRecord.createdAt),
          toBeijingHourBucket(normalizedRecord.createdAt),
        ],
      })),
      ...trendStatements,
    ];

    try {
      await executeBatch(db, statements);
      return { shareId: normalizedRecord.shareId, deduped: false };
    } catch (error) {
      const raceWinner = await resolveExistingShareIdByHash(db, contentHash);
      if (raceWinner) {
        return { shareId: raceWinner, deduped: true };
      }
      throwStorageError("saveShare failed: d1 write error", error);
    }
  },

  async getShare(shareId) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("getShare failed: d1 is not ready");
    }

    const row = await queryFirst<ShareRegistryRow>(
      db,
      `
      SELECT share_id, kind, creator_name, hot_payload, created_at, updated_at, last_viewed_at
      FROM ${SHARES_V2_TABLE}
      WHERE share_id = ?
      LIMIT 1
      `,
      [shareId]
    );
    if (row) {
      return await inflateShareFromRegistryRow(db, row);
    }

    const aliasRow = await queryFirst<{ target_share_id: string }>(
      db,
      `
      SELECT target_share_id
      FROM ${SHARE_ALIAS_TABLE}
      WHERE share_id = ?
      LIMIT 1
      `,
      [shareId]
    );
    if (!aliasRow?.target_share_id) {
      return null;
    }

    const targetRow = await queryFirst<ShareRegistryRow>(
      db,
      `
      SELECT share_id, kind, creator_name, hot_payload, created_at, updated_at, last_viewed_at
      FROM ${SHARES_V2_TABLE}
      WHERE share_id = ?
      LIMIT 1
      `,
      [aliasRow.target_share_id]
    );
    if (!targetRow) {
      return null;
    }

    const inflated = await inflateShareFromRegistryRow(db, targetRow);
    return inflated ? { ...inflated, shareId } : null;
  },

  async touchShare(shareId, now = Date.now()) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("touchShare failed: d1 is not ready");
    }

    const aliasRow = await queryFirst<{ target_share_id: string }>(
      db,
      `
      SELECT target_share_id
      FROM ${SHARE_ALIAS_TABLE}
      WHERE share_id = ?
      LIMIT 1
      `,
      [shareId]
    );
    const resolvedId = aliasRow?.target_share_id ?? shareId;
    return (
      (await execute(
        db,
        `
        UPDATE ${SHARES_V2_TABLE}
        SET updated_at = ?, last_viewed_at = ?
        WHERE share_id = ?
        `,
        [now, now, resolvedId]
      )) > 0
    );
  },

  async listAllShares() {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("listAllShares failed: d1 is not ready");
    }

    const rows = await queryAll<ShareRegistryRow>(
      db,
      `
      SELECT share_id, kind, creator_name, hot_payload, created_at, updated_at, last_viewed_at
      FROM ${SHARES_V2_TABLE}
      ORDER BY created_at DESC
      `
    );

    const result: StoredShareV1[] = [];
    for (const row of rows) {
      const inflated = await inflateShareFromRegistryRow(db, row);
      if (inflated) result.push(inflated);
    }
    return result;
  },

  async countAllShares() {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("countAllShares failed: d1 is not ready");
    }
    const row = await queryFirst<ShareCountRow>(
      db,
      `
      SELECT COUNT(*) AS total_count
      FROM ${SHARES_V2_TABLE}
      `
    );
    return toNumber(row?.total_count, 0);
  },

  async listSharesByPeriod(period) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("listSharesByPeriod failed: d1 is not ready");
    }

    const from = getPeriodStart(period);
    const rows =
      from > 0
        ? await queryAll<ShareRegistryRow>(
            db,
            `
            SELECT share_id, kind, creator_name, hot_payload, created_at, updated_at, last_viewed_at
            FROM ${SHARES_V2_TABLE}
            WHERE created_at >= ?
            ORDER BY created_at DESC
            `,
            [from]
          )
        : await queryAll<ShareRegistryRow>(
            db,
            `
            SELECT share_id, kind, creator_name, hot_payload, created_at, updated_at, last_viewed_at
            FROM ${SHARES_V2_TABLE}
            ORDER BY created_at DESC
            `
          );

    const result: StoredShareV1[] = [];
    for (const row of rows) {
      const inflated = await inflateShareFromRegistryRow(db, row);
      if (inflated) result.push(inflated);
    }
    return result;
  },

  async getAggregatedTrendResponse(params) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      return null;
    }

    const fromTimestamp = getPeriodStart(params.period);
    const trendRollupCheckpoint = await getTrendRollupCheckpoint(db);
    const sampleQuery = buildTrendSampleQuery(params.period, Boolean(trendRollupCheckpoint));
    const sampleRow = await queryFirst<TrendSampleRow>(
      db,
      sampleQuery.sql,
      sampleQuery.bindValues(params.kind, fromTimestamp, trendRollupCheckpoint)
    );

    const sampleCount = toNumber(sampleRow?.sample_count, 0);
    const rangeFrom = sampleRow?.min_created === null ? null : toNumber(sampleRow?.min_created, 0) || null;
    const rangeTo = sampleRow?.max_created === null ? null : toNumber(sampleRow?.max_created, 0) || null;
    const lastUpdatedAt = await resolveTrendLastUpdatedAt(db, params.period, params.kind);

    if (sampleCount === 0) {
      return {
        period: params.period,
        view: params.view,
        sampleCount,
        range: { from: rangeFrom, to: rangeTo },
        lastUpdatedAt,
        items: [],
      };
    }

    const items =
      params.view === "overall"
        ? await loadOverallTrendBuckets(db, params.period, params.kind, params.overallPage)
        : params.view === "genre"
          ? await loadGenreTrendBuckets(db, params.period, params.kind)
          : await loadTemporalTrendBuckets(db, params.period, params.kind, params.view, params.yearPage);

    return {
      period: params.period,
      view: params.view,
      sampleCount,
      range: { from: rangeFrom, to: rangeTo },
      lastUpdatedAt,
      items,
    };
  },

  async getTrendSampleSummary(period, kind) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      return null;
    }

    const fromTimestamp = getPeriodStart(period);
    const trendRollupCheckpoint = await getTrendRollupCheckpoint(db);
    const sampleQuery = buildTrendSampleQuery(period, Boolean(trendRollupCheckpoint));
    const row = await queryFirst<TrendSampleRow>(
      db,
      sampleQuery.sql,
      sampleQuery.bindValues(kind, fromTimestamp, trendRollupCheckpoint)
    );

    return {
      sampleCount: toNumber(row?.sample_count, 0),
      range: {
        from: row?.min_created === null ? null : toNumber(row?.min_created, 0) || null,
        to: row?.max_created === null ? null : toNumber(row?.max_created, 0) || null,
      },
    };
  },

  async getTrendSampleSummaryCache(period, kind, options) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("getTrendSampleSummaryCache failed: d1 is not ready");
    }

    const key = trendSampleCacheKey(period, kind);
    const row = await queryFirst<TrendCacheRow>(
      db,
      `
      SELECT payload, expires_at, updated_at
      FROM ${TRENDS_CACHE_TABLE}
      WHERE cache_key = ?
      LIMIT 1
      `,
      [key]
    );
    if (!row) {
      return null;
    }

    const expiresAt = toNumber(row.expires_at, 0);
    const updatedAt = resolveTrendCacheUpdatedAt(expiresAt, row.updated_at);
    if (isTrendCacheExpired(expiresAt, updatedAt, Date.now()) && options?.allowExpired !== true) {
      await execute(
        db,
        `
        DELETE FROM ${TRENDS_CACHE_TABLE}
        WHERE cache_key = ?
        `,
        [key]
      );
      return null;
    }

    return parseTrendSampleSummaryPayload(row.payload);
  },

  async setTrendSampleSummaryCache(period, kind, value, ttlSeconds = 3600) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("setTrendSampleSummaryCache failed: d1 is not ready");
    }

    const key = trendSampleCacheKey(period, kind);
    const updatedAt = Date.now();
    const expiresAt = updatedAt + ttlSeconds * 1000;
    await execute(
      db,
      `
      INSERT INTO ${TRENDS_CACHE_TABLE} (
        cache_key, period, view, kind, payload, expires_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (cache_key) DO UPDATE SET
        period = excluded.period,
        view = excluded.view,
        kind = excluded.kind,
        payload = excluded.payload,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at
      `,
      [key, period, SAMPLE_SUMMARY_CACHE_VIEW, kind, JSON.stringify(value), expiresAt, updatedAt]
    );
  },

  async getTrendsCache(period, view, kind, overallPage, yearPage, options) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("getTrendsCache failed: d1 is not ready");
    }

    const key = trendCacheKey(period, view, kind, overallPage, yearPage);
    const row = await queryFirst<TrendCacheRow>(
      db,
      `
      SELECT payload, expires_at, updated_at
      FROM ${TRENDS_CACHE_TABLE}
      WHERE cache_key = ?
      LIMIT 1
      `,
      [key]
    );
    if (!row) {
      return null;
    }

    const expiresAt = toNumber(row.expires_at, 0);
    const updatedAt = resolveTrendCacheUpdatedAt(expiresAt, row.updated_at);
    if (isTrendCacheExpired(expiresAt, updatedAt, Date.now()) && options?.allowExpired !== true) {
      await execute(
        db,
        `
        DELETE FROM ${TRENDS_CACHE_TABLE}
        WHERE cache_key = ?
        `,
        [key]
      );
      return null;
    }

    return parseTrendPayload(row.payload);
  },

  async setTrendsCache(period, view, kind, overallPage, yearPage, value, ttlSeconds = 3600) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("setTrendsCache failed: d1 is not ready");
    }

    const updatedAt = Date.now();
    const expiresAt = updatedAt + ttlSeconds * 1000;
    await execute(
      db,
      `
      INSERT INTO ${TRENDS_CACHE_TABLE} (
        cache_key, period, view, kind, payload, expires_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (cache_key) DO UPDATE SET
        period = excluded.period,
        view = excluded.view,
        kind = excluded.kind,
        payload = excluded.payload,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at
      `,
      [trendCacheKey(period, view, kind, overallPage, yearPage), period, view, kind, JSON.stringify(value), expiresAt, updatedAt]
    );
  },

  async getShareViewRollupCheckpoint() {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("getShareViewRollupCheckpoint failed: d1 is not ready");
    }

    const systemCheckpoint = await getSystemCheckpoint(
      db,
      SHARE_VIEW_ROLLUP_CHECKPOINT_KEY,
      (value) => toOptionalNumber(parseJsonValue<{ rolledThroughMs?: number | string }>(value)?.rolledThroughMs)
    );
    if (systemCheckpoint !== null) {
      return systemCheckpoint;
    }

    const checkpointRow = await queryFirst<ShareViewRollupCheckpointRow>(
      db,
      `
      SELECT payload
      FROM ${TRENDS_CACHE_TABLE}
      WHERE cache_key = ?
      LIMIT 1
      `,
      [SHARE_VIEW_ROLLUP_CHECKPOINT_KEY]
    );
    const parsedPayload = parseJsonValue<{ rolledThroughMs?: number | string }>(checkpointRow?.payload);
    const checkpointMs = toOptionalNumber(parsedPayload?.rolledThroughMs);
    if (checkpointMs !== null) {
      return checkpointMs;
    }

    const fallbackRow = await queryFirst<ShareViewAggregationRow>(
      db,
      `
      SELECT MAX(last_aggregated_at) AS last_aggregated_at
      FROM ${SHARE_VIEW_TOTAL_TABLE}
      `
    );
    return toOptionalNumber(fallbackRow?.last_aggregated_at);
  },

  async setShareViewRollupCheckpoint(checkpointMs) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("setShareViewRollupCheckpoint failed: d1 is not ready");
    }

    await setSystemCheckpoint(db, SHARE_VIEW_ROLLUP_CHECKPOINT_KEY, {
      rolledThroughMs: checkpointMs,
    });
  },

  async upsertShareViewTotalCounts(rows, options) {
    const normalizedRows = rows
      .map((row) => ({
        shareId: typeof row.shareId === "string" ? row.shareId.trim().toLowerCase() : "",
        kind: parseSubjectKind(row.kind) ?? null,
        viewCount: Number.isFinite(row.viewCount) ? Math.trunc(row.viewCount) : 0,
      }))
      .filter((row) => row.shareId && row.kind && row.viewCount > 0) as Array<{
      shareId: string;
      kind: SubjectKind;
      viewCount: number;
    }>;

    if (normalizedRows.length === 0) {
      return 0;
    }

    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("upsertShareViewTotalCounts failed: d1 is not ready");
    }

    const lastAggregatedAt = Number.isFinite(options?.lastAggregatedAt)
      ? Math.trunc(options?.lastAggregatedAt ?? Date.now())
      : Date.now();
    const mode = options?.mode === "increment" ? "increment" : "replace";
    const folded = new Map<string, { shareId: string; kind: SubjectKind; viewCount: number }>();
    for (const row of normalizedRows) {
      const key = `${row.shareId}:${row.kind}`;
      const current = folded.get(key);
      if (current) {
        current.viewCount += row.viewCount;
      } else {
        folded.set(key, { ...row });
      }
    }

    const shareIds = Array.from(new Set(Array.from(folded.values()).map((row) => row.shareId)));
    const aliasMap = new Map<string, string>();
    for (const ids of chunkArray(shareIds, D1_IN_CLAUSE_CHUNK_SIZE)) {
      if (ids.length === 0) continue;
      const rows = await queryAll<{ share_id: string; target_share_id: string }>(
        db,
        `
        SELECT share_id, target_share_id
        FROM ${SHARE_ALIAS_TABLE}
        WHERE share_id IN (${buildPlaceholders(ids.length)})
        `,
        ids
      );
      for (const row of rows) {
        aliasMap.set(row.share_id, row.target_share_id);
      }
    }

    const resolvedIds = Array.from(new Set(shareIds.map((shareId) => aliasMap.get(shareId) ?? shareId)));
    const shareKindMap = new Map<string, SubjectKind>();
    for (const ids of chunkArray(resolvedIds, D1_IN_CLAUSE_CHUNK_SIZE)) {
      if (ids.length === 0) continue;
      const rows = await queryAll<{ share_id: string; kind: string }>(
        db,
        `
        SELECT share_id, kind
        FROM ${SHARES_V2_TABLE}
        WHERE share_id IN (${buildPlaceholders(ids.length)})
        `,
        ids
      );
      for (const row of rows) {
        const kind = parseSubjectKind(row.kind);
        if (kind) {
          shareKindMap.set(row.share_id, kind);
        }
      }
    }

    const finalRows = new Map<string, { shareId: string; kind: SubjectKind; viewCount: number }>();
    for (const row of folded.values()) {
      const resolvedShareId = aliasMap.get(row.shareId) ?? row.shareId;
      const resolvedKind = shareKindMap.get(resolvedShareId) ?? row.kind;
      const key = `${resolvedShareId}:${resolvedKind}`;
      const current = finalRows.get(key);
      if (current) {
        current.viewCount += row.viewCount;
      } else {
        finalRows.set(key, { shareId: resolvedShareId, kind: resolvedKind, viewCount: row.viewCount });
      }
    }

    await executeBatch(
      db,
      Array.from(finalRows.values()).map<StatementInput>((row) => ({
        sql: `
        INSERT INTO ${SHARE_VIEW_TOTAL_TABLE} (
          share_id, kind, view_count, last_aggregated_at
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT (share_id) DO UPDATE SET
          kind = excluded.kind,
          view_count = ${mode === "increment" ? `${SHARE_VIEW_TOTAL_TABLE}.view_count + excluded.view_count` : "excluded.view_count"},
          last_aggregated_at = excluded.last_aggregated_at
        `,
        params: [row.shareId, row.kind, row.viewCount, lastAggregatedAt],
      }))
    );
    return finalRows.size;
  },

  async runTrendRollup(options): Promise<TrendRollupResult> {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      throw new Error("runTrendRollup failed: d1 is not ready");
    }

    const batchSize = Math.max(1, Math.min(2000, Math.trunc(options?.batchSize ?? DEFAULT_TREND_ROLLUP_BATCH_SIZE)));
    const updatedAt = Number.isFinite(options?.nowMs) ? Math.trunc(options?.nowMs ?? Date.now()) : Date.now();
    let checkpoint = await getTrendRollupCheckpoint(db);

    if (!checkpoint) {
      const head = await resolveTrendRollupHead(db);
      if (!head) {
        return {
          ok: true,
          mode: "idle",
          rowsFetched: 0,
          rowsWritten: 0,
          checkpoint: null,
        };
      }

      if (await hasExistingTrendAggregates(db)) {
        await setTrendRollupCheckpoint(db, head);
        return {
          ok: true,
          mode: "bootstrap-skip",
          rowsFetched: 0,
          rowsWritten: 1,
          checkpoint: head,
        };
      }
    }

    let rowsFetched = 0;
    let rowsWritten = 0;

    for (;;) {
      const sourceRows = await fetchTrendRollupSourceRows(db, checkpoint, batchSize);
      if (sourceRows.length === 0) {
        break;
      }

      const nextCheckpoint = toTrendRollupCheckpoint(sourceRows[sourceRows.length - 1]);
      const statements = buildTrendRollupStatements(checkpoint, nextCheckpoint, updatedAt);
      rowsFetched += sourceRows.length;
      if (statements.length > 0) {
        rowsWritten += await executeBatch(db, statements);
      }

      checkpoint = nextCheckpoint;
      trendRollupCheckpointCache = {
        expiresAt: Date.now() + TREND_ROLLUP_CHECKPOINT_CACHE_TTL_MS,
        hasCheckpoint: true,
      };
      rowsWritten += 1;
    }

    return {
      ok: true,
      mode: rowsFetched > 0 ? "rollup" : "idle",
      rowsFetched,
      rowsWritten,
      checkpoint,
    };
  },

  async cleanupOldTrendCounts(params) {
    const cleanupTrendDays = Math.max(
      180,
      params?.cleanupTrendDays ?? parsePositiveInt(readEnv("MY9_TREND_CLEANUP_DAYS"), 190)
    );
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      return {
        cleanupTrendDays,
        cleanedTrendRows: 0,
        cleanedDayRows: 0,
        cleanedHourRows: 0,
      };
    }

    const cleanupBeforeDayKey = toBeijingDayKey(Date.now() - cleanupTrendDays * 24 * 60 * 60 * 1000);
    const cleanupBeforeHourBucket = toBeijingHourBucket(getBeijingDayStart(Date.now()) - 24 * 60 * 60 * 1000);
    const cleanedDayRows = await execute(
      db,
      `
      DELETE FROM ${TREND_COUNT_DAY_TABLE}
      WHERE day_key < ?
      `,
      [cleanupBeforeDayKey]
    );
    const cleanedHourRows = await execute(
      db,
      `
      DELETE FROM ${TREND_COUNT_HOUR_TABLE}
      WHERE hour_bucket < ?
      `,
      [cleanupBeforeHourBucket]
    );

    return {
      cleanupTrendDays,
      cleanedTrendRows: cleanedDayRows + cleanedHourRows,
      cleanedDayRows,
      cleanedHourRows,
    };
  },
};

export default d1StorageBackend;
