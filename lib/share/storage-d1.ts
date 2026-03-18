import {
  compactPayloadToGames,
  createContentHash,
  normalizeCompactPayload,
  toCompactSharePayload,
} from "@/lib/share/compact";
import type { StorageBackend } from "@/lib/share/storage-contract";
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

const GROUPED_TOP_GAMES_LIMIT = 5;

function resolveCompactPayload(row: ShareRegistryRow) {
  return normalizeCompactPayload(parseJsonValue<unknown>(row.hot_payload));
}

async function fetchSubjectSnapshots(db: D1DatabaseLike, kind: SubjectKind, subjectIds: string[]) {
  const snapshots = new Map<string, ReturnType<typeof toSubjectSnapshot>>();
  for (const ids of chunkArray(subjectIds, 300)) {
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

    const increments = buildTrendIncrements({
      payload,
      createdAt: normalizedRecord.createdAt,
    });
    const subjectRows = Array.from(subjectSnapshots.values());
    const genreRows = subjectRows.flatMap((snapshot) =>
      (snapshot.genres ?? []).map((genre) => ({
        subjectId: snapshot.subjectId,
        genre,
      }))
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
      ...subjectRows.map<StatementInput>((snapshot) => ({
        sql: `
        INSERT INTO ${SUBJECT_DIM_TABLE} (
          kind, subject_id, name, localized_name, cover, release_year, genres, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (kind, subject_id) DO UPDATE SET
          name = excluded.name,
          localized_name = COALESCE(excluded.localized_name, ${SUBJECT_DIM_TABLE}.localized_name),
          cover = COALESCE(excluded.cover, ${SUBJECT_DIM_TABLE}.cover),
          release_year = COALESCE(excluded.release_year, ${SUBJECT_DIM_TABLE}.release_year),
          genres = COALESCE(excluded.genres, ${SUBJECT_DIM_TABLE}.genres),
          updated_at = excluded.updated_at
        `,
        params: [
          normalizedRecord.kind,
          snapshot.subjectId,
          snapshot.name,
          snapshot.localizedName ?? null,
          snapshot.cover,
          snapshot.releaseYear ?? null,
          snapshot.genres && snapshot.genres.length > 0 ? JSON.stringify(snapshot.genres) : null,
          normalizedRecord.updatedAt,
        ],
      })),
      ...subjectRows.map<StatementInput>((snapshot) => ({
        sql: `
        DELETE FROM ${SUBJECT_GENRE_DIM_TABLE}
        WHERE kind = ?
          AND subject_id = ?
        `,
        params: [normalizedRecord.kind, snapshot.subjectId],
      })),
      ...genreRows.map<StatementInput>((row) => ({
        sql: `
        INSERT OR IGNORE INTO ${SUBJECT_GENRE_DIM_TABLE} (
          kind, subject_id, genre, updated_at
        ) VALUES (?, ?, ?, ?)
        `,
        params: [normalizedRecord.kind, row.subjectId, row.genre, normalizedRecord.updatedAt],
      })),
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
    const sampleRow = await queryFirst<TrendSampleRow>(
      db,
      fromTimestamp > 0
        ? `
          SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
          FROM ${SHARES_V2_TABLE}
          WHERE kind = ?
            AND created_at >= ?
          `
        : `
          SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
          FROM ${SHARES_V2_TABLE}
          WHERE kind = ?
          `,
      fromTimestamp > 0 ? [params.kind, fromTimestamp] : [params.kind]
    );

    const sampleCount = toNumber(sampleRow?.sample_count, 0);
    const rangeFrom = sampleRow?.min_created === null ? null : toNumber(sampleRow?.min_created, 0) || null;
    const rangeTo = sampleRow?.max_created === null ? null : toNumber(sampleRow?.max_created, 0) || null;

    if (sampleCount === 0) {
      return {
        period: params.period,
        view: params.view,
        sampleCount,
        range: { from: rangeFrom, to: rangeTo },
        lastUpdatedAt: Date.now(),
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
      lastUpdatedAt: Date.now(),
      items,
    };
  },

  async getTrendSampleSummary(period, kind) {
    const db = await getD1Database();
    if (!db || !(await ensureD1Schema())) {
      return null;
    }

    const fromTimestamp = getPeriodStart(period);
    const row = await queryFirst<TrendSampleRow>(
      db,
      fromTimestamp > 0
        ? `
          SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
          FROM ${SHARES_V2_TABLE}
          WHERE kind = ?
            AND created_at >= ?
          `
        : `
          SELECT COUNT(*) AS sample_count, MIN(created_at) AS min_created, MAX(created_at) AS max_created
          FROM ${SHARES_V2_TABLE}
          WHERE kind = ?
          `,
      fromTimestamp > 0 ? [kind, fromTimestamp] : [kind]
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
    for (const ids of chunkArray(shareIds, 300)) {
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
    for (const ids of chunkArray(resolvedIds, 300)) {
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
          view_count = excluded.view_count,
          last_aggregated_at = excluded.last_aggregated_at
        `,
        params: [row.shareId, row.kind, row.viewCount, lastAggregatedAt],
      }))
    );
    return finalRows.size;
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
