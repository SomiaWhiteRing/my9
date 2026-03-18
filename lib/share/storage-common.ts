import type { SubjectSnapshot } from "@/lib/share/compact";
import type { TrendSampleSummary } from "@/lib/share/storage-contract";
import type {
  ShareSubject,
  StoredShareV1,
  TrendBucket,
  TrendGameItem,
  TrendPeriod,
  TrendResponse,
  TrendView,
  TrendYearPage,
} from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, type SubjectKind, parseSubjectKind } from "@/lib/subject-kind";

export const TRENDS_CACHE_PREFIX = "trends:cache:";
export const TRENDS_SAMPLE_CACHE_PREFIX = "trends:sample:";

export const SHARES_V2_TABLE = "my9_share_registry_v2";
export const SHARE_ALIAS_TABLE = "my9_share_alias_v1";
export const SUBJECT_DIM_TABLE = "my9_subject_dim_v1";
export const SUBJECT_GENRE_DIM_TABLE = "my9_subject_genre_dim_v1";
export const SHARE_SUBJECT_SLOT_TABLE = "my9_share_subject_slot_v1";
export const TREND_COUNT_ALL_TABLE = "my9_trend_subject_kind_all_v3";
export const TREND_COUNT_DAY_TABLE = "my9_trend_subject_kind_day_v3";
export const TREND_COUNT_HOUR_TABLE = "my9_trend_subject_kind_hour_v3";
export const TRENDS_CACHE_TABLE = "my9_trends_cache_v1";
export const SHARE_VIEW_TOTAL_TABLE = "my9_share_view_total_v1";
export const TRENDS_CACHE_VERSION = "v9";
export const TRENDS_SAMPLE_CACHE_VERSION = "v5";
export const SAMPLE_SUMMARY_CACHE_VIEW = "sample";
export const OVERALL_TREND_PAGE_SIZE = 20;
export const GROUPED_BUCKET_LIMIT = 20;
export const DAY_MS = 24 * 60 * 60 * 1000;
export const HOUR_MS = 60 * 60 * 1000;
export const HALF_HOUR_MS = 30 * 60 * 1000;
export const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;
export const TREND_CACHE_COMPAT_TTL_MS = 60 * 60 * 1000;
export const MAX_BATCH_SIZE = 96;
export const TREND_24H_SOURCE = readEnv("MY9_TRENDS_24H_SOURCE") === "hour" ? "hour" : "day";

export type ShareRegistryRow = {
  share_id: string;
  kind: string;
  creator_name: string | null;
  hot_payload: unknown;
  created_at: number | string;
  updated_at: number | string;
  last_viewed_at: number | string;
};

export type SubjectDimRow = {
  subject_id: string;
  name: string;
  localized_name: string | null;
  cover: string | null;
  release_year: number | string | null;
  genres: unknown;
};

export type TrendSampleRow = {
  sample_count: number | string;
  min_created: number | string | null;
  max_created: number | string | null;
};

export type TrendSubjectSummary = {
  subjectId: string;
  count: number;
  name: string;
  localizedName?: string;
  cover: string | null;
  releaseYear?: number;
  genres: string[];
};

export type TrendIncrement = {
  dayKey: number;
  hourBucket: number;
  subjectId: string;
  count: number;
};

export function readEnv(...names: string[]): string | null {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

export function parsePositiveInt(value: string | null | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
}

export function toNumber(value: unknown, fallback = 0): number {
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

export function normalizeStoredShare(input: StoredShareV1): StoredShareV1 {
  return {
    ...input,
    kind: parseSubjectKind(input.kind) ?? DEFAULT_SUBJECT_KIND,
  };
}

export function parseJsonValue<T>(value: unknown): T | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return null;
    }
  }
  if (typeof value === "object") {
    return value as T;
  }
  return null;
}

export function parseStringArray(value: unknown): string[] {
  const parsed = parseJsonValue<unknown>(value);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((item) => (typeof item === "string" ? item.trim() : ""))
    .filter((item) => Boolean(item));
}

export function toBeijingDayKey(timestampMs: number): number {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

export function toBeijingHourBucket(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS);
}

export function getBeijingDayStart(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / DAY_MS) * DAY_MS - BEIJING_TZ_OFFSET_MS;
}

export function getBeijingHourStart(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS) * HOUR_MS - BEIJING_TZ_OFFSET_MS;
}

export function toBeijingTrendRefreshBucket(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS + HALF_HOUR_MS) / HOUR_MS);
}

export function getPeriodStart(period: TrendPeriod, now = Date.now()): number {
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

export function resolveTrendCacheUpdatedAt(expiresAt: number, updatedAt: unknown): number {
  if (typeof updatedAt === "number" && Number.isFinite(updatedAt) && updatedAt > 0) {
    return Math.trunc(updatedAt);
  }
  if (typeof updatedAt === "string") {
    const parsed = Number(updatedAt);
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.trunc(parsed);
    }
  }
  const inferred = expiresAt - TREND_CACHE_COMPAT_TTL_MS;
  if (Number.isFinite(inferred) && inferred > 0) {
    return Math.trunc(inferred);
  }
  return Date.now();
}

export function isTrendCacheExpired(expiresAt: number, updatedAt: number, nowMs: number): boolean {
  if (nowMs > expiresAt) {
    return true;
  }
  return toBeijingTrendRefreshBucket(nowMs) !== toBeijingTrendRefreshBucket(updatedAt);
}

export function trendCacheKey(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  overallPage: number,
  yearPage: TrendYearPage
) {
  const sourcePart = period === "24h" ? `:src${TREND_24H_SOURCE}` : "";
  return `${TRENDS_CACHE_PREFIX}${TRENDS_CACHE_VERSION}:${period}:${view}:${kind}${sourcePart}:op${overallPage}:yp${yearPage}`;
}

export function trendSampleCacheKey(period: TrendPeriod, kind: SubjectKind) {
  const sourcePart = period === "24h" ? `:src${TREND_24H_SOURCE}` : "";
  return `${TRENDS_SAMPLE_CACHE_PREFIX}${TRENDS_SAMPLE_CACHE_VERSION}:${period}:${kind}${sourcePart}`;
}

export function parseTrendPayload(value: unknown): TrendResponse | null {
  const data = parseJsonValue<Partial<TrendResponse>>(value);
  if (!data || !data.period || !data.view || !data.range || !Array.isArray(data.items)) {
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

export function parseTrendSampleSummaryPayload(value: unknown): TrendSampleSummary | null {
  const parsed = parseJsonValue<Partial<TrendSampleSummary>>(value);
  if (!parsed || typeof parsed.sampleCount !== "number" || !parsed.range) {
    return null;
  }

  return {
    sampleCount: parsed.sampleCount,
    range: {
      from: typeof parsed.range.from === "number" ? parsed.range.from : null,
      to: typeof parsed.range.to === "number" ? parsed.range.to : null,
    },
  };
}

export function normalizeGames(value: unknown): Array<ShareSubject | null> {
  if (!Array.isArray(value)) {
    return Array.from({ length: 9 }, () => null);
  }

  const next = Array.from({ length: 9 }, () => null as ShareSubject | null);
  for (let index = 0; index < 9; index += 1) {
    const item = value[index];
    next[index] = item && typeof item === "object" ? (item as ShareSubject) : null;
  }
  return next;
}

export function collectSubjectIdsFromPayload(payload: Array<{ sid: string } | null>): string[] {
  const unique = new Set<string>();
  for (const slot of payload) {
    if (!slot) continue;
    unique.add(slot.sid);
  }
  return Array.from(unique);
}

export function toSubjectSnapshot(row: SubjectDimRow): SubjectSnapshot {
  const genres = parseStringArray(row.genres);

  return {
    subjectId: row.subject_id,
    name: row.name,
    localizedName: row.localized_name || undefined,
    cover: row.cover,
    releaseYear:
      row.release_year === null || row.release_year === undefined ? undefined : toNumber(row.release_year, 0) || undefined,
    genres: genres.length > 0 ? genres : undefined,
  };
}

export function buildTrendIncrements(params: {
  payload: Array<{ sid: string } | null>;
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

export function sortTrendSubjects(items: TrendSubjectSummary[]): TrendSubjectSummary[] {
  return items.slice().sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    return a.subjectId.localeCompare(b.subjectId);
  });
}

export function createTrendGameItem(row: TrendSubjectSummary): TrendGameItem {
  return {
    id: row.subjectId,
    name: row.name,
    localizedName: row.localizedName,
    cover: row.cover,
    releaseYear: row.releaseYear,
    count: row.count,
  };
}

export function buildOverallBuckets(rows: TrendSubjectSummary[], overallPage: number): TrendBucket[] {
  const offset = Math.max(0, (overallPage - 1) * OVERALL_TREND_PAGE_SIZE);
  const pageRows = sortTrendSubjects(rows).slice(offset, offset + OVERALL_TREND_PAGE_SIZE);
  return pageRows.map((row, index) => ({
    key: String(index + 1),
    label: `#${index + 1}`,
    count: row.count,
    games: [createTrendGameItem(row)],
  }));
}

export function isExcludedGenre(kind: SubjectKind, genre: string): boolean {
  if (kind === "manga") {
    return genre === "漫画";
  }
  if (kind === "lightnovel") {
    return genre === "轻小说" || genre === "小说";
  }
  return false;
}

export function buildGenreBuckets(rows: TrendSubjectSummary[], kind: SubjectKind): TrendBucket[] {
  const buckets = new Map<string, { count: number; games: TrendGameItem[] }>();

  for (const row of rows) {
    const rawGenres = row.genres.length > 0 ? row.genres : ["未分类"];
    const effectiveGenres = rawGenres.filter((genre) => !isExcludedGenre(kind, genre));
    if (effectiveGenres.length === 0) continue;

    for (const genre of effectiveGenres) {
      const current = buckets.get(genre) ?? { count: 0, games: [] };
      current.count += row.count;
      current.games.push(createTrendGameItem(row));
      buckets.set(genre, current);
    }
  }

  return Array.from(buckets.entries())
    .map(([genre, value]) => ({
      key: genre,
      label: genre,
      count: value.count,
      games: value.games
        .slice()
        .sort((a, b) => (b.count !== a.count ? b.count - a.count : a.id.localeCompare(b.id)))
        .slice(0, 5),
    }))
    .sort((a, b) => (b.count !== a.count ? b.count - a.count : a.label.localeCompare(b.label)))
    .slice(0, GROUPED_BUCKET_LIMIT);
}

export function buildYearBuckets(rows: TrendSubjectSummary[], yearPage: TrendYearPage): TrendBucket[] {
  const buckets = new Map<number, { count: number; games: TrendGameItem[] }>();

  for (const row of rows) {
    if (typeof row.releaseYear !== "number" || !Number.isFinite(row.releaseYear)) continue;
    if (yearPage === "legacy" ? row.releaseYear > 2009 : row.releaseYear < 2010) continue;
    const current = buckets.get(row.releaseYear) ?? { count: 0, games: [] };
    current.count += row.count;
    current.games.push(createTrendGameItem(row));
    buckets.set(row.releaseYear, current);
  }

  return Array.from(buckets.entries())
    .sort((a, b) => b[0] - a[0])
    .map(([year, value]) => ({
      key: String(year),
      label: String(year),
      count: value.count,
      games: value.games
        .slice()
        .sort((a, b) => (b.count !== a.count ? b.count - a.count : a.id.localeCompare(b.id)))
        .slice(0, 5),
    }));
}

export function buildDecadeBuckets(rows: TrendSubjectSummary[]): TrendBucket[] {
  const buckets = new Map<number, { count: number; games: TrendGameItem[] }>();

  for (const row of rows) {
    if (typeof row.releaseYear !== "number" || !Number.isFinite(row.releaseYear)) continue;
    const decade = Math.floor(row.releaseYear / 10) * 10;
    const current = buckets.get(decade) ?? { count: 0, games: [] };
    current.count += row.count;
    current.games.push(createTrendGameItem(row));
    buckets.set(decade, current);
  }

  return Array.from(buckets.entries())
    .sort((a, b) => b[0] - a[0])
    .map(([decade, value]) => ({
      key: `${decade}s`,
      label: `${decade}s`,
      count: value.count,
      games: value.games
        .slice()
        .sort((a, b) => (b.count !== a.count ? b.count - a.count : a.id.localeCompare(b.id)))
        .slice(0, 5),
    }));
}

export function chunkArray<T>(items: T[], size = MAX_BATCH_SIZE): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

export function throwStorageError(context: string, cause?: unknown): never {
  if (cause instanceof Error) {
    throw new Error(`${context}: ${cause.message}`);
  }
  throw new Error(context);
}
