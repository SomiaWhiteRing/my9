import {
  getAggregatedTrendResponse,
  getTrendSampleSummary,
  getTrendSampleSummaryCache,
  getTrendsCache,
  setTrendSampleSummaryCache,
  setTrendsCache,
} from "@/lib/share/storage";
import { TrendPeriod, TrendResponse, TrendView, TrendYearPage } from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";

export const VALID_TREND_PERIODS: TrendPeriod[] = ["today", "24h", "7d", "30d", "90d", "180d", "all"];
export const VALID_TREND_VIEWS: TrendView[] = ["overall", "genre", "decade", "year"];
export const DEFAULT_TREND_PERIOD: TrendPeriod = "24h";
export const DEFAULT_TREND_VIEW: TrendView = "overall";
export const DEFAULT_TREND_KIND: SubjectKind = DEFAULT_SUBJECT_KIND;
export const DEFAULT_TREND_OVERALL_PAGE = 1;
export const DEFAULT_TREND_YEAR_PAGE: TrendYearPage = "recent";
const MAX_TREND_OVERALL_PAGE = 5;
const OVERALL_ONLY_TREND_KINDS = new Set<SubjectKind>(["character", "person"]);

export const TRENDS_STORE_CACHE_TTL_SECONDS = 3600;

type TrendSampleSummary = {
  sampleCount: number;
  range: { from: number | null; to: number | null };
};

type ResolveTrendParams = {
  period: TrendPeriod;
  view: TrendView;
  kind: SubjectKind;
  overallPage: number;
  yearPage: TrendYearPage;
};

function applySampleSummary(response: TrendResponse, summary: TrendSampleSummary | null): TrendResponse {
  if (!summary) {
    return response;
  }

  const sampleCount = Math.max(response.sampleCount, summary.sampleCount);
  const rangeFromCandidates = [response.range.from, summary.range.from].filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value)
  );
  const rangeToCandidates = [response.range.to, summary.range.to].filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value)
  );

  return {
    ...response,
    sampleCount,
    range: {
      from: rangeFromCandidates.length > 0 ? Math.min(...rangeFromCandidates) : null,
      to: rangeToCandidates.length > 0 ? Math.max(...rangeToCandidates) : null,
    },
  };
}

function toSampleSummary(response: TrendResponse): TrendSampleSummary {
  return {
    sampleCount: response.sampleCount,
    range: response.range,
  };
}

function isSameSampleSummary(a: TrendSampleSummary | null, b: TrendSampleSummary | null): boolean {
  if (!a || !b) return false;
  return (
    a.sampleCount === b.sampleCount &&
    a.range.from === b.range.from &&
    a.range.to === b.range.to
  );
}

function suppressSmallSamples(response: TrendResponse): TrendResponse {
  if (response.sampleCount < 30) {
    return {
      ...response,
      items: [],
    };
  }
  return response;
}

function createEmptyTrendResponse(params: ResolveTrendParams, summary: TrendSampleSummary | null): TrendResponse {
  return {
    period: params.period,
    view: params.view,
    sampleCount: summary?.sampleCount ?? 0,
    range: summary?.range ?? { from: null, to: null },
    lastUpdatedAt: Date.now(),
    items: [],
  };
}

function resolveInflightKey(params: ResolveTrendParams): string {
  return `${params.period}:${params.view}:${params.kind}:op${params.overallPage}:yp${params.yearPage}`;
}

function getInflightMap(): Map<string, Promise<TrendResponse>> {
  const g = globalThis as typeof globalThis & {
    __MY9_TRENDS_INFLIGHT__?: Map<string, Promise<TrendResponse>>;
  };

  if (!g.__MY9_TRENDS_INFLIGHT__) {
    g.__MY9_TRENDS_INFLIGHT__ = new Map<string, Promise<TrendResponse>>();
  }
  return g.__MY9_TRENDS_INFLIGHT__;
}

async function safeGetTrendSampleSummaryCache(
  period: TrendPeriod,
  kind: SubjectKind,
  allowExpired = false
): Promise<TrendSampleSummary | null> {
  try {
    return await getTrendSampleSummaryCache(period, kind, { allowExpired });
  } catch {
    return null;
  }
}

async function safeGetTrendsCache(
  params: ResolveTrendParams,
  allowExpired = false
): Promise<TrendResponse | null> {
  try {
    return await getTrendsCache(
      params.period,
      params.view,
      params.kind,
      params.overallPage,
      params.yearPage,
      { allowExpired }
    );
  } catch {
    return null;
  }
}

async function safeSetTrendSampleSummaryCache(
  period: TrendPeriod,
  kind: SubjectKind,
  value: TrendSampleSummary
): Promise<void> {
  try {
    await setTrendSampleSummaryCache(period, kind, value, TRENDS_STORE_CACHE_TTL_SECONDS);
  } catch {
    // Intentionally swallow cache write errors.
  }
}

async function safeSetTrendsCache(params: ResolveTrendParams, value: TrendResponse): Promise<void> {
  try {
    await setTrendsCache(
      params.period,
      params.view,
      params.kind,
      params.overallPage,
      params.yearPage,
      value,
      TRENDS_STORE_CACHE_TTL_SECONDS
    );
  } catch {
    // Intentionally swallow cache write errors.
  }
}

async function safeGetTrendSampleSummary(period: TrendPeriod, kind: SubjectKind): Promise<TrendSampleSummary | null> {
  try {
    return await getTrendSampleSummary(period, kind);
  } catch {
    return null;
  }
}

export function parseTrendPeriod(value: string | null | undefined): TrendPeriod {
  if (value && VALID_TREND_PERIODS.includes(value as TrendPeriod)) {
    return value as TrendPeriod;
  }
  return DEFAULT_TREND_PERIOD;
}

export function parseTrendView(value: string | null | undefined): TrendView {
  if (value && VALID_TREND_VIEWS.includes(value as TrendView)) {
    return value as TrendView;
  }
  return DEFAULT_TREND_VIEW;
}

export function isOverallOnlyTrendKind(kind: SubjectKind): boolean {
  return OVERALL_ONLY_TREND_KINDS.has(kind);
}

export function resolveTrendViewByKind(kind: SubjectKind, view: TrendView): TrendView {
  if (isOverallOnlyTrendKind(kind)) {
    return "overall";
  }
  return view;
}

export function parseTrendKind(value: string | null | undefined): SubjectKind {
  return parseSubjectKind(value) ?? DEFAULT_TREND_KIND;
}

export function parseTrendOverallPage(value: string | null | undefined): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    return DEFAULT_TREND_OVERALL_PAGE;
  }
  if (parsed < 1 || parsed > MAX_TREND_OVERALL_PAGE) {
    return DEFAULT_TREND_OVERALL_PAGE;
  }
  return parsed;
}

export function parseTrendYearPage(value: string | null | undefined): TrendYearPage {
  return value === "legacy" ? "legacy" : DEFAULT_TREND_YEAR_PAGE;
}

async function resolveTrendResponseInternal(params: ResolveTrendParams): Promise<TrendResponse> {
  const { period, kind } = params;
  let sampleSummary = await safeGetTrendSampleSummaryCache(period, kind, false);

  const cached = await safeGetTrendsCache(params, false);
  if (cached) {
    let mergedSampleCount = sampleSummary ? Math.max(sampleSummary.sampleCount, cached.sampleCount) : cached.sampleCount;
    let shouldBypassCached = false;

    // `today` is vulnerable to low-sample seed cache after midnight.
    // If cached payload is empty and sample < 30, probe live summary once.
    if (period === "today" && cached.items.length === 0 && mergedSampleCount < 30) {
      const liveSummary = await safeGetTrendSampleSummary(period, kind);
      if (liveSummary) {
        mergedSampleCount = Math.max(mergedSampleCount, liveSummary.sampleCount);
        if (!isSameSampleSummary(sampleSummary, liveSummary)) {
          sampleSummary = liveSummary;
          await safeSetTrendSampleSummaryCache(period, kind, liveSummary);
        } else {
          sampleSummary = liveSummary;
        }
        if (liveSummary.sampleCount >= 30) {
          shouldBypassCached = true;
        }
      }
    }

    const cachedLooksStaleSuppressed = cached.items.length === 0 && mergedSampleCount >= 30;

    if (!cachedLooksStaleSuppressed && !shouldBypassCached) {
      if (!sampleSummary) {
        const cachedLooksSuppressedSmallSample = cached.items.length === 0 && cached.sampleCount > 0 && cached.sampleCount < 30;
        if (!cachedLooksSuppressedSmallSample) {
          sampleSummary = toSampleSummary(cached);
          await safeSetTrendSampleSummaryCache(period, kind, sampleSummary);
        }
      }
      return suppressSmallSamples(applySampleSummary(cached, sampleSummary));
    }

    if (!sampleSummary) {
      sampleSummary = await safeGetTrendSampleSummary(period, kind);
    }
    if (!sampleSummary) {
      sampleSummary = toSampleSummary(cached);
      await safeSetTrendSampleSummaryCache(period, kind, sampleSummary);
    }
  }

  try {
    const aggregated = await getAggregatedTrendResponse({
      period: params.period,
      view: params.view,
      kind: params.kind,
      overallPage: params.overallPage,
      yearPage: params.yearPage,
    });

    if (aggregated) {
      sampleSummary = toSampleSummary(aggregated);
      await safeSetTrendSampleSummaryCache(period, kind, sampleSummary);
      const normalized = suppressSmallSamples(aggregated);
      await safeSetTrendsCache(params, normalized);
      return normalized;
    }
  } catch (error) {
    console.error("[trends] aggregation failed", {
      period: params.period,
      view: params.view,
      kind: params.kind,
      overallPage: params.overallPage,
      yearPage: params.yearPage,
      error,
    });
  }

  const staleCached = await safeGetTrendsCache(params, true);
  if (staleCached) {
    if (!sampleSummary) {
      sampleSummary = (await safeGetTrendSampleSummaryCache(period, kind, true)) ?? toSampleSummary(staleCached);
    }
    return suppressSmallSamples(applySampleSummary(staleCached, sampleSummary));
  }

  if (!sampleSummary) {
    sampleSummary = (await safeGetTrendSampleSummary(period, kind)) ?? {
      sampleCount: 0,
      range: { from: null, to: null },
    };
  }

  return suppressSmallSamples(createEmptyTrendResponse(params, sampleSummary));
}

export async function resolveTrendResponse(params: ResolveTrendParams): Promise<TrendResponse> {
  const key = resolveInflightKey(params);
  const inflight = getInflightMap();
  const existing = inflight.get(key);
  if (existing) {
    return existing;
  }

  const pending = resolveTrendResponseInternal(params).finally(() => {
    inflight.delete(key);
  });
  inflight.set(key, pending);
  return pending;
}
