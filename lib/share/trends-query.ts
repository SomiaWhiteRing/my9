import { getAggregatedTrendResponse } from "@/lib/share/storage";
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

type ResolveTrendParams = {
  period: TrendPeriod;
  view: TrendView;
  kind: SubjectKind;
  overallPage: number;
  yearPage: TrendYearPage;
};

function suppressSmallSamples(response: TrendResponse): TrendResponse {
  if (response.sampleCount < 30) {
    return {
      ...response,
      items: [],
    };
  }
  return response;
}

function createEmptyTrendResponse(params: ResolveTrendParams): TrendResponse {
  return {
    period: params.period,
    view: params.view,
    sampleCount: 0,
    range: { from: null, to: null },
    lastUpdatedAt: 0,
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
  try {
    const aggregated = await getAggregatedTrendResponse({
      period: params.period,
      view: params.view,
      kind: params.kind,
      overallPage: params.overallPage,
      yearPage: params.yearPage,
    });

    if (aggregated) {
      return suppressSmallSamples(aggregated);
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

  return createEmptyTrendResponse(params);
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
