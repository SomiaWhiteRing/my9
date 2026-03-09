import { listSharesByPeriod, getTrendsCache, setTrendsCache } from "@/lib/share/storage";
import { buildTrendResponse } from "@/lib/share/trends";
import { TrendPeriod, TrendResponse, TrendView } from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";

export const VALID_TREND_PERIODS: TrendPeriod[] = ["30d", "90d", "180d", "all"];
export const VALID_TREND_VIEWS: TrendView[] = ["overall", "genre", "decade", "year"];
export const DEFAULT_TREND_PERIOD: TrendPeriod = "90d";
export const DEFAULT_TREND_VIEW: TrendView = "overall";
export const DEFAULT_TREND_KIND: SubjectKind = DEFAULT_SUBJECT_KIND;

const TRENDS_STORE_CACHE_TTL_SECONDS = 600;

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

export function parseTrendKind(value: string | null | undefined): SubjectKind {
  return parseSubjectKind(value) ?? DEFAULT_TREND_KIND;
}

export async function resolveTrendResponse(params: {
  period: TrendPeriod;
  view: TrendView;
  kind: SubjectKind;
}): Promise<TrendResponse> {
  const { period, view, kind } = params;

  const cached = await getTrendsCache(period, view, kind);
  if (cached) {
    return cached;
  }

  const shares = (await listSharesByPeriod(period)).filter((item) => item.kind === kind);
  const response = buildTrendResponse({
    period,
    view,
    shares,
  });

  const normalizedResponse =
    response.sampleCount < 30
      ? {
          ...response,
          items: [],
        }
      : response;

  await setTrendsCache(period, view, kind, normalizedResponse, TRENDS_STORE_CACHE_TTL_SECONDS);
  return normalizedResponse;
}
