import { NextResponse } from "next/server";
import {
  parseTrendKind,
  parseTrendOverallPage,
  parseTrendPeriod,
  parseTrendView,
  parseTrendYearPage,
  resolveTrendViewByKind,
  resolveTrendResponse,
  TRENDS_STORE_CACHE_TTL_SECONDS,
} from "@/lib/share/trends-query";
const TRENDS_CDN_MAX_TTL_SECONDS = 300;

function resolveCdnTtlSeconds(lastUpdatedAt: number): number {
  if (!Number.isFinite(lastUpdatedAt) || lastUpdatedAt <= 0) {
    return 60;
  }
  const elapsedSeconds = Math.max(0, Math.floor((Date.now() - lastUpdatedAt) / 1000));
  const remainingSeconds = TRENDS_STORE_CACHE_TTL_SECONDS - elapsedSeconds;
  return Math.max(1, Math.min(TRENDS_CDN_MAX_TTL_SECONDS, remainingSeconds));
}

function createTrendsCacheHeaders(cdnTtlSeconds: number) {
  const cacheControlValue = `public, max-age=0, s-maxage=${cdnTtlSeconds}`;
  return {
    "Cache-Control": cacheControlValue,
    "CDN-Cache-Control": cacheControlValue,
  };
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const period = parseTrendPeriod(searchParams.get("period"));
  const kind = parseTrendKind(searchParams.get("kind"));
  const view = resolveTrendViewByKind(kind, parseTrendView(searchParams.get("view")));
  const overallPage = parseTrendOverallPage(searchParams.get("overallPage"));
  const yearPage = parseTrendYearPage(searchParams.get("yearPage"));
  const response = await resolveTrendResponse({
    period,
    view,
    kind,
    overallPage,
    yearPage,
  });
  const cdnTtlSeconds = resolveCdnTtlSeconds(response.lastUpdatedAt);

  return NextResponse.json({
    ok: true,
    ...response,
  }, {
    headers: createTrendsCacheHeaders(cdnTtlSeconds),
  });
}
