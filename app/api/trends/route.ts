import { NextResponse } from "next/server";
import {
  parseTrendKind,
  parseTrendOverallPage,
  parseTrendPeriod,
  parseTrendView,
  parseTrendYearPage,
  resolveTrendViewByKind,
  resolveTrendResponse,
} from "@/lib/share/trends-query";
const TRENDS_CDN_MAX_TTL_SECONDS = 300;

function createTrendsCacheHeaders(cdnTtlSeconds: number) {
  const cacheControlValue = `public, max-age=0, s-maxage=${cdnTtlSeconds}, stale-while-revalidate=${cdnTtlSeconds}`;
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

  return NextResponse.json({
    ok: true,
    ...response,
  }, {
    headers: createTrendsCacheHeaders(TRENDS_CDN_MAX_TTL_SECONDS),
  });
}
