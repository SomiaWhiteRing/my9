import { NextResponse } from "next/server";
import { parseTrendKind, parseTrendPeriod, parseTrendView, resolveTrendResponse } from "@/lib/share/trends-query";
const TRENDS_CDN_TTL_SECONDS = 600;
const TRENDS_STALE_TTL_SECONDS = 86400;
const TRENDS_CACHE_CONTROL_VALUE = `public, max-age=0, s-maxage=${TRENDS_CDN_TTL_SECONDS}, stale-while-revalidate=${TRENDS_STALE_TTL_SECONDS}`;

function createTrendsCacheHeaders() {
  return {
    "Cache-Control": TRENDS_CACHE_CONTROL_VALUE,
    "CDN-Cache-Control": TRENDS_CACHE_CONTROL_VALUE,
    "Vercel-CDN-Cache-Control": TRENDS_CACHE_CONTROL_VALUE,
  };
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const period = parseTrendPeriod(searchParams.get("period"));
  const view = parseTrendView(searchParams.get("view"));
  const kind = parseTrendKind(searchParams.get("kind"));
  const response = await resolveTrendResponse({
    period,
    view,
    kind,
  });

  return NextResponse.json({
    ok: true,
    ...response,
  }, {
    headers: createTrendsCacheHeaders(),
  });
}
