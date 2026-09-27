import {
  parseTrendKind,
  parseTrendOverallPage,
  parseTrendPeriod,
  parseTrendView,
  parseTrendYearPage,
  resolveTrendViewByKind,
  resolveTrendResponse,
} from "@/lib/share/trends-query";

const TRENDS_CACHE_CONTROL = "public, max-age=0, s-maxage=300, stale-while-revalidate=300";

export async function handleTrendsRequest(request: Request) {
  const { searchParams } = new URL(request.url);
  const period = parseTrendPeriod(searchParams.get("period"));
  const kind = parseTrendKind(searchParams.get("kind"));
  const view = resolveTrendViewByKind(kind, parseTrendView(searchParams.get("view")));
  const overallPage = parseTrendOverallPage(searchParams.get("overallPage"));
  const yearPage = parseTrendYearPage(searchParams.get("yearPage"));
  const response = await resolveTrendResponse({ period, view, kind, overallPage, yearPage });

  return Response.json({ ok: true, ...response }, {
    headers: {
      "Cache-Control": TRENDS_CACHE_CONTROL,
      "CDN-Cache-Control": TRENDS_CACHE_CONTROL,
    },
  });
}
