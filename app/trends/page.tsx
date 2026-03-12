import type { Metadata } from "next";
import TrendsClientPage from "@/app/components/TrendsClientPage";
import { unstable_cache } from "next/cache";
import type { TrendResponse } from "@/lib/share/types";
import { getSubjectKindMeta } from "@/lib/subject-kind";
import {
  parseTrendKind,
  parseTrendOverallPage,
  parseTrendPeriod,
  parseTrendView,
  parseTrendYearPage,
  resolveTrendResponse,
} from "@/lib/share/trends-query";

const TRENDS_PAGE_ISR_TTL_SECONDS = 300;

const resolveTrendResponseCached = unstable_cache(
  async (
    kind: string,
    period: string,
    view: string,
    overallPage: number,
    yearPage: string
  ) => {
    return resolveTrendResponse({
      kind: parseTrendKind(kind),
      period: parseTrendPeriod(period),
      view: parseTrendView(view),
      overallPage: parseTrendOverallPage(String(overallPage)),
      yearPage: parseTrendYearPage(yearPage),
    });
  },
  ["trends-page-response-v1"],
  { revalidate: TRENDS_PAGE_ISR_TTL_SECONDS }
);

function resolveSearchParam(
  value: string | string[] | undefined
): string | null {
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}

type TrendsSearchParams = {
  kind?: string | string[];
  period?: string | string[];
  view?: string | string[];
  overallPage?: string | string[];
  yearPage?: string | string[];
};

export function generateMetadata({
  searchParams,
}: {
  searchParams?: TrendsSearchParams;
}): Metadata {
  const kind = parseTrendKind(resolveSearchParam(searchParams?.kind));
  return {
    title: `构成大家的${getSubjectKindMeta(kind).trendLabel}`,
  };
}

export default async function TrendsPage({
  searchParams,
}: {
  searchParams?: TrendsSearchParams;
}) {
  const initialKind = parseTrendKind(resolveSearchParam(searchParams?.kind));
  const initialPeriod = parseTrendPeriod(resolveSearchParam(searchParams?.period));
  const initialView = parseTrendView(resolveSearchParam(searchParams?.view));
  const initialOverallPage = parseTrendOverallPage(resolveSearchParam(searchParams?.overallPage));
  const initialYearPage = parseTrendYearPage(resolveSearchParam(searchParams?.yearPage));
  const initialParams = {
    kind: initialKind,
    period: initialPeriod,
    view: initialView,
    overallPage: initialOverallPage,
    yearPage: initialYearPage,
  };

  let initialData: TrendResponse | null = null;
  let initialError = "";

  try {
    // Keep `today` requests uncached at the page layer so low-sample recovery can
    // run on every request after the midnight seed window.
    if (initialParams.period === "today") {
      initialData = await resolveTrendResponse(initialParams);
    } else {
      initialData = await resolveTrendResponseCached(
        initialParams.kind,
        initialParams.period,
        initialParams.view,
        initialParams.overallPage,
        initialParams.yearPage
      );
    }
  } catch {
    initialError = "趋势数据加载失败";
  }

  return (
    <TrendsClientPage
      initialKind={initialKind}
      initialPeriod={initialPeriod}
      initialView={initialView}
      initialOverallPage={initialOverallPage}
      initialYearPage={initialYearPage}
      initialData={initialData}
      initialError={initialError}
    />
  );
}
