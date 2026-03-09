import TrendsClientPage from "@/app/components/TrendsClientPage";
import type { TrendResponse } from "@/lib/share/types";
import {
  DEFAULT_TREND_KIND,
  DEFAULT_TREND_PERIOD,
  DEFAULT_TREND_VIEW,
  resolveTrendResponse,
} from "@/lib/share/trends-query";

export default async function TrendsPage() {
  let initialData: TrendResponse | null = null;
  let initialError = "";

  try {
    initialData = await resolveTrendResponse({
      kind: DEFAULT_TREND_KIND,
      period: DEFAULT_TREND_PERIOD,
      view: DEFAULT_TREND_VIEW,
    });
  } catch {
    initialError = "趋势数据加载失败";
  }

  return (
    <TrendsClientPage
      initialKind={DEFAULT_TREND_KIND}
      initialPeriod={DEFAULT_TREND_PERIOD}
      initialView={DEFAULT_TREND_VIEW}
      initialData={initialData}
      initialError={initialError}
    />
  );
}
