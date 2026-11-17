import { Activity } from "lucide-react";
import TrendsClientPage from "@/app/components/TrendsClientPage";
import { createResonancePreview } from "@/lib/dev/resonance-preview";
import type { TrendResponse } from "@/lib/share/types";

export const metadata = {
  title: "榜单共鸣预览",
  robots: { index: false, follow: false },
};

export default function TrendsResonancePreviewPage({ searchParams }: {
  searchParams: { empty?: string };
}) {
  const { empty } = searchParams;
  const preview = createResonancePreview(empty === "1");
  const initialData: TrendResponse = {
    period: "30d",
    view: "overall",
    sampleCount: 5231,
    range: { from: Date.parse("2026-08-19T01:00:00Z"), to: Date.parse("2026-09-18T01:00:00Z") },
    lastUpdatedAt: preview.selectionStats.updatedAt ?? 0,
    items: preview.games.map((game, index) => {
      const count = 1800 - index * 157;
      return {
        key: String(game.id),
        label: game.localizedName || game.name,
        count,
        games: [{ ...game, id: String(game.id), count }],
      };
    }),
  };

  return (
    <>
      <aside className="border-b border-sky-200 bg-sky-50 px-4 py-3 text-sky-800 dark:border-sky-900 dark:bg-sky-950/50 dark:text-sky-200">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-x-4 gap-y-2 text-xs leading-relaxed">
          <p className="inline-flex items-center gap-2">
            <Activity className="h-4 w-4 shrink-0" aria-hidden="true" />
            本地预览 · 封面与统计均为示例数据
          </p>
          <nav aria-label="预览状态" className="flex items-center gap-3">
            <a href="/trends/preview" className="underline underline-offset-4">正常结果</a>
            <a href="/trends/preview?empty=1" className="underline underline-offset-4">空结果</a>
            <a href="/game/s/preview#selected-games" className="underline underline-offset-4">分享页</a>
          </nav>
        </div>
      </aside>
      <TrendsClientPage
        key={empty === "1" ? "empty" : "results"}
        initialData={initialData}
        relatedSelectionPreviews={preview.relatedSelectionPreviews}
      />
    </>
  );
}
