import { Activity } from "lucide-react";
import My9ReadonlyPage from "@/app/components/My9ReadonlyPage";
import { createResonancePreview } from "@/lib/dev/resonance-preview";

export const metadata = {
  title: "共鸣样式预览",
  robots: { index: false, follow: false },
};

export default function ResonancePreviewPage({
  params,
  searchParams,
}: {
  params: { kind: string };
  searchParams: { empty?: string };
}) {
  const { kind } = params;
  if (kind !== "game") return null;

  const { empty } = searchParams;
  const data = createResonancePreview(empty === "1");

  return (
    <>
      <aside className="border-b border-sky-200 bg-sky-50 px-4 py-3 text-sky-800 dark:border-sky-900 dark:bg-sky-950/50 dark:text-sky-200">
        <div className="mx-auto flex max-w-2xl flex-wrap items-center justify-between gap-x-4 gap-y-2 text-xs leading-relaxed">
          <p className="inline-flex items-center gap-2">
            <Activity className="h-4 w-4 shrink-0" aria-hidden="true" />
            本地预览 · 封面与统计均为示例数据
          </p>
          <nav aria-label="预览状态" className="flex items-center gap-3">
            <a href="/game/s/preview#selected-games" className="underline underline-offset-4">正常结果</a>
            <a href="/game/s/preview?empty=1#selected-games" className="underline underline-offset-4">空结果</a>
          </nav>
        </div>
      </aside>
      <My9ReadonlyPage
        key={empty === "1" ? "empty" : "results"}
        kind="game"
        shareId="preview"
        initialShareData={{
          shareId: "preview",
          kind: "game",
          creatorName: "共鸣预览",
          games: data.games,
          selectionStats: data.selectionStats,
        }}
        relatedSelectionPreviews={data.relatedSelectionPreviews}
      />
    </>
  );
}
