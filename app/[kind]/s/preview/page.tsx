import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { Activity } from "lucide-react";
import My9ReadonlyPage from "@/app/components/My9ReadonlyPage";
import { createResonancePreview } from "@/lib/dev/resonance-preview";

export const dynamic = "force-dynamic";
export const metadata: Metadata = {
  title: "共鸣样式预览",
  robots: { index: false, follow: false },
};

export default async function ResonancePreviewPage({
  params,
  searchParams,
}: {
  params: Promise<{ kind: string }>;
  searchParams: Promise<{ empty?: string }>;
}) {
  if (process.env.NODE_ENV !== "development") notFound();
  const { kind } = await params;
  if (kind !== "game") notFound();

  const { empty } = await searchParams;
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
            <Link href="/game/s/preview#selected-games" prefetch={false} className="underline underline-offset-4">正常结果</Link>
            <Link href="/game/s/preview?empty=1#selected-games" prefetch={false} className="underline underline-offset-4">空结果</Link>
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
