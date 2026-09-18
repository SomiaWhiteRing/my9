import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import My9ReadonlyPage from "@/app/components/My9ReadonlyPage";
import { ShareDiscoveryPreview } from "@/app/components/ShareDiscoveryPreview";
import { DISCOVERY_PREVIEW_ITEMS, createDiscoveryPreviewShare } from "@/lib/dev/share-discovery-preview";
import { parseSubjectKind } from "@/lib/subject-kind";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "查找构成本地预览", robots: { index: false, follow: false } };

export default async function DiscoveryPreviewPage({ params, searchParams }: {
  params: Promise<{ kind: string }>;
  searchParams: Promise<{ state?: string; open?: string; query?: string }>;
}) {
  if (process.env.NODE_ENV !== "development") notFound();
  const kind = parseSubjectKind((await params).kind);
  if (!kind) notFound();
  const options = await searchParams;
  const state = options.state === "empty" || options.state === "error" ? options.state : "normal";
  const initialQuery = typeof options.query === "string" ? options.query.trim().slice(0, 40) : "";
  const entry = DISCOVERY_PREVIEW_ITEMS.find((item) => item.kind === kind)!;
  const data = createDiscoveryPreviewShare(entry);

  return (
    <>
      <aside className="border-b border-orange-200 bg-orange-50 px-4 py-3 text-xs text-orange-900 dark:border-orange-900 dark:bg-orange-950/50 dark:text-orange-200">
        <div className="mx-auto flex max-w-2xl flex-wrap items-center justify-between gap-3">
          <p>本地预览 · 名称、排名与分享详情均为示例数据</p>
          <nav aria-label="预览状态" className="flex gap-3">
            <Link href={`/${kind}/find-preview?open=1`} prefetch={false} className="underline underline-offset-4">正常</Link>
            <Link href={`/${kind}/find-preview?state=empty&open=1`} prefetch={false} className="underline underline-offset-4">空结果</Link>
            <Link href={`/${kind}/find-preview?state=error&open=1`} prefetch={false} className="underline underline-offset-4">加载失败</Link>
          </nav>
        </div>
      </aside>
      <My9ReadonlyPage
        kind={kind}
        shareId={entry.shareId}
        initialShareData={{ ...entry, games: data.games }}
        relatedSelectionPreviews={data.relatedSelectionPreviews}
        headerActions={<ShareDiscoveryPreview key={`${state}-${options.open}-${initialQuery}`} state={state} initiallyOpen={options.open === "1"} initialQuery={initialQuery} />}
      />
    </>
  );
}
