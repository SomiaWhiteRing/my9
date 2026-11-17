import My9ReadonlyPage from "@/app/components/My9ReadonlyPage";
import { ShareDiscoveryPreview } from "@/app/components/ShareDiscoveryPreview";
import { DISCOVERY_PREVIEW_ITEMS, createDiscoveryPreviewShare } from "@/lib/dev/share-discovery-preview";
import { parseSubjectKind } from "@/lib/subject-kind";

export const metadata = { title: "查找构成本地预览", robots: { index: false, follow: false } };

export default function DiscoveryPreviewPage({ params, searchParams }: {
  params: { kind: string };
  searchParams: { state?: string; open?: string; query?: string };
}) {
  const kind = parseSubjectKind(params.kind);
  if (!kind) return null;
  const options = searchParams;
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
            <a href={`/${kind}/find-preview?open=1`} className="underline underline-offset-4">正常</a>
            <a href={`/${kind}/find-preview?state=empty&open=1`} className="underline underline-offset-4">空结果</a>
            <a href={`/${kind}/find-preview?state=error&open=1`} className="underline underline-offset-4">加载失败</a>
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
