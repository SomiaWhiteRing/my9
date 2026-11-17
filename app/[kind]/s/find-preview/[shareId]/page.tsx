import { ArrowLeft } from "lucide-react";
import My9ReadonlyPage from "@/app/components/My9ReadonlyPage";
import { ShareDiscoveryPreview } from "@/app/components/ShareDiscoveryPreview";
import { DISCOVERY_PREVIEW_ITEMS, createDiscoveryPreviewShare } from "@/lib/dev/share-discovery-preview";

export const metadata = { title: "构成详情本地预览", robots: { index: false, follow: false } };

export default function DiscoverySharePreviewPage({ params, searchParams }: {
  params: { kind: string; shareId: string };
  searchParams: { query?: string };
}) {
  const { kind, shareId } = params;
  const entry = DISCOVERY_PREVIEW_ITEMS.find((item) => item.kind === kind && item.shareId === shareId);
  if (!entry) return null;
  const data = createDiscoveryPreviewShare(entry);
  const options = searchParams;
  const query = typeof options.query === "string" ? options.query.trim().slice(0, 40) : "";
  const returnParams = new URLSearchParams({ open: "1", ...(query ? { query } : {}) });

  return (
    <>
      <aside className="border-b border-orange-200 bg-orange-50 px-4 py-3 text-xs text-orange-900 dark:border-orange-900 dark:bg-orange-950/50 dark:text-orange-200">
        <div className="mx-auto flex max-w-2xl flex-wrap items-center justify-between gap-3">
          <p>本地预览 · 分享详情为示例数据</p>
          <a href={`/${kind}/find-preview?${returnParams}`} className="inline-flex items-center gap-1 underline underline-offset-4">
            <ArrowLeft className="h-3.5 w-3.5" aria-hidden="true" />返回查找构成
          </a>
        </div>
      </aside>
      <My9ReadonlyPage
        kind={entry.kind}
        shareId={entry.shareId}
        initialShareData={{ ...entry, games: data.games }}
        relatedSelectionPreviews={data.relatedSelectionPreviews}
        headerActions={<ShareDiscoveryPreview initialQuery={query} />}
      />
    </>
  );
}
