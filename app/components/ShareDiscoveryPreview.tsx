"use client";

import { useCallback, useRef } from "react";
import { ShareDiscoveryDialog } from "@/components/share/ShareDiscoveryDialog";
import { DISCOVERY_PREVIEW_ITEMS, type DiscoveryPreviewState } from "@/lib/dev/share-discovery-preview";
import type { ShareDiscoveryItem, ShareDiscoveryLoader } from "@/lib/share/discovery";

function getPreviewHref(item: ShareDiscoveryItem, query: string) {
  const suffix = query ? `?${new URLSearchParams({ query })}` : "";
  return `/${item.kind}/s/find-preview/${item.shareId}${suffix}`;
}

export function ShareDiscoveryPreview({ state = "normal", initiallyOpen = false, initialQuery = "" }: {
  state?: DiscoveryPreviewState;
  initiallyOpen?: boolean;
  initialQuery?: string;
}) {
  const hasFailed = useRef(false);
  const loadPage = useCallback<ShareDiscoveryLoader>(async ({ query, cursor, signal }) => {
    await new Promise<void>((resolve, reject) => {
      if (signal.aborted) { reject(new DOMException("Aborted", "AbortError")); return; }
      const abort = () => { clearTimeout(timer); reject(new DOMException("Aborted", "AbortError")); };
      const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, 650);
      signal.addEventListener("abort", abort, { once: true });
    });
    if (state === "error" && !hasFailed.current) {
      hasFailed.current = true;
      throw new Error("暂时无法读取构成，请稍后重试。");
    }
    const normalized = query.trim();
    const matches = state === "empty" ? [] : query
      ? DISCOVERY_PREVIEW_ITEMS.filter((item) => item.creatorName === normalized)
        .sort((a, b) => b.createdAt - a.createdAt || b.shareId.localeCompare(a.shareId))
      : [...DISCOVERY_PREVIEW_ITEMS].sort((a, b) => b.viewCount - a.viewCount).slice(0, 20);
    const offset = cursor ? Number(cursor) : 0;
    return {
      items: matches.slice(offset, offset + 20),
      nextCursor: offset + 20 < matches.length ? String(offset + 20) : null,
      viewsThrough: "2026-09-17",
    };
  }, [state]);

  return <ShareDiscoveryDialog loadPage={loadPage} getShareHref={getPreviewHref} initiallyOpen={initiallyOpen} initialQuery={initialQuery} />;
}
