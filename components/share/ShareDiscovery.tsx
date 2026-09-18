"use client";

import { ShareDiscoveryDialog } from "@/components/share/ShareDiscoveryDialog";
import type { ShareDiscoveryItem, ShareDiscoveryLoader, ShareDiscoveryPage } from "@/lib/share/discovery";

const loadPage: ShareDiscoveryLoader = async ({ query, cursor, signal }) => {
  const params = new URLSearchParams();
  if (query) params.set("query", query);
  if (cursor) params.set("cursor", cursor);
  const response = await fetch(`/api/shares/discover?${params}`, { signal });
  if (!response.ok) throw new Error("暂时无法读取构成，请稍后重试。");
  return await response.json() as ShareDiscoveryPage;
};

function getShareHref(item: ShareDiscoveryItem) {
  return `/${item.kind}/s/${item.shareId}`;
}

export function ShareDiscovery() {
  return <ShareDiscoveryDialog loadPage={loadPage} getShareHref={getShareHref} />;
}
