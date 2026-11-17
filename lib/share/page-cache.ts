import { parseSubjectKind } from "@/lib/subject-kind";
import assets from "#client-assets";

export const shareHeaders = { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "private, no-store", "CDN-Cache-Control": "no-store" };

export function shareCacheKey(request: Request) {
  const url = new URL(request.url);
  const match = /^\/([^/]+)\/s\/([a-f0-9]{16})$/.exec(url.pathname);
  if (!assets.version || request.method !== "GET" || url.search || !match || !parseSubjectKind(match[1]) ||
    ["authorization", "range", "if-none-match", "if-modified-since", "rsc", "next-router-state-tree"].some(h => request.headers.has(h)) ||
    ["purpose", "sec-purpose", "x-moz"].some(h => /prefetch/i.test(request.headers.get(h) ?? ""))) return null;
  url.pathname = "/__my9_page_cache/" + assets.version + url.pathname;
  return new Request(url);
}
