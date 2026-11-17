import type { Context } from "hono";
import { trackShareViewRequest } from "@/lib/share/view-stats";
import { resolveSharePage } from "@/lib/share/page-data";
import { getShareSelectionStats } from "@/lib/share/storage";
import { shareCacheKey, shareHeaders } from "@/lib/share/page-cache";
import { renderDocument } from "./document";
import { ShareView, shareMetadata } from "./share-view";

export async function shareRoute(c: Context<{ Bindings: CloudflareEnv }, "/:kind/s/:id">) {
  const req = c.req.raw;
  const head = req.method === "HEAD";
  // Old open tabs can still send Next navigation requests. HTML makes Next retry as a document.
  trackShareViewRequest(req, c.env.MY9_SHARE_VIEW_ANALYTICS);
  const key = shareCacheKey(req);
  const cache = (caches as CacheStorage & { default: Cache }).default;
  const reply = (body: BodyInit | null, cacheStatus: string) => {
    console.log({ event: "share-page", cache: cacheStatus });
    return new Response(body, { headers: { ...shareHeaders, "X-My9-Page-Cache": cacheStatus } });
  };
  if (key) {
    const cached = await cache.match(key).catch(() => undefined);
    if (cached) return reply(cached.body, "HIT");
  }
  const page = await resolveSharePage(c.req.param("kind"), c.req.param("id"));
  if (page.type === "not-found") return c.redirect("/", 307);
  if (page.type === "redirect") return c.redirect(page.location, page.status);
  if (head) return new Response(null, { headers: shareHeaders });

  const { kind, shareId } = page;
  const meta = shareMetadata(kind, shareId);
  if (page.type === "fallback") {
    const { default: Retry } = await import("@/app/components/My9ReadonlyApp");
    return reply(renderDocument(meta, <Retry kind={kind} initialShareId={shareId} />, { page: "retry", kind, shareId }), "BYPASS");
  }
  const { creatorName, games } = page.share;
  const selectionStats = await getShareSelectionStats(page.share);
  const props = { kind, shareId, data: { kind, shareId: page.share.shareId, creatorName, games, selectionStats } };
  const html = renderDocument(meta, <ShareView {...props} />, { page: "share", ...props });
  if (key && selectionStats?.updatedAt != null) {
    c.executionCtx.waitUntil(cache.put(key, new Response(html, { headers: {
      "Content-Type": shareHeaders["Content-Type"], "Cache-Control": "public, max-age=300"
    } })).catch(() => console.warn("[share-page-cache] write skipped")));
  }
  return reply(html, key ? "MISS" : "BYPASS");
}
