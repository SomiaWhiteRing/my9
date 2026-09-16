import { resolveSharePage } from "@/lib/share/page-data";

export async function handleShareHeadRequest(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const match = /^\/([^/]+)\/s\/([^/]+)$/.exec(url.pathname);
  if (!match || request.headers.has("rsc") || request.headers.has("next-router-state-tree") ||
      request.headers.has("next-router-prefetch") || request.headers.has("next-router-segment-prefetch") ||
      url.searchParams.has("_rsc") || request.headers.has("next-action") ||
      /__prerender_bypass|__next_preview_data/.test(request.headers.get("cookie") ?? "")) {
    return null;
  }
  // Leave encoded paths to Next's URL normalization and parameter decoder.
  if (match[1].includes("%") || match[2].includes("%")) return null;
  const page = await resolveSharePage(match[1], match[2]);
  // Next owns not-found/error rendering and the client retry fallback.
  if (page.type === "not-found" || page.type === "fallback") return null;

  const headers = new Headers({
    "Cache-Control": "private, no-cache, no-store, max-age=0, must-revalidate",
    "Content-Type": "text/html; charset=utf-8",
    "Vary": "rsc, next-router-state-tree, next-router-prefetch, next-router-segment-prefetch",
    "X-Powered-By": "Next.js",
    "x-opennext": "1",
  });
  if (page.type === "redirect") headers.set("Location", page.location);
  // Do not invent a Content-Length/ETag: these require rendering the GET body.
  return new Response(null, { status: page.type === "redirect" ? page.status : 200, headers });
}
