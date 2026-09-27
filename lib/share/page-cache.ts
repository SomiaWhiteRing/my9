import { parseSubjectKind } from "@/lib/subject-kind";

const PAGE_TTL_SECONDS = 300;
const MAX_HTML_BYTES = 256 * 1024;
const CLIENT_CACHE_CONTROL = "private, no-cache, no-store, max-age=0, must-revalidate";
const PAGE_VARY_HEADERS = new Set([
  "rsc", "next-router-state-tree", "next-router-prefetch", "next-router-segment-prefetch", "accept-encoding",
]);
const BYPASS_HEADERS = [
  "authorization", "range", "if-none-match", "if-modified-since",
  "rsc", "next-action", "next-router-state-tree", "next-router-prefetch",
  "next-router-segment-prefetch", "next-url", "x-nextjs-data", "x-deployment-id",
];

function cacheKey(request: Request, buildVersion: number | undefined) {
  const url = new URL(request.url);
  const match = /^\/([^/]+)\/s\/([a-f0-9]{16})$/.exec(url.pathname);
  if (request.method !== "GET" || !buildVersion || !match || !parseSubjectKind(match[1]) || url.search ||
      BYPASS_HEADERS.some((header) => request.headers.has(header)) ||
      /__prerender_bypass|__next_preview_data/.test(request.headers.get("cookie") ?? "") ||
      ["purpose", "sec-purpose", "x-moz"].some((header) => /prefetch/i.test(request.headers.get(header) ?? ""))) {
    return null;
  }
  // Include the OpenNext build timestamp: old HTML references old JS bundles.
  url.pathname = `/__my9_page_cache/${buildVersion}${url.pathname}`;
  return new Request(url);
}

function clientResponse(response: Response, status: "HIT" | "MISS") {
  const headers = new Headers(response.headers);
  // The Worker must see every document view, including page-cache hits.
  headers.set("Cache-Control", CLIENT_CACHE_CONTROL);
  headers.set("CDN-Cache-Control", "no-store");
  headers.set("X-My9-Page-Cache", status);
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

async function readSmallHtml(response: Response): Promise<string | null> {
  if (!response.body) return null;
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let size = 0;
  let html = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return html + decoder.decode();
      size += value.byteLength;
      if (size > MAX_HTML_BYTES) {
        void reader.cancel().catch(() => {});
        return null;
      }
      html += decoder.decode(value, { stream: true });
    }
  } finally {
    reader.releaseLock();
  }
}

async function storePage(cache: Cache, key: Request, response: Response, identity: string) {
  const html = await readSmallHtml(response);
  // Next can send an error/retry shell with HTTP 200. Only a complete, healthy
  // readonly page opts in; user text is HTML-escaped and cannot supply this tag.
  if (!html || !html.includes(`data-my9-share-cache="${identity}"`) ||
      !html.endsWith("</body></html>") ||
      /<!--\$[!?]-->|<meta\b[^>]*http-equiv=["']refresh|<html\b[^>]*id=["']__next_error__/i.test(html)) return;

  const headers = new Headers(response.headers);
  headers.set("Cache-Control", `public, max-age=${PAGE_TTL_SECONDS}`);
  headers.delete("CDN-Cache-Control");
  headers.delete("Content-Length");
  headers.delete("Content-Encoding");
  await cache.put(key, new Response(html, { headers }));
}

export async function fetchSharePageWithCache(
  request: Request,
  ctx: { waitUntil(promise: Promise<unknown>): void },
  buildVersion: number | undefined,
  render: () => Promise<Response>,
): Promise<Response> {
  const key = cacheKey(request, buildVersion);
  const cache = typeof caches === "undefined" ? null : (caches as CacheStorage & { default?: Cache }).default;
  if (!key || !cache) return render();

  try {
    const cached = await cache.match(key);
    if (cached) return clientResponse(cached, "HIT");
  } catch {
    // Cache availability must not determine page availability.
  }

  const response = await render();
  if (response.status === 200 && response.headers.get("Content-Type")?.startsWith("text/html") &&
      !response.headers.has("Set-Cookie") && !response.headers.has("Location") &&
      (response.headers.get("Vary") ?? "").split(",").every((header) => !header.trim() || PAGE_VARY_HEADERS.has(header.trim().toLowerCase()))) {
    const { pathname } = new URL(request.url);
    const identity = pathname.slice(1).replace("/s/", "/");
    ctx.waitUntil(storePage(cache, key, response.clone(), identity).catch(() => {
      console.warn("[share-page-cache] Cache write skipped.");
    }));
  }
  return clientResponse(response, "MISS");
}
