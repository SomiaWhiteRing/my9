import {
  BANGUMI_IMAGE_CACHE_CONTROL,
  BANGUMI_IMAGE_ERROR_CACHE_CONTROL,
  normalizeBangumiImageProxyTarget,
} from "@/lib/image-proxy";

export const dynamic = "force-dynamic";

function buildProxyHeaders(upstream: Response): Headers {
  const headers = new Headers();
  const contentType = upstream.headers.get("content-type");
  const etag = upstream.headers.get("etag");
  const lastModified = upstream.headers.get("last-modified");

  if (contentType) headers.set("Content-Type", contentType);
  if (etag) headers.set("ETag", etag);
  if (lastModified) headers.set("Last-Modified", lastModified);

  headers.set(
    "Cache-Control",
    upstream.ok ? BANGUMI_IMAGE_CACHE_CONTROL : BANGUMI_IMAGE_ERROR_CACHE_CONTROL
  );
  headers.set(
    "CDN-Cache-Control",
    upstream.ok ? BANGUMI_IMAGE_CACHE_CONTROL : BANGUMI_IMAGE_ERROR_CACHE_CONTROL
  );
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("X-Content-Type-Options", "nosniff");
  return headers;
}

async function handleBangumiImageProxy(request: Request): Promise<Response> {
  const requestUrl = new URL(request.url);
  const targetUrl = normalizeBangumiImageProxyTarget(requestUrl.searchParams.get("url"));

  if (!targetUrl) {
    return new Response("Invalid Bangumi image URL", {
      status: 400,
      headers: {
        "Cache-Control": "no-store",
      },
    });
  }

  try {
    const upstream = await fetch(targetUrl, {
      method: request.method === "HEAD" ? "HEAD" : "GET",
      cache: "force-cache",
    });

    return new Response(request.method === "HEAD" ? null : upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: buildProxyHeaders(upstream),
    });
  } catch {
    return new Response("Bangumi image fetch failed", {
      status: 502,
      headers: {
        "Cache-Control": "no-store",
      },
    });
  }
}

export async function GET(request: Request): Promise<Response> {
  return handleBangumiImageProxy(request);
}

export async function HEAD(request: Request): Promise<Response> {
  return handleBangumiImageProxy(request);
}
