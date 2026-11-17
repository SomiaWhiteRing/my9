import { Hono } from "hono";
import { POST as saveShare, GET as handleShareGetRequest } from "./app/api/share/route";
import { GET as customSearch } from "./app/api/custom/search/route";
import { GET as shareCount } from "./app/api/stats/share-count/route";
import { GET as retiredArchive } from "./app/api/cron/archive/route";
import {
  BANGUMI_IMAGE_PROXY_PATH, BANGUMI_IMAGE_CACHE_CONTROL, BANGUMI_IMAGE_ERROR_CACHE_CONTROL,
  BANGUMI_IMAGE_FORBIDDEN_CACHE_CONTROL, getBangumiImageProxyCorsOrigin,
  normalizeBangumiImageProxyTarget, isAllowedBangumiImageProxyRequest,
} from "./lib/image-proxy";
import { runDailyShareMaintenance } from "./lib/share/daily-maintenance";
import { runHourlyTrendMaintenance } from "./lib/share/hourly-trend-maintenance";
import { GET as handleSubjectSearchRequest } from "./app/api/subjects/search/route";
import { shareRoute } from "./src/share-route";
import { GET as handleRelatedSelectionsRequest } from "./app/api/subjects/related/route";
import { GET as handleShareDiscoveryRequest } from "./app/api/shares/discover/route";
import { runCooccurrenceMaintenance } from "./lib/share/cooccurrence-maintenance";
import { GET as handleTrendsRequest } from "./app/api/trends/route";

const TREND_ROLLUP_CRON = "30 * * * *";
const DAILY_MAINTENANCE_CRON = "5 16 * * *";
const COOCCURRENCE_CRON = "*/10 * * * *";
function appendBangumiImageProxyAccessHeaders(headers, request) {
  const corsOrigin = getBangumiImageProxyCorsOrigin(request);

  if (corsOrigin) {
    headers.set("Access-Control-Allow-Origin", corsOrigin);
  } else {
    headers.delete("Access-Control-Allow-Origin");
  }

  headers.set("Vary", "Origin, Referer, Sec-Fetch-Site");
}

function withBangumiImageProxyAccessHeaders(response, request) {
  const headers = new Headers(response.headers);
  appendBangumiImageProxyAccessHeaders(headers, request);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function buildBangumiImageProxyHeaders(upstream) {
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
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("X-My9-Image-Proxy", "bangumi");
  return headers;
}

function toBangumiImageProxyCacheRequest(requestUrl, targetUrl) {
  const cacheUrl = new URL(requestUrl);
  cacheUrl.search = "";
  cacheUrl.searchParams.set("url", targetUrl);
  return new Request(cacheUrl.toString(), { method: "GET" });
}

async function handleBangumiImageProxy(request, ctx) {
  const isAllowedRequest = isAllowedBangumiImageProxyRequest(request);

  if (request.method === "OPTIONS") {
    if (!isAllowedRequest) {
      return new Response("Forbidden", {
        status: 403,
        headers: {
          "Cache-Control": BANGUMI_IMAGE_FORBIDDEN_CACHE_CONTROL,
          "Vary": "Origin, Referer, Sec-Fetch-Site",
        },
      });
    }

    const corsOrigin = getBangumiImageProxyCorsOrigin(request);
    return new Response(null, {
      status: 204,
      headers: {
        ...(corsOrigin ? { "Access-Control-Allow-Origin": corsOrigin } : {}),
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": "no-store",
        Vary: "Origin, Referer, Sec-Fetch-Site",
      },
    });
  }

  if (request.method !== "GET" && request.method !== "HEAD") {
    return new Response("Method Not Allowed", {
      status: 405,
      headers: {
        Allow: "GET, HEAD, OPTIONS",
        "Cache-Control": "no-store",
      },
    });
  }

  if (!isAllowedRequest) {
    return new Response("Forbidden", {
      status: 403,
      headers: {
        "Cache-Control": BANGUMI_IMAGE_FORBIDDEN_CACHE_CONTROL,
        "Vary": "Origin, Referer, Sec-Fetch-Site",
      },
    });
  }

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

  const cache = typeof caches !== "undefined" ? caches.default : null;
  const cacheRequest = toBangumiImageProxyCacheRequest(request.url, targetUrl);

  if (request.method === "GET" && cache) {
    const cached = await cache.match(cacheRequest);
    if (cached) return withBangumiImageProxyAccessHeaders(cached, request);
  }

  try {
    const upstream = await fetch(targetUrl, {
      method: request.method === "HEAD" ? "HEAD" : "GET",
      cf: {
        cacheEverything: true,
        cacheTtlByStatus: {
          "200-299": BANGUMI_IMAGE_CACHE_TTL_SECONDS,
          "300-399": 3600,
          "404": 300,
          "500-599": 0,
        },
      },
    });

    const response = new Response(request.method === "HEAD" ? null : upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: buildBangumiImageProxyHeaders(upstream),
    });

    if (request.method === "GET" && upstream.ok && cache) {
      ctx.waitUntil(cache.put(cacheRequest, response.clone()));
    }

    return withBangumiImageProxyAccessHeaders(response, request);
  } catch {
    return new Response("Bangumi image fetch failed", {
      status: 502,
      headers: {
        "Cache-Control": "no-store",
      },
    });
  }
}

const app = new Hono();
app.get("/api/subjects/search", c => handleSubjectSearchRequest(c.req.raw));
app.get("/api/subjects/related", c => handleRelatedSelectionsRequest(c.req.raw));
app.get("/api/share", c => handleShareGetRequest(c.req.raw));
app.post("/api/share", c => saveShare(c.req.raw));
app.get("/api/shares/discover", c => handleShareDiscoveryRequest(c.req.raw));
app.get("/api/trends", c => handleTrendsRequest(c.req.raw));
app.get("/api/custom/search", c => customSearch(c.req.raw));
app.get("/api/stats/share-count", () => shareCount());
app.get("/api/cron/archive", () => retiredArchive());
app.all(BANGUMI_IMAGE_PROXY_PATH, c => handleBangumiImageProxy(c.req.raw, c.executionCtx));

if (__MY9_DEV__) {
  app.get("*", async (c, next) => {
    const { renderDevPage } = await import("./src/dev-pages");
    const html = await renderDevPage(new URL(c.req.url));
    return html === null ? next() : c.html(html);
  });
}

app.get("/:kind/s/:id", shareRoute);
app.notFound(c => {
  const url = new URL(c.req.url);
  if (url.pathname.endsWith("/") && url.pathname !== "/") {
    url.pathname = url.pathname.replace(/\/+$/, "");
    return c.redirect(url.pathname + url.search, 308);
  }
  if (url.pathname.startsWith("/api/")) {
    const methods = app.routes.filter(route => route.path === url.pathname).map(route => route.method);
    if (methods.length) {
      const allow = [...new Set([...methods, ...(methods.includes("GET") ? ["HEAD"] : []), "OPTIONS"])].join(", ");
      return new Response(null, { status: c.req.method === "OPTIONS" ? 204 : 405, headers: { Allow: allow } });
    }
    return new Response(null, { status: 404 });
  }
  if (/\.[a-z0-9]+$/i.test(url.pathname) || url.pathname.startsWith("/assets/")) return new Response(null, { status: 404 });
  return c.redirect("/", 307);
});

const worker = {
  fetch(request, env, ctx) {
    if (new URL(request.url).hostname === "bgm-img.shatranj.space") {
      return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
    }
    return app.fetch(request, env, ctx);
  },
  scheduled(controller, env, ctx) {

    if (controller.cron === COOCCURRENCE_CRON) {
      ctx.waitUntil(runCooccurrenceMaintenance().then((result) => {
        if (result.phase !== "idle") console.log("[cooccurrence]", JSON.stringify(result));
      }));
      return;
    }

    if (controller.cron === TREND_ROLLUP_CRON) {
      ctx.waitUntil(
        runHourlyTrendMaintenance({
          logLabel: `[trend-cron:${controller.cron}]`,
        }).catch((error) => {
          console.error("[trend-cron] failed", error);
          throw error;
        })
      );
      return;
    }

    if (controller.cron !== DAILY_MAINTENANCE_CRON) {
      console.warn(`[scheduled] unsupported cron ${controller.cron}`);
      return;
    }

    ctx.waitUntil(
      runDailyShareMaintenance({
        env,
        logLabel: `[daily-cron:${controller.cron}]`,
      }).catch((error) => {
        console.error("[daily-cron] failed", error);
        throw error;
      })
    );
  },
};

export default worker;
