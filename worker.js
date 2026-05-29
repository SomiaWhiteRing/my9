import { runDailyShareMaintenance } from "./lib/share/daily-maintenance";
import { runHourlyTrendMaintenance } from "./lib/share/hourly-trend-maintenance";
import { trackShareViewRequest } from "./lib/share/view-stats";
import openNextWorker from "./.cf-build/.open-next/worker.js";

const TREND_ROLLUP_CRON = "30 * * * *";
const DAILY_MAINTENANCE_CRON = "5 16 * * *";
const BANGUMI_IMAGE_PROXY_PATH = "/api/image/bgm";
const BANGUMI_IMAGE_HOSTS = new Set(["lain.bgm.tv", "img.bgm.tv"]);
const BANGUMI_IMAGE_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30;
const BANGUMI_IMAGE_CACHE_CONTROL = `public, max-age=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, s-maxage=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, immutable`;
const BANGUMI_IMAGE_ERROR_CACHE_CONTROL = "public, max-age=300, s-maxage=300";

function bindRuntimeEnv(env) {
  globalThis.__MY9_CF_ENV = env;
}

function normalizeBangumiImageProxyTarget(value) {
  if (typeof value !== "string") return null;

  const raw = value.trim();
  if (!raw || raw.startsWith("data:") || raw.startsWith("blob:")) {
    return null;
  }

  const normalized = raw.startsWith("//") ? `https:${raw}` : raw;

  try {
    const parsed = new URL(normalized);
    if (!BANGUMI_IMAGE_HOSTS.has(parsed.hostname.toLowerCase())) {
      return null;
    }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }

    parsed.protocol = "https:";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return null;
  }
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
  headers.set("Access-Control-Allow-Origin", "*");
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
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": "no-store",
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
    if (cached) return cached;
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

    return response;
  } catch {
    return new Response("Bangumi image fetch failed", {
      status: 502,
      headers: {
        "Cache-Control": "no-store",
      },
    });
  }
}

const worker = {
  fetch(request, env, ctx) {
    bindRuntimeEnv(env);

    const requestUrl = new URL(request.url);
    if (requestUrl.pathname === BANGUMI_IMAGE_PROXY_PATH) {
      return handleBangumiImageProxy(request, ctx);
    }

    trackShareViewRequest(request, env.MY9_SHARE_VIEW_ANALYTICS ?? null);
    return openNextWorker.fetch(request, env, ctx);
  },
  scheduled(controller, env, ctx) {
    bindRuntimeEnv(env);

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
