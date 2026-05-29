import { runDailyShareMaintenance } from "./lib/share/daily-maintenance";
import { runHourlyTrendMaintenance } from "./lib/share/hourly-trend-maintenance";
import { trackShareViewRequest } from "./lib/share/view-stats";
import openNextWorker from "./.cf-build/.open-next/worker.js";

const TREND_ROLLUP_CRON = "30 * * * *";
const DAILY_MAINTENANCE_CRON = "5 16 * * *";
const BANGUMI_IMAGE_PROXY_PATH = "/api/image/bgm";
const BANGUMI_IMAGE_HOSTS = new Set(["lain.bgm.tv", "img.bgm.tv"]);
const ALLOWED_SITE_ROOT = "shatranj.space";
const LOCAL_DEVELOPMENT_HOSTS = new Set(["localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]"]);
const BANGUMI_IMAGE_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30;
const BANGUMI_IMAGE_CACHE_CONTROL = `public, max-age=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, s-maxage=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, immutable`;
const BANGUMI_IMAGE_ERROR_CACHE_CONTROL = "public, max-age=300, s-maxage=300";
const BANGUMI_IMAGE_FORBIDDEN_CACHE_CONTROL = "no-store";

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

function normalizeHostname(hostname) {
  return hostname.trim().toLowerCase().replace(/\.$/, "");
}

function isAllowedSiteHostname(hostname) {
  const normalized = normalizeHostname(hostname);
  return normalized === ALLOWED_SITE_ROOT || normalized.endsWith(`.${ALLOWED_SITE_ROOT}`);
}

function isLocalDevelopmentHostname(hostname) {
  return LOCAL_DEVELOPMENT_HOSTS.has(normalizeHostname(hostname));
}

function parseHeaderHostname(value) {
  if (!value) return null;

  try {
    return new URL(value).hostname;
  } catch {
    return null;
  }
}

function isAllowedSourceHostname(requestHostname, sourceHostname) {
  if (isAllowedSiteHostname(sourceHostname)) {
    return true;
  }

  return isLocalDevelopmentHostname(requestHostname) && isLocalDevelopmentHostname(sourceHostname);
}

function isAllowedBangumiImageProxyRequest(request) {
  const requestHostname = new URL(request.url).hostname;
  const originHostname = parseHeaderHostname(request.headers.get("origin"));
  const refererHostname = parseHeaderHostname(request.headers.get("referer"));

  if (originHostname) {
    return isAllowedSourceHostname(requestHostname, originHostname);
  }

  if (refererHostname) {
    return isAllowedSourceHostname(requestHostname, refererHostname);
  }

  const fetchSite = request.headers.get("sec-fetch-site")?.toLowerCase();
  if (fetchSite === "same-origin") {
    return isAllowedSiteHostname(requestHostname) || isLocalDevelopmentHostname(requestHostname);
  }
  if (fetchSite === "same-site") {
    return isAllowedSiteHostname(requestHostname);
  }

  return isLocalDevelopmentHostname(requestHostname);
}

function getBangumiImageProxyCorsOrigin(request) {
  const origin = request.headers.get("origin");
  const originHostname = parseHeaderHostname(origin);
  if (!origin || !originHostname) return null;

  const requestHostname = new URL(request.url).hostname;
  if (!isAllowedSourceHostname(requestHostname, originHostname)) {
    return null;
  }

  try {
    return new URL(origin).origin;
  } catch {
    return null;
  }
}

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
