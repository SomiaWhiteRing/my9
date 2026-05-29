const BANGUMI_IMAGE_HOSTS = new Set(["lain.bgm.tv", "img.bgm.tv"]);
const ALLOWED_SITE_ROOT = "shatranj.space";
const LOCAL_DEVELOPMENT_HOSTS = new Set(["localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]"]);

export const BANGUMI_IMAGE_PROXY_PATH = "/api/image/bgm";
export const BANGUMI_IMAGE_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30;
export const BANGUMI_IMAGE_CACHE_CONTROL = `public, max-age=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, s-maxage=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, immutable`;
export const BANGUMI_IMAGE_ERROR_CACHE_CONTROL = "public, max-age=300, s-maxage=300";
export const BANGUMI_IMAGE_FORBIDDEN_CACHE_CONTROL = "no-store";

function normalizeRemoteImageUrl(value: string): string | null {
  const raw = value.trim();
  if (!raw) return null;

  if (raw.startsWith("data:") || raw.startsWith("blob:") || raw.startsWith("/")) {
    return raw;
  }

  const normalized = raw.startsWith("//") ? `https:${raw}` : raw;

  try {
    const parsed = new URL(normalized);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

export function normalizeBangumiImageProxyTarget(value: string | null | undefined): string | null {
  if (!value) return null;

  const normalized = normalizeRemoteImageUrl(value);
  if (!normalized || normalized.startsWith("data:") || normalized.startsWith("blob:") || normalized.startsWith("/")) {
    return null;
  }

  try {
    const parsed = new URL(normalized);
    if (!BANGUMI_IMAGE_HOSTS.has(parsed.hostname.toLowerCase())) {
      return null;
    }

    parsed.protocol = "https:";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return null;
  }
}

export function toProxiedBangumiImageUrl(value: string | null | undefined): string | null {
  if (!value) return null;

  const normalized = normalizeRemoteImageUrl(value);
  if (!normalized) return null;

  const target = normalizeBangumiImageProxyTarget(normalized);
  if (!target) {
    return normalized;
  }

  return `${BANGUMI_IMAGE_PROXY_PATH}?url=${encodeURIComponent(target)}`;
}

function normalizeHostname(hostname: string): string {
  return hostname.trim().toLowerCase().replace(/\.$/, "");
}

function isAllowedSiteHostname(hostname: string): boolean {
  const normalized = normalizeHostname(hostname);
  return normalized === ALLOWED_SITE_ROOT || normalized.endsWith(`.${ALLOWED_SITE_ROOT}`);
}

function isLocalDevelopmentHostname(hostname: string): boolean {
  return LOCAL_DEVELOPMENT_HOSTS.has(normalizeHostname(hostname));
}

function parseHeaderHostname(value: string | null): string | null {
  if (!value) return null;

  try {
    return new URL(value).hostname;
  } catch {
    return null;
  }
}

function isAllowedSourceHostname(requestHostname: string, sourceHostname: string): boolean {
  if (isAllowedSiteHostname(sourceHostname)) {
    return true;
  }

  return isLocalDevelopmentHostname(requestHostname) && isLocalDevelopmentHostname(sourceHostname);
}

export function isAllowedBangumiImageProxyRequest(request: Request): boolean {
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

export function getBangumiImageProxyCorsOrigin(request: Request): string | null {
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
