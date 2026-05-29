const BANGUMI_IMAGE_HOSTS = new Set(["lain.bgm.tv", "img.bgm.tv"]);

export const BANGUMI_IMAGE_PROXY_PATH = "/api/image/bgm";
export const BANGUMI_IMAGE_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30;
export const BANGUMI_IMAGE_CACHE_CONTROL = `public, max-age=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, s-maxage=${BANGUMI_IMAGE_CACHE_TTL_SECONDS}, immutable`;
export const BANGUMI_IMAGE_ERROR_CACHE_CONTROL = "public, max-age=300, s-maxage=300";

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
