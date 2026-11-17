import settings from "#public-settings";
const DEFAULT_SITE_URL = "https://my9.shatranj.space";

function normalizeSiteUrl(value: string | null | undefined): string {
  if (typeof value !== "string") {
    return DEFAULT_SITE_URL;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return DEFAULT_SITE_URL;
  }

  try {
    return new URL(trimmed).origin;
  } catch {
    return DEFAULT_SITE_URL;
  }
}

export function getServerSiteUrl(): string {
  return normalizeSiteUrl(settings.siteUrl);
}

export function getPublicSiteUrl(): string {
  return normalizeSiteUrl(settings.siteUrl);
}

export function getSiteHost(siteUrl: string): string {
  try {
    return new URL(siteUrl).host;
  } catch {
    return new URL(DEFAULT_SITE_URL).host;
  }
}

export function getSiteHostname(siteUrl: string): string {
  try {
    return new URL(siteUrl).hostname;
  } catch {
    return new URL(DEFAULT_SITE_URL).hostname;
  }
}
