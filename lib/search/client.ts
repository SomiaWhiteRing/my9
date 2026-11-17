import { buildCustomSearchResponse, type CustomSearchResponse, type CustomSearchSource } from "@/lib/custom/types";
import {
  buildAppleSearchItems, buildItunesSearchResponse, ItunesHttpError, rememberItunesRateLimit,
  searchItunesAlbum, searchItunesSong,
} from "@/lib/itunes/search";
import type { SubjectSearchResponse } from "@/lib/share/types";
import type { SubjectKind } from "@/lib/subject-kind";
import { normalizeSearchQuery } from "./query";

async function fetchSearch<T extends { ok: boolean }>(url: string, direct?: () => Promise<T>): Promise<T & { error?: string }> {
  if (direct) {
    try {
      return await direct();
    } catch (error) {
      const unavailable = error instanceof TypeError || (error instanceof Error && error.name === "TimeoutError")
        || (error instanceof ItunesHttpError && error.retryable);
      if (!unavailable) throw error;
    }
  }

  const response = await fetch(url);
  if (direct && response.status === 429) rememberItunesRateLimit(response.headers.get("Retry-After"));
  const data = await response.json() as T & { error?: string };
  return response.ok ? data : { ...data, ok: false };
}

export function searchSubjectsFromBrowser(query: string, kind: SubjectKind) {
  const url = `/api/subjects/search?q=${encodeURIComponent(query)}&kind=${kind}`;
  if (kind !== "song" && kind !== "album") return fetchSearch<SubjectSearchResponse>(url);
  return fetchSearch(url, async () => {
    const items = kind === "song" ? await searchItunesSong({ query, kind }) : await searchItunesAlbum({ query, kind });
    return buildItunesSearchResponse({ query, kind, items });
  });
}

export function searchCustomFromBrowser(query: string, source: CustomSearchSource) {
  const url = `/api/custom/search?q=${encodeURIComponent(query)}&source=${source}`;
  if (source !== "apple") return fetchSearch<CustomSearchResponse>(url);
  return fetchSearch(url, async () => {
    const normalized = normalizeSearchQuery(query);
    const items = await buildAppleSearchItems(normalized);
    return buildCustomSearchResponse({ source, query: normalized, items });
  });
}
