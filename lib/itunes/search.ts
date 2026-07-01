import { SubjectKind } from "@/lib/subject-kind";
import { ShareSubject, SubjectSearchResponse } from "@/lib/share/types";
import { resolveItunesStorefrontForQuery, type ItunesStorefront } from "@/lib/itunes/storefront";

const ITUNES_API_BASE_URL = "https://itunes.apple.com";
const ITUNES_RETRY_MAX_ATTEMPTS = 3;
const ITUNES_RETRY_BASE_DELAY_MS = 300;
const ITUNES_RETRY_MAX_DELAY_MS = 10 * 1000;
const ITUNES_RETRYABLE_STATUS = new Set([408, 425, 500, 502, 503, 504]);
const ITUNES_RELEVANT_MATCH_SCORE = 25;
const ITUNES_ALBUM_FALLBACK_TRACK_LIMIT = 40;

type ItunesTrackResult = {
  wrapperType: string;
  kind?: string;
  artistId?: number;
  collectionId?: number;
  trackId?: number;
  artistName: string;
  collectionName?: string;
  trackName: string;
  artworkUrl100?: string;
  releaseDate?: string;
  primaryGenreName?: string;
  collectionViewUrl?: string;
  trackViewUrl?: string;
};

type ItunesCollectionResult = {
  wrapperType: string;
  collectionType?: string;
  artistId?: number;
  collectionId: number;
  artistName: string;
  collectionName: string;
  artworkUrl100?: string;
  releaseDate?: string;
  primaryGenreName?: string;
  collectionViewUrl?: string;
};

type ItunesSearchResult = ItunesTrackResult | ItunesCollectionResult;

type ItunesAlbumLikeResult = {
  artistName: string;
  collectionId: number;
  collectionName: string;
  artworkUrl100?: string;
  releaseDate?: string;
  primaryGenreName?: string;
  collectionViewUrl?: string;
};

export type ItunesMixedSearchResult = {
  songs: ShareSubject[];
  albums: ShareSubject[];
};

class ItunesHttpError extends Error {
  status: number;

  constructor(status: number) {
    super(`iTunes search failed: ${status}`);
    this.name = "ItunesHttpError";
    this.status = status;
  }
}

function extractYear(raw?: string | null): number | undefined {
  if (!raw) return undefined;
  const year = Number.parseInt(raw.slice(0, 4), 10);
  if (!Number.isFinite(year) || year < 1970 || year > 2100) {
    return undefined;
  }
  return year;
}

function normalizeText(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, "");
}

function tokenizeText(value: string): string[] {
  if (!value.trim()) {
    return [];
  }

  return Array.from(
    new Set(
      value
        .trim()
        .toLowerCase()
        .split(/[\s\-_/,:;|&()[\]{}"'`~!@#$%^*+=?.<>\\]+/)
        .map((token) => token.trim())
        .filter((token) => token.length > 1)
    )
  );
}

function enhanceArtworkUrl(url?: string | null): string | null {
  if (!url) return null;
  // Replace 100x100bb with a much higher resolution like 1000x1000bb
  return url.replace("100x100bb", "1000x1000bb");
}

function resolveSongSubjectId(result: ItunesTrackResult): number | null {
  if (typeof result.trackId === "number" && Number.isFinite(result.trackId)) {
    return result.trackId;
  }
  if (typeof result.collectionId === "number" && Number.isFinite(result.collectionId)) {
    return result.collectionId;
  }
  return null;
}

function toShareSongSubject(result: ItunesTrackResult): ShareSubject | null {
  const id = resolveSongSubjectId(result);
  if (id === null) {
    return null;
  }

  const cover = enhanceArtworkUrl(result.artworkUrl100);
  const releaseYear = extractYear(result.releaseDate);
  const genres = result.primaryGenreName ? [result.primaryGenreName] : [];

  return {
    id,
    name: result.artistName, // Name 原本项目组件在副标题展示，用于显示歌手
    localizedName: result.trackName, // LocalizedName 原本组件做大标题展示，用于显示歌名
    cover,
    releaseYear,
    genres,
    storeUrls: {
      apple: result.trackViewUrl || result.collectionViewUrl || "",
    },
  };
}

function toAlbumLikeResultFromTrack(result: ItunesTrackResult): ItunesAlbumLikeResult | null {
  if (
    typeof result.collectionId !== "number" ||
    !Number.isFinite(result.collectionId) ||
    typeof result.collectionName !== "string" ||
    !result.collectionName.trim()
  ) {
    return null;
  }

  return {
    artistName: result.artistName,
    collectionId: result.collectionId,
    collectionName: result.collectionName,
    artworkUrl100: result.artworkUrl100,
    releaseDate: result.releaseDate,
    primaryGenreName: result.primaryGenreName,
    collectionViewUrl: result.collectionViewUrl,
  };
}

function toShareAlbumSubject(result: ItunesAlbumLikeResult): ShareSubject {
  const cover = enhanceArtworkUrl(result.artworkUrl100);
  const releaseYear = extractYear(result.releaseDate);
  const genres = result.primaryGenreName ? [result.primaryGenreName] : [];

  return {
    id: result.collectionId,
    name: result.artistName, 
    localizedName: result.collectionName,
    cover,
    releaseYear,
    genres,
    storeUrls: {
      apple: result.collectionViewUrl || "",
    },
  };
}

function isItunesTrackResult(value: ItunesSearchResult): value is ItunesTrackResult {
  const maybe = value as Partial<ItunesTrackResult>;
  return (
    value.wrapperType === "track" &&
    typeof maybe.artistName === "string" &&
    typeof maybe.trackName === "string"
  );
}

function isItunesCollectionResult(value: ItunesSearchResult): value is ItunesCollectionResult {
  const maybe = value as Partial<ItunesCollectionResult>;
  return (
    value.wrapperType === "collection" &&
    typeof maybe.collectionId === "number" &&
    Number.isFinite(maybe.collectionId) &&
    typeof maybe.artistName === "string" &&
    typeof maybe.collectionName === "string"
  );
}

function isRetryableStatus(status: number): boolean {
  return ITUNES_RETRYABLE_STATUS.has(status);
}

function createItunesRequestHeaders(): HeadersInit {
  const headers: Record<string, string> = {
    Accept: "application/json",
    "Accept-Language": "en-US,en;q=0.9",
  };

  if (typeof window === "undefined") {
    headers["User-Agent"] = "Mozilla/5.0 (compatible; My9/1.0; +https://my9.shatranj.space)";
  }

  return headers;
}

function isRetryableFetchError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  if (error.name === "AbortError") {
    return false;
  }
  return true;
}

function parseRetryAfterMs(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  if (/^\d+$/.test(trimmed)) {
    const seconds = Number.parseInt(trimmed, 10);
    if (!Number.isFinite(seconds) || seconds < 0) {
      return null;
    }
    return seconds * 1000;
  }
  const dateMs = Date.parse(trimmed);
  if (!Number.isFinite(dateMs)) {
    return null;
  }
  return Math.max(0, dateMs - Date.now());
}

function computeRetryDelayMs(params: {
  attempt: number;
  retryAfterMs?: number | null;
}): number {
  const { attempt, retryAfterMs } = params;
  if (typeof retryAfterMs === "number" && Number.isFinite(retryAfterMs)) {
    return Math.min(Math.max(0, Math.trunc(retryAfterMs)), ITUNES_RETRY_MAX_DELAY_MS);
  }
  const exponentialDelay = Math.min(
    ITUNES_RETRY_BASE_DELAY_MS * 2 ** Math.max(0, attempt - 1),
    ITUNES_RETRY_MAX_DELAY_MS
  );
  const jitter = Math.floor(Math.random() * 120);
  return Math.min(exponentialDelay + jitter, ITUNES_RETRY_MAX_DELAY_MS);
}

async function sleep(ms: number): Promise<void> {
  if (ms <= 0) {
    return;
  }
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function fetchItunesSearch<T>(
  term: string,
  options?: {
    entity?: "musicTrack" | "album";
    country?: ItunesStorefront;
    limit?: number;
  }
): Promise<T[]> {
  const url = new URL(`${ITUNES_API_BASE_URL}/search`);
  url.searchParams.set("term", term);
  url.searchParams.set("media", "music");
  if (options?.entity) {
    url.searchParams.set("entity", options.entity);
  }
  url.searchParams.set("country", options?.country ?? "us");
  url.searchParams.set("limit", String(options?.limit ?? 20));

  const requestInit = {
    method: "GET",
    headers: createItunesRequestHeaders(),
    next: { revalidate: 0 },
  } as RequestInit & { next?: { revalidate?: number } };

  let lastError: unknown = null;

  for (let attempt = 1; attempt <= ITUNES_RETRY_MAX_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetch(url.toString(), requestInit);
      if (response.ok) {
        const json = (await response.json()) as { results?: T[] };
        return Array.isArray(json?.results) ? json.results : [];
      }

      const canRetry =
        attempt < ITUNES_RETRY_MAX_ATTEMPTS && isRetryableStatus(response.status);
      if (!canRetry) {
        throw new ItunesHttpError(response.status);
      }

      const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
      const waitMs = computeRetryDelayMs({
        attempt,
        retryAfterMs,
      });
      await sleep(waitMs);
    } catch (error) {
      lastError = error;
      if (error instanceof ItunesHttpError) {
        throw error;
      }
      const canRetry =
        attempt < ITUNES_RETRY_MAX_ATTEMPTS && isRetryableFetchError(error);
      if (!canRetry) {
        throw error;
      }
      const waitMs = computeRetryDelayMs({ attempt });
      await sleep(waitMs);
    }
  }

  if (lastError instanceof Error) {
    throw lastError;
  }
  throw new Error("iTunes search failed");
}

function scoreCandidate(query: string, subject: ShareSubject): number {
  const q = normalizeText(query);
  if (!q) return 0;

  const title = subject.localizedName || "";
  const subtitle = subject.name;
  const combined = [title, subtitle].filter(Boolean).join(" ");
  const candidates = [title, subtitle, combined];
  let score = 0;

  for (const text of candidates) {
    const normalized = normalizeText(text);
    if (!normalized) continue;
    if (normalized === q) score += 100;
    if (normalized.startsWith(q)) score += 60;
    if (normalized.includes(q)) score += 25;
  }

  const queryTokens = tokenizeText(query);
  if (queryTokens.length > 1) {
    const normalizedCombined = normalizeText(combined);
    const matchedCount = queryTokens.reduce((count, token) => {
      const normalizedToken = normalizeText(token);
      if (!normalizedToken || !normalizedCombined.includes(normalizedToken)) {
        return count;
      }
      return count + 1;
    }, 0);

    if (matchedCount > 0) {
      score += matchedCount * 8;
      if (matchedCount === queryTokens.length) {
        score += 40;
      }
    }
  }

  if (typeof subject.releaseYear === "number") {
    const yearText = String(subject.releaseYear);
    if (yearText.includes(q)) score += 5;
  }

  return score;
}

function hasRelevantSearchMatch(query: string, items: ShareSubject[]): boolean {
  return items.some((item) => scoreCandidate(query, item) >= ITUNES_RELEVANT_MATCH_SCORE);
}

function sortBySearchScore(query: string, items: ShareSubject[]): ShareSubject[] {
  return items
    .map((item, index) => ({
      item,
      index,
      score: scoreCandidate(query, item),
    }))
    .sort((a, b) => b.score - a.score || a.index - b.index)
    .map(({ item }) => item);
}

function dedupeSubjectsById(items: ShareSubject[]): ShareSubject[] {
  const seen = new Set<string>();
  const deduped: ShareSubject[] = [];

  for (const item of items) {
    const key = String(item.id);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(item);
  }

  return deduped;
}

function reorderByPromotedIds<T extends { id: number | string }>(
  items: T[],
  promotedIds: Array<number | string>
): T[] {
  if (items.length === 0 || promotedIds.length === 0) {
    return items;
  }

  const promotedSet = new Set(promotedIds.map((id) => String(id)));
  const promoted: T[] = [];
  const rest: T[] = [];

  for (const item of items) {
    if (promotedSet.has(String(item.id))) {
      promoted.push(item);
    } else {
      rest.push(item);
    }
  }

  return [...promoted, ...rest];
}

export function buildItunesSearchResponse(params: {
  query: string;
  kind: SubjectKind;
  items: ShareSubject[];
}): SubjectSearchResponse {
  const { query, kind, items } = params;

  const ranked = items
    .map((item) => ({
      id: item.id,
      score: scoreCandidate(query, item),
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((item) => item.id);

  const promotedIds =
    ranked.length > 0 ? ranked : items.slice(0, 2).map((item) => item.id);
  const orderedItems = reorderByPromotedIds(items, promotedIds);

  return {
    ok: true,
    source: "itunes",
    kind,
    items: orderedItems,
    noResultQuery: items.length === 0 && query.trim() ? query : null,
  };
}

export async function searchItunesSong(params: {
  query: string;
  kind: SubjectKind;
}): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) return [];
  const storefront = resolveItunesStorefrontForQuery(q);

  const results = await fetchItunesSearch<ItunesTrackResult>(q, {
    entity: "musicTrack",
    country: storefront,
    limit: 20,
  });
  return results
    .filter((r) => r.wrapperType === "track")
    .map(toShareSongSubject)
    .filter((item): item is ShareSubject => item !== null);
}

export async function searchItunesAlbum(params: {
  query: string;
  kind: SubjectKind;
}): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) return [];
  const storefront = resolveItunesStorefrontForQuery(q);

  const results = await fetchItunesSearch<ItunesCollectionResult>(q, {
    entity: "album",
    country: storefront,
    limit: 20,
  });
  const directItems = results.filter((r) => r.wrapperType === "collection").map(toShareAlbumSubject);

  if (hasRelevantSearchMatch(q, directItems)) {
    return directItems;
  }

  // Some Apple Music albums are only discoverable through track search even though
  // the album itself can still be resolved by collectionId via the iTunes API.
  const fallbackTrackResults = await fetchItunesSearch<ItunesTrackResult>(q, {
    entity: "musicTrack",
    country: storefront,
    limit: ITUNES_ALBUM_FALLBACK_TRACK_LIMIT,
  });

  const fallbackItems = sortBySearchScore(
    q,
    dedupeSubjectsById(
      fallbackTrackResults
        .filter((result) => result.wrapperType === "track")
        .map(toAlbumLikeResultFromTrack)
        .filter((item): item is ItunesAlbumLikeResult => item !== null)
        .map(toShareAlbumSubject)
        .filter((item) => scoreCandidate(q, item) >= ITUNES_RELEVANT_MATCH_SCORE)
    )
  );

  if (fallbackItems.length === 0) {
    return directItems;
  }

  return dedupeSubjectsById([...fallbackItems, ...directItems]);
}

export async function searchItunesMixed(params: {
  query: string;
}): Promise<ItunesMixedSearchResult> {
  const { query } = params;
  const q = query.trim();
  if (!q) {
    return {
      songs: [],
      albums: [],
    };
  }

  const storefront = resolveItunesStorefrontForQuery(q);
  const results = await fetchItunesSearch<ItunesSearchResult>(q, {
    country: storefront,
    limit: 40,
  });

  const songs: ShareSubject[] = [];
  const albums: ShareSubject[] = [];

  for (const result of results) {
    if (isItunesTrackResult(result)) {
      const song = toShareSongSubject(result);
      if (song) {
        songs.push(song);
      }
      continue;
    }
    if (isItunesCollectionResult(result)) {
      albums.push(toShareAlbumSubject(result));
    }
  }

  return {
    songs,
    albums,
  };
}
