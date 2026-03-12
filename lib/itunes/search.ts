import { SubjectKind, getSubjectKindMeta } from "@/lib/subject-kind";
import { ShareSubject, SubjectSearchResponse } from "@/lib/share/types";

const ITUNES_API_BASE_URL = "https://itunes.apple.com";

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

function enhanceArtworkUrl(url?: string | null): string | null {
  if (!url) return null;
  // Replace 100x100bb with a much higher resolution like 1000x1000bb
  return url.replace("100x100bb", "1000x1000bb");
}

function toShareSongSubject(result: ItunesTrackResult): ShareSubject {
  const cover = enhanceArtworkUrl(result.artworkUrl100);
  const releaseYear = extractYear(result.releaseDate);
  const genres = result.primaryGenreName ? [result.primaryGenreName] : [];

  return {
    id: result.trackId || result.collectionId || Math.random(),
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

function toShareAlbumSubject(result: ItunesCollectionResult): ShareSubject {
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

async function fetchItunesSearch<T>(
  term: string,
  entity: "musicTrack" | "album"
): Promise<T[]> {
  const url = new URL(`${ITUNES_API_BASE_URL}/search`);
  url.searchParams.set("term", term);
  url.searchParams.set("media", "music");
  url.searchParams.set("entity", entity);
  url.searchParams.set("country", "cn"); // Default to China region for better local results
  url.searchParams.set("limit", "20");

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
    next: { revalidate: 0 },
  } as RequestInit & { next?: { revalidate?: number } });

  if (!response.ok) {
    throw new Error(`iTunes search failed: ${response.status}`);
  }

  const json = (await response.json()) as { results?: T[] };
  return Array.isArray(json?.results) ? json.results : [];
}

function scoreCandidate(query: string, subject: ShareSubject): number {
  const q = normalizeText(query);
  if (!q) return 0;

  const candidates = [subject.localizedName || "", subject.name];
  let score = 0;

  for (const text of candidates) {
    const normalized = normalizeText(text);
    if (!normalized) continue;
    if (normalized === q) score += 100;
    if (normalized.startsWith(q)) score += 60;
    if (normalized.includes(q)) score += 25;
  }

  if (typeof subject.releaseYear === "number") {
    const yearText = String(subject.releaseYear);
    if (yearText.includes(q)) score += 5;
  }

  return score;
}

function createSuggestions(kind: SubjectKind): string[] {
  const kindLabel = getSubjectKindMeta(kind).label;
  return [
    `尝试输入"歌手名 ${kindLabel}名"进行精确组合搜索`,
    "如果搜不到，可以尝试繁体字或英文原名",
    "减少多余的关键词，仅保留核心要素",
  ];
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

  const topPickIds =
    ranked.length > 0 ? ranked : items.slice(0, 2).map((item) => item.id);

  return {
    ok: true,
    source: "itunes",
    kind,
    items,
    topPickIds,
    suggestions: createSuggestions(kind),
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

  const results = await fetchItunesSearch<ItunesTrackResult>(q, "musicTrack");
  return results.filter(r => r.wrapperType === "track").map(toShareSongSubject);
}

export async function searchItunesAlbum(params: {
  query: string;
  kind: SubjectKind;
}): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) return [];

  const results = await fetchItunesSearch<ItunesCollectionResult>(q, "album");
  return results.filter(r => r.wrapperType === "collection").map(toShareAlbumSubject);
}
