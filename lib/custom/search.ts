import { searchBangumiSubjects } from "@/lib/bangumi/search";
import { buildAppleSearchItems } from "@/lib/itunes/search";
import { normalizeSearchQuery } from "@/lib/search/query";
import type { CustomSearchItem, CustomSearchSource } from "@/lib/custom/types";

const TMDB_API_BASE_URL = "https://api.themoviedb.org/3";
const SEARCH_LIMIT = 20;

type TmdbMultiResult = {
  id: number;
  media_type?: string;
  name?: string;
  original_name?: string;
  title?: string;
  original_title?: string;
  poster_path?: string | null;
  first_air_date?: string;
  release_date?: string;
};

function extractYear(raw?: string | null): number | undefined {
  if (!raw) return undefined;
  const year = Number.parseInt(raw.slice(0, 4), 10);
  if (!Number.isFinite(year) || year < 1900 || year > 2100) {
    return undefined;
  }
  return year;
}

function buildBangumiSearchItems(query: string) {
  return searchBangumiSubjects({ query, kind: "work" }).then((items) =>
    items.slice(0, SEARCH_LIMIT).map<CustomSearchItem>((item) => ({
      id: String(item.id),
      name: item.name,
      localizedName: item.localizedName,
      cover: item.cover,
      coverMode: "remote",
      source: "bangumi",
      sourceLabel: "Bangumi",
      externalUrl: typeof item.id === "number" || /^\d+$/.test(String(item.id))
        ? `https://bgm.tv/subject/${String(item.id).trim()}`
        : undefined,
      releaseYear: item.releaseYear,
    }))
  );
}

async function fetchTmdbMultiSearch(query: string): Promise<TmdbMultiResult[]> {
  if (!process.env.TMDB_API_READ_ACCESS_TOKEN) {
    throw new Error("process.env.TMDB_API_READ_ACCESS_TOKEN 未配置");
  }

  const url = new URL(`${TMDB_API_BASE_URL}/search/multi`);
  url.searchParams.set("query", query);
  url.searchParams.set("language", "zh-CN");
  url.searchParams.set("page", "1");

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${process.env.TMDB_API_READ_ACCESS_TOKEN}`,
    },
  });

  if (!response.ok) {
    throw new Error(`TMDB search failed: ${response.status}`);
  }

  const json = (await response.json()) as { results?: TmdbMultiResult[] };
  return Array.isArray(json.results) ? json.results : [];
}

async function buildTmdbSearchItems(query: string) {
  const results = await fetchTmdbMultiSearch(query);
  return results
    .filter((item) => item.media_type === "movie" || item.media_type === "tv")
    .slice(0, SEARCH_LIMIT)
    .map<CustomSearchItem>((item) => {
      const mediaType = item.media_type === "tv" ? "tv" : "movie";
      const localizedName = mediaType === "tv" ? item.name : item.title;
      const name = mediaType === "tv" ? item.original_name : item.original_title;
      return {
        id: `${mediaType}:${item.id}`,
        name: name?.trim() || localizedName?.trim() || `TMDB ${item.id}`,
        localizedName:
          localizedName && name && localizedName.trim() !== name.trim()
            ? localizedName.trim()
            : undefined,
        cover: item.poster_path ? `https://image.tmdb.org/t/p/w500${item.poster_path}` : null,
        coverMode: "remote",
        source: "tmdb",
        sourceLabel: "TMDB",
        externalUrl: `https://www.themoviedb.org/${mediaType}/${item.id}`,
        releaseYear: extractYear(mediaType === "tv" ? item.first_air_date : item.release_date),
      };
    });
}

export function parseCustomSearchSource(value: string | null | undefined): CustomSearchSource | null {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  if (normalized === "bangumi" || normalized === "tmdb" || normalized === "apple") {
    return normalized;
  }
  return null;
}

export async function searchCustomSource(params: {
  source: CustomSearchSource;
  query: string;
}): Promise<CustomSearchItem[]> {
  const { source, query } = params;
  const normalizedQuery = normalizeSearchQuery(query);
  if (!normalizedQuery) {
    return [];
  }

  if (source === "bangumi") {
    return buildBangumiSearchItems(normalizedQuery);
  }
  if (source === "tmdb") {
    return buildTmdbSearchItems(normalizedQuery);
  }
  return buildAppleSearchItems(normalizedQuery);
}
