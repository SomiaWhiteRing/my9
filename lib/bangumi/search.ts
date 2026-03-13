import { SubjectKind, getSubjectKindMeta } from "@/lib/subject-kind";
import { ShareSubject, SubjectSearchResponse } from "@/lib/share/types";

const BANGUMI_API_BASE_URL = "https://api.bgm.tv";
const BANGUMI_ACCESS_TOKEN = process.env.BANGUMI_ACCESS_TOKEN;
const BANGUMI_USER_AGENT = process.env.BANGUMI_USER_AGENT;
const BANGUMI_EXACT_KEYWORD_OVERRIDES: Record<string, string> = {
  // Bangumi API currently misses this query under simplified Chinese.
  仙剑奇侠传: "仙劍奇俠傳",
  哆啦A梦: "ドラえもん",
  哆啦a梦: "ドラえもん",
};
const BANGUMI_BLOCKED_SUBJECT_TITLES = new Set(["devil lover"]);

type BangumiV0Tag = {
  name?: string;
};

type BangumiV0Images = {
  large?: string;
  common?: string;
  medium?: string;
  small?: string;
  grid?: string;
};

type BangumiV0Subject = {
  id: number;
  type: number;
  name: string;
  name_cn?: string;
  date?: string;
  platform?: string | null;
  images?: BangumiV0Images | null;
  tags?: BangumiV0Tag[];
};

type BangumiV0PagedSubject = {
  total?: number;
  limit?: number;
  offset?: number;
  data?: BangumiV0Subject[];
};

type BangumiV0CharacterImages = {
  large?: string;
  medium?: string;
  small?: string;
  grid?: string;
};

type BangumiV0Character = {
  id: number;
  name: string;
  type?: number;
  images?: BangumiV0CharacterImages | null;
  short_summary?: string;
};

type BangumiV0PagedCharacter = {
  total?: number;
  limit?: number;
  offset?: number;
  data?: BangumiV0Character[];
};

type BangumiV0PersonImages = {
  large?: string;
  medium?: string;
  small?: string;
  grid?: string;
};

type BangumiV0Person = {
  id: number;
  name: string;
  type?: number;
  images?: BangumiV0PersonImages | null;
  short_summary?: string;
  career?: string[];
};

type BangumiV0PagedPerson = {
  total?: number;
  limit?: number;
  offset?: number;
  data?: BangumiV0Person[];
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

function normalizeBlockedTitle(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}

function isBlockedSubject(subject: Pick<ShareSubject, "name" | "localizedName">): boolean {
  const titleCandidates = [subject.name, subject.localizedName || ""];
  return titleCandidates.some((title) =>
    BANGUMI_BLOCKED_SUBJECT_TITLES.has(normalizeBlockedTitle(title))
  );
}

function filterBlockedSubjects<T extends Pick<ShareSubject, "name" | "localizedName">>(
  items: T[]
): T[] {
  return items.filter((item) => !isBlockedSubject(item));
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

function toSearchResponseItem(item: ShareSubject): ShareSubject {
  return {
    id: item.id,
    name: item.name,
    localizedName: item.localizedName,
    cover: item.cover,
    releaseYear: item.releaseYear,
    genres: Array.isArray(item.genres) ? item.genres.slice(0, 3) : undefined,
  };
}

export function buildBangumiSearchResponse(
  params: {
    query: string;
    kind: SubjectKind;
    items: ShareSubject[];
  }
): SubjectSearchResponse {
  const { query, kind, items } = params;
  const visibleItems = filterBlockedSubjects(items);
  const responseItems = visibleItems.map(toSearchResponseItem);
  const ranked = visibleItems
    .map((item) => ({
      id: item.id,
      score: scoreCandidate(query, item),
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((item) => item.id);

  const promotedIds =
    ranked.length > 0 ? ranked : responseItems.slice(0, 2).map((item) => item.id);
  const orderedItems = reorderByPromotedIds(responseItems, promotedIds);

  return {
    ok: true,
    source: "bangumi",
    kind,
    items: orderedItems,
    noResultQuery: visibleItems.length === 0 && query.trim() ? query : null,
  };
}

function toBangumiKeyword(query: string): string {
  return BANGUMI_EXACT_KEYWORD_OVERRIDES[query] || query;
}

function toShareSubject(item: BangumiV0Subject): ShareSubject {
  const cover =
    item.images?.large ||
    item.images?.common ||
    item.images?.medium ||
    item.images?.small ||
    item.images?.grid ||
    null;

  const subjectPlatform = typeof item.platform === "string" ? item.platform.trim() : null;
  const genres = Array.isArray(item.tags)
    ? item.tags
        .map((tag) => tag?.name?.trim())
        .filter((name): name is string => Boolean(name))
        .slice(0, 3)
    : [];

  return {
    id: item.id,
    name: item.name,
    localizedName: item.name_cn || undefined,
    cover,
    releaseYear: extractYear(item.date),
    gameTypeId: 0,
    platforms: subjectPlatform ? [subjectPlatform] : [],
    genres,
    subjectType: item.type,
    subjectPlatform,
  };
}

export async function searchBangumiSubjects(
  params: {
    query: string;
    kind: SubjectKind;
  }
): Promise<ShareSubject[]> {
  const { query, kind } = params;
  const q = query.trim();
  if (!q) {
    return [];
  }
  const keyword = toBangumiKeyword(q);

  const kindMeta = getSubjectKindMeta(kind);
  const requestBody: {
    keyword: string;
    sort: "match";
    filter: {
      type?: number[];
    };
  } = {
    keyword,
    sort: "match",
    filter: {},
  };

  if (Array.isArray(kindMeta.search.typeFilter) && kindMeta.search.typeFilter.length > 0) {
    requestBody.filter.type = kindMeta.search.typeFilter;
  }

  const response = await fetch(`${BANGUMI_API_BASE_URL}/v0/search/subjects?limit=20`, {
    method: "POST",
    headers: toBangumiRequestHeaders(),
    body: JSON.stringify(requestBody),
    next: { revalidate: 0 },
  });

  if (!response.ok) {
    throw new Error(`Bangumi search failed: ${response.status}`);
  }

  const json = (await response.json()) as BangumiV0PagedSubject;
  const list = Array.isArray(json?.data) ? json.data : [];

  let items = list.map(toShareSubject);
  const strictPlatform = kindMeta.search.strictPlatform;
  if (strictPlatform) {
    items = items.filter((item) => item.subjectPlatform === strictPlatform);
  }
  items = filterBlockedSubjects(items);

  return items.slice(0, 20);
}

function toBangumiRequestHeaders() {
  return {
    "User-Agent": BANGUMI_USER_AGENT || "My9/4.0",
    Accept: "application/json",
    "Content-Type": "application/json",
    ...(BANGUMI_ACCESS_TOKEN ? { Authorization: `Bearer ${BANGUMI_ACCESS_TOKEN}` } : {}),
  };
}

function toShareSubjectFromCharacter(item: BangumiV0Character): ShareSubject {
  const cover =
    item.images?.large ||
    item.images?.medium ||
    item.images?.small ||
    item.images?.grid ||
    null;
  return {
    id: item.id,
    name: item.name,
    localizedName: undefined,
    cover,
    subjectType: item.type,
  };
}

function toShareSubjectFromPerson(item: BangumiV0Person): ShareSubject {
  const cover =
    item.images?.large ||
    item.images?.medium ||
    item.images?.small ||
    item.images?.grid ||
    null;
  return {
    id: item.id,
    name: item.name,
    localizedName: undefined,
    cover,
    genres: Array.isArray(item.career) ? item.career.slice(0, 3) : [],
    subjectType: item.type,
  };
}

export async function searchBangumiCharacters(params: { query: string }): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) return [];

  const response = await fetch(`${BANGUMI_API_BASE_URL}/v0/search/characters?limit=20`, {
    method: "POST",
    headers: toBangumiRequestHeaders(),
    body: JSON.stringify({ keyword: q }),
    next: { revalidate: 0 },
  });

  if (!response.ok) {
    throw new Error(`Bangumi character search failed: ${response.status}`);
  }

  const json = (await response.json()) as BangumiV0PagedCharacter;
  const list = Array.isArray(json?.data) ? json.data : [];
  return list.map(toShareSubjectFromCharacter).slice(0, 20);
}

export async function searchBangumiPersons(params: { query: string }): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) return [];

  const response = await fetch(`${BANGUMI_API_BASE_URL}/v0/search/persons?limit=20`, {
    method: "POST",
    headers: toBangumiRequestHeaders(),
    body: JSON.stringify({ keyword: q }),
    next: { revalidate: 0 },
  });

  if (!response.ok) {
    throw new Error(`Bangumi person search failed: ${response.status}`);
  }

  const json = (await response.json()) as BangumiV0PagedPerson;
  const list = Array.isArray(json?.data) ? json.data : [];
  return list.map(toShareSubjectFromPerson).slice(0, 20);
}
