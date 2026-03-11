import { SubjectKind, getSubjectKindMeta } from "@/lib/subject-kind";
import { ShareSubject, SubjectSearchResponse } from "@/lib/share/types";

const BANGUMI_API_BASE_URL = "https://api.bgm.tv";
const BANGUMI_ACCESS_TOKEN = process.env.BANGUMI_ACCESS_TOKEN;
const BANGUMI_USER_AGENT = process.env.BANGUMI_USER_AGENT;
const BANGUMI_EXACT_KEYWORD_OVERRIDES: Record<string, string> = {
  // Bangumi API currently misses this query under simplified Chinese.
  仙剑奇侠传: "仙劍奇俠傳",
};

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
    `可尝试${kindLabel}正式名或别名`,
    "中日英名称切换检索通常更有效",
    "减少关键词，仅保留核心词",
  ];
}

export function buildBangumiSearchResponse(
  params: {
    query: string;
    kind: SubjectKind;
    items: ShareSubject[];
  }
): SubjectSearchResponse {
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

  const topPickIds = ranked.length > 0 ? ranked : items.slice(0, 2).map((item) => item.id);

  return {
    ok: true,
    source: "bangumi",
    kind,
    items,
    topPickIds,
    suggestions: createSuggestions(kind),
    noResultQuery: items.length === 0 && query.trim() ? query : null,
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
    headers: {
      "User-Agent": BANGUMI_USER_AGENT || "My9/4.0",
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(BANGUMI_ACCESS_TOKEN
        ? { Authorization: `Bearer ${BANGUMI_ACCESS_TOKEN}` }
        : {}),
    },
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

  return items.slice(0, 20);
}
