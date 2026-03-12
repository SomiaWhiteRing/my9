import { NextResponse } from "next/server";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";
import { normalizeSearchQuery } from "@/lib/search/query";
import { buildItunesSearchResponse, searchItunesSong, searchItunesAlbum } from "@/lib/itunes/search";

const SEARCH_CDN_TTL_SECONDS = 900;
const SEARCH_STALE_TTL_SECONDS = 86400;
const SEARCH_MEMORY_TTL_MS = 3 * 60 * 1000;
const SEARCH_MEMORY_CACHE_MAX = 256;
const SEARCH_RATE_LIMIT_WINDOW_MS = 10 * 1000;
const SEARCH_RATE_LIMIT_MAX_REQUESTS = 12;
const SEARCH_RATE_LIMIT_STORE_MAX = 20000;

const SEARCH_CACHE_CONTROL_VALUE = `public, max-age=0, s-maxage=${SEARCH_CDN_TTL_SECONDS}, stale-while-revalidate=${SEARCH_STALE_TTL_SECONDS}`;
const NO_STORE_HEADERS = {
  "Cache-Control": "no-store, max-age=0",
};

type SearchItems = Awaited<ReturnType<typeof searchItunesSong>>;
type SearchMemoryStore = {
  resultCache: Map<string, { expiresAt: number; items: SearchItems }>;
  inflight: Map<string, Promise<SearchItems>>;
  rateLimit: Map<string, { windowStart: number; count: number }>;
  rateLimitBlockedCount: number;
};

function getSearchMemoryStore(): SearchMemoryStore {
  const g = globalThis as typeof globalThis & {
    __MY9_ITUNES_SEARCH_MEMORY__?: SearchMemoryStore;
  };

  if (!g.__MY9_ITUNES_SEARCH_MEMORY__) {
    g.__MY9_ITUNES_SEARCH_MEMORY__ = {
      resultCache: new Map<string, { expiresAt: number; items: SearchItems }>(),
      inflight: new Map<string, Promise<SearchItems>>(),
      rateLimit: new Map<string, { windowStart: number; count: number }>(),
      rateLimitBlockedCount: 0,
    };
  }

  return g.__MY9_ITUNES_SEARCH_MEMORY__;
}

function trimSearchMemoryCache(cache: Map<string, { expiresAt: number; items: SearchItems }>) {
  while (cache.size > SEARCH_MEMORY_CACHE_MAX) {
    const firstKey = cache.keys().next().value;
    if (!firstKey) return;
    cache.delete(firstKey);
  }
}

function toSearchCacheKey(kind: SubjectKind, query: string) {
  return `${kind}:${normalizeSearchQuery(query)}`;
}

function toRateLimitKey(kind: SubjectKind, ip: string) {
  return `${kind}:${ip}`;
}

function parseForwardedFor(value: string): string | null {
  const first = value
    .split(",")
    .map((part) => part.trim())
    .find((part) => part.length > 0);
  return first || null;
}

function getClientIp(request: Request): string | null {
  const forwarded = request.headers.get("x-forwarded-for");
  if (forwarded) {
    return parseForwardedFor(forwarded);
  }

  const direct =
    request.headers.get("x-real-ip") ||
    request.headers.get("cf-connecting-ip") ||
    request.headers.get("x-vercel-forwarded-for") ||
    request.headers.get("x-client-ip");

  if (!direct) return null;
  const trimmed = direct.trim();
  return trimmed || null;
}

function trimRateLimitStore(
  rateLimit: Map<string, { windowStart: number; count: number }>,
  now: number
) {
  const expiredKeys: string[] = [];
  rateLimit.forEach((value, key) => {
    if (now - value.windowStart >= SEARCH_RATE_LIMIT_WINDOW_MS) {
      expiredKeys.push(key);
    }
  });

  for (const key of expiredKeys) {
    rateLimit.delete(key);
  }

  while (rateLimit.size > SEARCH_RATE_LIMIT_STORE_MAX) {
    const firstKey = rateLimit.keys().next().value;
    if (!firstKey) return;
    rateLimit.delete(firstKey);
  }
}

function checkSearchRateLimit(request: Request, kind: SubjectKind): {
  limited: boolean;
  retryAfterSeconds: number;
} {
  const ip = getClientIp(request);
  if (!ip) {
    return { limited: false, retryAfterSeconds: 0 };
  }

  const now = Date.now();
  const memory = getSearchMemoryStore();
  const key = toRateLimitKey(kind, ip);
  const existing = memory.rateLimit.get(key);

  if (!existing || now - existing.windowStart >= SEARCH_RATE_LIMIT_WINDOW_MS) {
    memory.rateLimit.set(key, { windowStart: now, count: 1 });
    trimRateLimitStore(memory.rateLimit, now);
    return { limited: false, retryAfterSeconds: 0 };
  }

  if (existing.count >= SEARCH_RATE_LIMIT_MAX_REQUESTS) {
    trimRateLimitStore(memory.rateLimit, now);
    const retryAfterSeconds = Math.max(
      1,
      Math.ceil((SEARCH_RATE_LIMIT_WINDOW_MS - (now - existing.windowStart)) / 1000)
    );
    memory.rateLimitBlockedCount += 1;
    if (memory.rateLimitBlockedCount <= 5 || memory.rateLimitBlockedCount % 50 === 0) {
      console.warn(
        `[itunes-search-rate-limit] blocked=${memory.rateLimitBlockedCount} kind=${kind} retry=${retryAfterSeconds}s`
      );
    }
    return { limited: true, retryAfterSeconds };
  }

  existing.count += 1;
  memory.rateLimit.set(key, existing);
  trimRateLimitStore(memory.rateLimit, now);

  return { limited: false, retryAfterSeconds: 0 };
}

function createSearchCacheHeaders() {
  return {
    "Cache-Control": SEARCH_CACHE_CONTROL_VALUE,
    "CDN-Cache-Control": SEARCH_CACHE_CONTROL_VALUE,
    "Vercel-CDN-Cache-Control": SEARCH_CACHE_CONTROL_VALUE,
  };
}

async function getCachedSearchItems(query: string, kind: SubjectKind): Promise<SearchItems> {
  const memory = getSearchMemoryStore();
  const key = toSearchCacheKey(kind, query);
  const now = Date.now();

  const cached = memory.resultCache.get(key);
  if (cached && cached.expiresAt > now) {
    return cached.items;
  }

  if (cached) {
    memory.resultCache.delete(key);
  }

  const pending = memory.inflight.get(key);
  if (pending) {
    return pending;
  }

  const requestPromise = (kind === "song")
    ? searchItunesSong({ query, kind })
    : searchItunesAlbum({ query, kind });

  memory.inflight.set(key, requestPromise);

  try {
    const items = await requestPromise;
    memory.resultCache.set(key, {
      expiresAt: now + SEARCH_MEMORY_TTL_MS,
      items,
    });
    trimSearchMemoryCache(memory.resultCache);
    return items;
  } finally {
    if (memory.inflight.get(key) === requestPromise) {
      memory.inflight.delete(key);
    }
  }
}

export async function handleItunesSearchRequest(
  request: Request,
  options?: {
    forcedKind?: SubjectKind;
  }
) {
  const { searchParams } = new URL(request.url);
  const query = normalizeSearchQuery(searchParams.get("q"));
  const requestedKind = parseSubjectKind(searchParams.get("kind"));
  const kind = options?.forcedKind ?? requestedKind ?? DEFAULT_SUBJECT_KIND;

  if (!query) {
    return NextResponse.json(buildItunesSearchResponse({ query: "", kind, items: [] }), {
      headers: createSearchCacheHeaders(),
    });
  }

  const rateLimit = checkSearchRateLimit(request, kind);
  if (rateLimit.limited) {
    const payload = buildItunesSearchResponse({ query, kind, items: [] });
    return NextResponse.json(
      {
        ...payload,
        ok: false,
        error: "请求过于频繁，请稍后再试",
      },
      {
        status: 429,
        headers: {
          ...NO_STORE_HEADERS,
          "Retry-After": String(rateLimit.retryAfterSeconds),
          "X-RateLimit-Limit": String(SEARCH_RATE_LIMIT_MAX_REQUESTS),
          "X-RateLimit-Window": String(Math.ceil(SEARCH_RATE_LIMIT_WINDOW_MS / 1000)),
        },
      }
    );
  }

  try {
    const items = await getCachedSearchItems(query, kind);
    return NextResponse.json(buildItunesSearchResponse({ query, kind, items }), {
      headers: createSearchCacheHeaders(),
    });
  } catch (error) {
    const payload = buildItunesSearchResponse({ query, kind, items: [] });
    return NextResponse.json(
      {
        ...payload,
        ok: false,
        error: error instanceof Error ? error.message : "搜索失败",
      },
      {
        status: 500,
        headers: NO_STORE_HEADERS,
      }
    );
  }
}
