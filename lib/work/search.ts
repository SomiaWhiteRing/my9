import { searchBangumiSubjects } from "@/lib/bangumi/search";
import { searchTmdbMovie, searchTmdbTv } from "@/lib/tmdb/search";
import { searchItunesAlbum, searchItunesSong } from "@/lib/itunes/search";
import { ShareSubject, SubjectSearchResponse } from "@/lib/share/types";

const WORK_SEARCH_LIMIT = 20;
// Keep a very long timeout so each upstream source has enough time to respond.
const SOURCE_TIMEOUT_MS = 5 * 60 * 1000;

type WorkSourceKey = "bangumi" | "tmdb:movie" | "tmdb:tv" | "itunes:song" | "itunes:album";

type ScoredItem = {
  item: ShareSubject;
  score: number;
  order: number;
  source: WorkSourceKey;
};

type PickedEntry = {
  id: string;
  sameYearNameKey: string | null;
  source: WorkSourceKey;
  item: ShareSubject;
};

function normalizeText(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, "");
}

function normalizeSubjectId(value: string | number): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(Math.trunc(value));
  }
  return String(value).trim();
}

function normalizeBangumiSubjectType(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  const normalized = Math.trunc(value);
  return normalized > 0 ? normalized : null;
}

function sourcePriority(source: WorkSourceKey): number {
  if (source === "bangumi") return 0;
  if (source.startsWith("tmdb:")) return 1;
  return 2;
}

function compareSourcePriority(a: WorkSourceKey, b: WorkSourceKey): number {
  return sourcePriority(a) - sourcePriority(b);
}

function toNamespacedId(namespace: "tmdb" | "itunes", entity: "movie" | "tv" | "song" | "album", id: string | number) {
  const normalized = normalizeSubjectId(id);
  return `${namespace}:${entity}:${normalized}`;
}

function withNamespacedId(
  items: ShareSubject[],
  namespace: "tmdb" | "itunes",
  entity: "movie" | "tv" | "song" | "album"
): ShareSubject[] {
  return items.map((item) => ({
    ...item,
    id: toNamespacedId(namespace, entity, item.id),
  }));
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

function toSameYearNameKey(candidate: ScoredItem): string | null {
  const releaseYear =
    typeof candidate.item.releaseYear === "number"
      ? Math.trunc(candidate.item.releaseYear)
      : 0;
  if (!releaseYear) return null;

  const name = normalizeText((candidate.item.localizedName || candidate.item.name || "").trim());
  if (!name) return null;

  if (candidate.source === "bangumi") {
    // Bangumi work search mixes multiple media types; include subjectType to avoid cross-media dedupe.
    const subjectType = normalizeBangumiSubjectType(candidate.item.subjectType);
    if (!subjectType) {
      return null;
    }
    return `bangumi:${subjectType}:${releaseYear}:${name}`;
  }

  return `${candidate.source}:${releaseYear}:${name}`;
}

function rankChannelItems(query: string, source: WorkSourceKey, items: ShareSubject[]): ScoredItem[] {
  return items
    .map((item, order) => ({
      item,
      source,
      order,
      score: scoreCandidate(query, item),
    }))
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return a.order - b.order;
    });
}

function mergeWorkItems(query: string, channels: Array<{ source: WorkSourceKey; items: ShareSubject[] }>): ShareSubject[] {
  const rankedByChannel = channels.map((channel) => rankChannelItems(query, channel.source, channel.items));
  const cursors = rankedByChannel.map(() => 0);
  const pickedIds = new Set<string>();
  const sameYearNameIndexMap = new Map<string, number>();
  const picked: PickedEntry[] = [];

  function tryPickCandidate(candidate: ScoredItem): boolean {
    const id = normalizeSubjectId(candidate.item.id);
    if (!id || pickedIds.has(id)) {
      return false;
    }

    const sameYearNameKey = toSameYearNameKey(candidate);
    if (sameYearNameKey) {
      const existingIndex = sameYearNameIndexMap.get(sameYearNameKey);
      if (typeof existingIndex === "number") {
        const existing = picked[existingIndex];
        if (compareSourcePriority(candidate.source, existing.source) < 0) {
          pickedIds.delete(existing.id);
          picked[existingIndex] = {
            id,
            sameYearNameKey,
            source: candidate.source,
            item: candidate.item,
          };
          pickedIds.add(id);
          return true;
        }
        return false;
      }
    }

    const nextIndex = picked.length;
    picked.push({
      id,
      sameYearNameKey,
      source: candidate.source,
      item: candidate.item,
    });
    pickedIds.add(id);
    if (sameYearNameKey) {
      sameYearNameIndexMap.set(sameYearNameKey, nextIndex);
    }
    return true;
  }

  let progressed = true;
  while (picked.length < WORK_SEARCH_LIMIT && progressed) {
    progressed = false;

    for (let channelIndex = 0; channelIndex < rankedByChannel.length; channelIndex += 1) {
      const ranked = rankedByChannel[channelIndex];
      while (cursors[channelIndex] < ranked.length) {
        const candidate = ranked[cursors[channelIndex]];
        cursors[channelIndex] += 1;
        if (tryPickCandidate(candidate)) {
          progressed = true;
          break;
        }
      }
      if (picked.length >= WORK_SEARCH_LIMIT) {
        break;
      }
    }
  }

  if (picked.length >= WORK_SEARCH_LIMIT) {
    return picked.map((entry) => entry.item);
  }

  const allRanked = rankedByChannel
    .flat()
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (a.source !== b.source) return compareSourcePriority(a.source, b.source);
      return a.order - b.order;
    });

  for (const candidate of allRanked) {
    if (picked.length >= WORK_SEARCH_LIMIT) {
      break;
    }
    tryPickCandidate(candidate);
  }

  return picked.map((entry) => entry.item);
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  try {
    return await new Promise<T>((resolve, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error(`${label} timeout`));
      }, timeoutMs);
      promise.then(resolve).catch(reject);
    });
  } finally {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
  }
}

export function buildWorkSearchResponse(params: {
  query: string;
  items: ShareSubject[];
}): SubjectSearchResponse {
  const { query, items } = params;
  return {
    ok: true,
    source: "mixed",
    kind: "work",
    items,
    noResultQuery: items.length === 0 && query.trim() ? query : null,
  };
}

export async function searchWorkSubjects(params: {
  query: string;
}): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) {
    return [];
  }

  const sources: Array<{ source: WorkSourceKey; task: Promise<ShareSubject[]> }> = [
    {
      source: "bangumi",
      task: withTimeout(searchBangumiSubjects({ query: q, kind: "work" }), SOURCE_TIMEOUT_MS, "bangumi"),
    },
    {
      source: "tmdb:movie",
      task: withTimeout(
        searchTmdbMovie({ query: q, kind: "movie" }).then((items) => withNamespacedId(items, "tmdb", "movie")),
        SOURCE_TIMEOUT_MS,
        "tmdb:movie"
      ),
    },
    {
      source: "tmdb:tv",
      task: withTimeout(
        searchTmdbTv({ query: q, kind: "tv" }).then((items) => withNamespacedId(items, "tmdb", "tv")),
        SOURCE_TIMEOUT_MS,
        "tmdb:tv"
      ),
    },
    {
      source: "itunes:song",
      task: withTimeout(
        searchItunesSong({ query: q, kind: "song" }).then((items) => withNamespacedId(items, "itunes", "song")),
        SOURCE_TIMEOUT_MS,
        "itunes:song"
      ),
    },
    {
      source: "itunes:album",
      task: withTimeout(
        searchItunesAlbum({ query: q, kind: "album" }).then((items) => withNamespacedId(items, "itunes", "album")),
        SOURCE_TIMEOUT_MS,
        "itunes:album"
      ),
    },
  ];

  const allResults = await Promise.all(
    sources.map(async ({ source, task }) => ({
      source,
      items: await task,
    }))
  );

  return mergeWorkItems(q, allResults);
}
