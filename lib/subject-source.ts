import type { SubjectKind } from "@/lib/subject-kind";

type SubjectLike = {
  id?: string | number | null;
  name?: string | null;
  localizedName?: string | null;
  storeUrls?: Record<string, string> | null;
};

type WorkSubjectNamespace =
  | { source: "tmdb"; entity: "movie" | "tv"; id: string }
  | { source: "itunes"; entity: "song" | "album"; id: string };

export type SubjectSource = "bangumi" | "tmdb" | "itunes";

export type SubjectLinkResolution = {
  source: SubjectSource;
  sourceLabel: string;
  url: string;
};

function displayName(subject: SubjectLike): string {
  const localized = typeof subject.localizedName === "string" ? subject.localizedName.trim() : "";
  if (localized) return localized;
  const name = typeof subject.name === "string" ? subject.name.trim() : "";
  if (name) return name;
  return "未命名";
}

function normalizeSubjectId(value: string | number | null | undefined): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(Math.trunc(value));
  }
  if (typeof value === "string") {
    return value.trim();
  }
  return "";
}

function parseWorkNamespacedId(value: string | number | null | undefined): WorkSubjectNamespace | null {
  const normalized = normalizeSubjectId(value);
  if (!normalized) return null;

  const tmdbMatch = normalized.match(/^tmdb:(movie|tv):(.+)$/i);
  if (tmdbMatch) {
    const entity = tmdbMatch[1].toLowerCase();
    const id = tmdbMatch[2].trim();
    if ((entity === "movie" || entity === "tv") && id) {
      return {
        source: "tmdb",
        entity,
        id,
      };
    }
  }

  const itunesMatch = normalized.match(/^itunes:(song|album):(.+)$/i);
  if (itunesMatch) {
    const entity = itunesMatch[1].toLowerCase();
    const id = itunesMatch[2].trim();
    if ((entity === "song" || entity === "album") && id) {
      return {
        source: "itunes",
        entity,
        id,
      };
    }
  }

  return null;
}

function sanitizeHttpUrl(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  try {
    const parsed = new URL(trimmed);
    const protocol = parsed.protocol.toLowerCase();
    if (protocol !== "http:" && protocol !== "https:") {
      return null;
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

function toBangumiSubjectLink(subjectId: string, fallbackName: string, cat?: number): string {
  if (/^\d+$/.test(subjectId)) {
    return `https://bgm.tv/subject/${subjectId}`;
  }
  const query = encodeURIComponent(fallbackName);
  if (typeof cat === "number") {
    return `https://bgm.tv/subject_search/${query}?cat=${cat}`;
  }
  return `https://bgm.tv/subject_search/${query}`;
}

function toBangumiCharacterLink(subjectId: string): string {
  return `https://bgm.tv/character/${subjectId}`;
}

function toBangumiPersonLink(subjectId: string): string {
  return `https://bgm.tv/person/${subjectId}`;
}

function toTmdbLink(entity: "movie" | "tv", subjectId: string, fallbackName: string): string {
  if (/^\d+$/.test(subjectId)) {
    return `https://www.themoviedb.org/${entity}/${subjectId}`;
  }
  const query = encodeURIComponent(fallbackName);
  return `https://www.themoviedb.org/search/${entity}?query=${query}`;
}

function toAppleMusicLink(subject: SubjectLike): string {
  const storeUrl = sanitizeHttpUrl(subject.storeUrls?.apple);
  if (storeUrl) return storeUrl;
  const query = encodeURIComponent(displayName(subject));
  return `https://music.apple.com/cn/search?term=${query}`;
}

function bangumiResolution(subject: SubjectLike, bangumiSearchCat?: number): SubjectLinkResolution {
  const id = normalizeSubjectId(subject.id);
  return {
    source: "bangumi",
    sourceLabel: "Bangumi",
    url: toBangumiSubjectLink(id, displayName(subject), bangumiSearchCat),
  };
}

function tmdbResolution(entity: "movie" | "tv", subject: SubjectLike): SubjectLinkResolution {
  const id = normalizeSubjectId(subject.id);
  return {
    source: "tmdb",
    sourceLabel: "TMDB",
    url: toTmdbLink(entity, id, displayName(subject)),
  };
}

function appleMusicResolution(subject: SubjectLike): SubjectLinkResolution {
  return {
    source: "itunes",
    sourceLabel: "Apple Music",
    url: toAppleMusicLink(subject),
  };
}

export function resolveSubjectLink(params: {
  kind?: SubjectKind;
  subject: SubjectLike;
  bangumiSearchCat?: number;
}): SubjectLinkResolution {
  const { kind, subject, bangumiSearchCat } = params;
  const subjectId = normalizeSubjectId(subject.id);

  if (kind === "song" || kind === "album") {
    return appleMusicResolution(subject);
  }
  if (kind === "tv") {
    return tmdbResolution("tv", subject);
  }
  if (kind === "movie") {
    return tmdbResolution("movie", subject);
  }
  if (kind === "character") {
    return {
      source: "bangumi",
      sourceLabel: "Bangumi",
      url: toBangumiCharacterLink(subjectId),
    };
  }
  if (kind === "person") {
    return {
      source: "bangumi",
      sourceLabel: "Bangumi",
      url: toBangumiPersonLink(subjectId),
    };
  }

  if (kind === "work") {
    const namespaced = parseWorkNamespacedId(subjectId);
    if (namespaced?.source === "tmdb") {
      return {
        source: "tmdb",
        sourceLabel: "TMDB",
        url: toTmdbLink(namespaced.entity, namespaced.id, displayName(subject)),
      };
    }
    if (namespaced?.source === "itunes") {
      return appleMusicResolution(subject);
    }
    if (sanitizeHttpUrl(subject.storeUrls?.apple)) {
      return appleMusicResolution(subject);
    }
  }

  return bangumiResolution(subject, bangumiSearchCat);
}
