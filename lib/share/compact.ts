import { createHash } from "node:crypto";
import { ShareSubject } from "@/lib/share/types";

export type CompactShareSlot = {
  sid: string;
  c?: string;
  s?: 1;
  u?: Record<string, string>;
};

export type CompactSharePayload = Array<CompactShareSlot | null>;

export type SubjectSnapshot = {
  subjectId: string;
  name: string;
  localizedName?: string;
  cover: string | null;
  releaseYear?: number;
  genres?: string[];
};

function sanitizeText(value: unknown): string {
  if (typeof value !== "string") return "";
  return value.trim();
}

function normalizeGenres(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const cleaned = value
    .map((item) => sanitizeText(item))
    .filter((item) => Boolean(item))
    .slice(0, 5);
  if (cleaned.length === 0) return undefined;
  return Array.from(new Set(cleaned));
}

function normalizeCover(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed || null;
}

function normalizeReleaseYear(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  return undefined;
}

function sanitizeHttpUrl(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;

  try {
    const parsed = new URL(trimmed);
    const protocol = parsed.protocol.toLowerCase();
    if (protocol !== "http:" && protocol !== "https:") return null;
    return parsed.toString();
  } catch {
    return null;
  }
}

function normalizeStoreUrls(value: unknown): Record<string, string> | undefined {
  if (!value || typeof value !== "object") return undefined;
  const normalized = Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .map(([key, raw]) => [sanitizeText(key), sanitizeHttpUrl(raw)] as const)
      .filter((entry): entry is [string, string] => Boolean(entry[0]) && Boolean(entry[1]))
  );
  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

function toNumericIdIfSafe(subjectId: string): string | number {
  if (!/^\d+$/.test(subjectId)) {
    return subjectId;
  }
  const parsed = Number(subjectId);
  if (!Number.isSafeInteger(parsed)) {
    return subjectId;
  }
  return parsed;
}

export function normalizeSubjectId(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(Math.trunc(value));
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed) return trimmed;
  }
  return null;
}

function mergeSubjectSnapshot(existing: SubjectSnapshot, next: SubjectSnapshot): SubjectSnapshot {
  return {
    ...existing,
    name: existing.name || next.name,
    localizedName: existing.localizedName || next.localizedName,
    cover: existing.cover || next.cover,
    releaseYear: existing.releaseYear ?? next.releaseYear,
    genres: existing.genres && existing.genres.length > 0 ? existing.genres : next.genres,
  };
}

export function toCompactSharePayload(games: Array<ShareSubject | null>): {
  payload: CompactSharePayload;
  subjectSnapshots: Map<string, SubjectSnapshot>;
} {
  const payload: CompactSharePayload = Array.from({ length: 9 }, () => null);
  const subjectSnapshots = new Map<string, SubjectSnapshot>();

  for (let index = 0; index < 9; index += 1) {
    const item = games[index];
    if (!item || typeof item !== "object") {
      payload[index] = null;
      continue;
    }

    const name = sanitizeText(item.name) || "untitled";
    const subjectId = normalizeSubjectId(item.id);
    if (!subjectId) {
      payload[index] = null;
      continue;
    }
    const comment = sanitizeText(item.comment);
    const spoiler = Boolean(item.spoiler);
    const storeUrls = normalizeStoreUrls(item.storeUrls);

    const slot: CompactShareSlot = { sid: subjectId };
    if (comment) {
      slot.c = comment;
    }
    if (spoiler) {
      slot.s = 1;
    }
    if (storeUrls) {
      slot.u = storeUrls;
    }
    payload[index] = slot;

    const localizedNameRaw = sanitizeText(item.localizedName);
    const snapshot: SubjectSnapshot = {
      subjectId,
      name,
      localizedName: localizedNameRaw && localizedNameRaw !== name ? localizedNameRaw : undefined,
      cover: normalizeCover(item.cover),
      releaseYear: normalizeReleaseYear(item.releaseYear),
      genres: normalizeGenres(item.genres),
    };

    const existing = subjectSnapshots.get(subjectId);
    subjectSnapshots.set(subjectId, existing ? mergeSubjectSnapshot(existing, snapshot) : snapshot);
  }

  return {
    payload,
    subjectSnapshots,
  };
}

export function createContentHash(params: {
  kind: string;
  creatorName: string | null;
  payload: CompactSharePayload;
}): string {
  const normalizedSlots = params.payload.map((slot) => {
    if (!slot) return null;
    return {
      sid: slot.sid,
      c: slot.c || "",
      s: Boolean(slot.s),
    };
  });

  const canonical = JSON.stringify({
    kind: params.kind,
    creatorName: params.creatorName || "",
    slots: normalizedSlots,
  });

  return createHash("sha256").update(canonical).digest("hex");
}

export function compactPayloadToGames(params: {
  payload: CompactSharePayload;
  subjectSnapshots: Map<string, SubjectSnapshot>;
}): Array<ShareSubject | null> {
  return params.payload.map((slot) => {
    if (!slot) return null;

    const snapshot = params.subjectSnapshots.get(slot.sid);
    const name = snapshot?.name || slot.sid;
    const storeUrls = normalizeStoreUrls(slot.u);

    const game: ShareSubject = {
      id: toNumericIdIfSafe(slot.sid),
      name,
      localizedName: snapshot?.localizedName,
      cover: snapshot?.cover ?? null,
      releaseYear: snapshot?.releaseYear,
      genres: snapshot?.genres,
      storeUrls,
      comment: slot.c,
      spoiler: Boolean(slot.s),
    };

    return game;
  });
}

export function normalizeCompactPayload(value: unknown): CompactSharePayload | null {
  if (!Array.isArray(value) || value.length !== 9) {
    return null;
  }

  const payload: CompactSharePayload = Array.from({ length: 9 }, () => null);
  for (let index = 0; index < 9; index += 1) {
    const slot = value[index];
    if (!slot || typeof slot !== "object") {
      payload[index] = null;
      continue;
    }

    const sid = sanitizeText((slot as Record<string, unknown>).sid);
    if (!sid) {
      payload[index] = null;
      continue;
    }

    const comment = sanitizeText((slot as Record<string, unknown>).c);
    const spoiler = Boolean((slot as Record<string, unknown>).s);
    const storeUrls = normalizeStoreUrls((slot as Record<string, unknown>).u);

    payload[index] = {
      sid,
      c: comment || undefined,
      s: spoiler ? 1 : undefined,
      u: storeUrls,
    };
  }

  return payload;
}
