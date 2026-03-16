export type CustomSearchSource = "bangumi" | "tmdb" | "apple";
export type CustomEntrySource = CustomSearchSource | "upload";
export type CustomCoverMode = "remote" | "inline";

export interface CustomSearchItem {
  id: string;
  name: string;
  localizedName?: string;
  cover: string | null;
  coverMode: CustomCoverMode;
  source: CustomSearchSource;
  sourceLabel: string;
  externalUrl?: string;
  releaseYear?: number;
}

export interface CustomSearchResponse {
  ok: boolean;
  source: CustomSearchSource;
  items: CustomSearchItem[];
  noResultQuery: string | null;
}

export interface CustomEntry {
  id: string;
  title: string;
  fallbackName?: string;
  cover: string | null;
  coverMode: CustomCoverMode;
  source: CustomEntrySource;
  sourceLabel: string;
  externalUrl?: string;
  releaseYear?: number;
  comment?: string;
  spoiler?: boolean;
}

export interface CustomPendingSelection {
  id: string;
  fallbackName?: string;
  cover: string | null;
  coverMode: CustomCoverMode;
  source: CustomEntrySource;
  sourceLabel: string;
  externalUrl?: string;
  releaseYear?: number;
}

export type CustomDraftSnapshot = {
  entries: Array<CustomEntry | null>;
};

export function createEmptyCustomEntries() {
  return Array.from({ length: 9 }, () => null as CustomEntry | null);
}

export function cloneCustomEntries(entries: Array<CustomEntry | null>) {
  return entries.map((entry) => (entry ? { ...entry } : null));
}

export function normalizeCustomEntries(entries?: Array<CustomEntry | null>) {
  if (!Array.isArray(entries) || entries.length !== 9) {
    return createEmptyCustomEntries();
  }
  return cloneCustomEntries(entries);
}

export function getCustomEntryDisplayTitle(entry: CustomEntry | null | undefined): string {
  const title = entry?.title?.trim();
  if (title) return title;
  return "未命名条目";
}

export function getCustomEntryExportTitle(entry: CustomEntry | null | undefined): string {
  return entry?.title?.trim() || "";
}

export function getCustomEntrySubtitle(entry: CustomEntry | null | undefined): string | null {
  const fallbackName = entry?.fallbackName?.trim();
  const title = entry?.title?.trim();
  if (!fallbackName) return null;
  if (!title) return fallbackName;
  if (fallbackName === title) return null;
  return fallbackName;
}

export function getCustomSearchItemDefaultTitle(item: CustomSearchItem): string {
  return item.localizedName?.trim() || item.name.trim();
}

export function getDefaultCustomExportTitle(creatorName?: string | null): string {
  const name = creatorName?.trim() || "我";
  return `构成${name}的9部作品`;
}
