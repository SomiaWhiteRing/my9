import type { SubjectKind } from "@/lib/subject-kind";

export type ShareDiscoveryItem = {
  shareId: string;
  kind: SubjectKind;
  creatorName: string | null;
  createdAt: number;
  viewCount: number;
};

export type ShareDiscoveryPage = {
  items: ShareDiscoveryItem[];
  nextCursor: string | null;
  viewsThrough: string;
};

export type ShareDiscoveryLoader = (options: {
  query: string;
  cursor: string | null;
  signal: AbortSignal;
}) => Promise<ShareDiscoveryPage>;

// Adapted from CARTOColors Bold for white labels (CC BY 4.0).
// https://carto.com/carto-colors/
export const SHARE_KIND_COLORS: Record<SubjectKind, string> = {
  game: "#3969AC",
  anime: "#D13868",
  tv: "#0B8460",
  movie: "#7F3C8D",
  manga: "#B55851",
  lightnovel: "#936E00",
  song: "#00808E",
  album: "#567F3B",
  work: "#727569",
  character: "#CF1C90",
  person: "#AC6109",
};
