import type { SubjectKind } from "@/lib/subject-kind";

export type GameTypeId = 0 | 1 | 2 | 3 | 4 | 8 | 9 | 10 | 11;

export interface ShareSubject {
  id: number | string;
  name: string;
  localizedName?: string;
  cover: string | null;
  releaseYear?: number;
  gameTypeId?: GameTypeId;
  platforms?: string[];
  genres?: string[];
  storeUrls?: Record<string, string>;
  comment?: string;
  spoiler?: boolean;
  subjectType?: number;
  subjectPlatform?: string | null;
}

export type ShareGame = ShareSubject;

export interface SubjectSearchResponse {
  ok: boolean;
  source: "bangumi" | "tmdb" | "itunes";
  kind: SubjectKind;
  items: ShareSubject[];
  topPickIds: Array<string | number>;
  suggestions: string[];
  noResultQuery: string | null;
}

export type GameSearchResponse = SubjectSearchResponse;

export interface StoredShareV1 {
  shareId: string;
  kind: SubjectKind;
  creatorName: string | null;
  games: Array<ShareSubject | null>;
  createdAt: number;
  updatedAt: number;
  lastViewedAt: number;
}

export type TrendPeriod = "today" | "24h" | "7d" | "30d" | "90d" | "180d" | "all";
export type TrendView = "overall" | "genre" | "decade" | "year";
export type TrendYearPage = "recent" | "legacy";

export interface TrendGameItem {
  id: string;
  name: string;
  localizedName?: string;
  cover: string | null;
  releaseYear?: number;
  count: number;
}

export interface TrendBucket {
  key: string;
  label: string;
  count: number;
  games: TrendGameItem[];
}

export interface TrendResponse {
  period: TrendPeriod;
  view: TrendView;
  sampleCount: number;
  range: {
    from: number | null;
    to: number | null;
  };
  lastUpdatedAt: number;
  items: TrendBucket[];
}
