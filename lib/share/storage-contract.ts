import type { SubjectKind } from "@/lib/subject-kind";
import type { StoredShareV1, TrendPeriod, TrendResponse, TrendView, TrendYearPage } from "@/lib/share/types";

export type ShareSaveResult = {
  shareId: string;
  deduped: boolean;
};

export type TrendSampleSummary = {
  sampleCount: number;
  range: {
    from: number | null;
    to: number | null;
  };
};

export interface StorageBackend {
  readonly name: "d1";
  saveShare(record: StoredShareV1): Promise<ShareSaveResult>;
  getShare(shareId: string): Promise<StoredShareV1 | null>;
  touchShare(shareId: string, now?: number): Promise<boolean>;
  listAllShares(): Promise<StoredShareV1[]>;
  countAllShares(): Promise<number>;
  listSharesByPeriod(period: TrendPeriod): Promise<StoredShareV1[]>;
  getAggregatedTrendResponse(params: {
    period: TrendPeriod;
    view: TrendView;
    kind: SubjectKind;
    overallPage: number;
    yearPage: TrendYearPage;
  }): Promise<TrendResponse | null>;
  getTrendSampleSummary(period: TrendPeriod, kind: SubjectKind): Promise<TrendSampleSummary | null>;
  getTrendSampleSummaryCache(
    period: TrendPeriod,
    kind: SubjectKind,
    options?: { allowExpired?: boolean }
  ): Promise<TrendSampleSummary | null>;
  setTrendSampleSummaryCache(
    period: TrendPeriod,
    kind: SubjectKind,
    value: TrendSampleSummary,
    ttlSeconds?: number
  ): Promise<void>;
  getTrendsCache(
    period: TrendPeriod,
    view: TrendView,
    kind: SubjectKind,
    overallPage: number,
    yearPage: TrendYearPage,
    options?: { allowExpired?: boolean }
  ): Promise<TrendResponse | null>;
  setTrendsCache(
    period: TrendPeriod,
    view: TrendView,
    kind: SubjectKind,
    overallPage: number,
    yearPage: TrendYearPage,
    value: TrendResponse,
    ttlSeconds?: number
  ): Promise<void>;
  upsertShareViewTotalCounts(
    rows: Array<{
      shareId: string;
      kind: SubjectKind;
      viewCount: number;
    }>,
    options?: { lastAggregatedAt?: number }
  ): Promise<number>;
  cleanupOldTrendCounts(params?: {
    cleanupTrendDays?: number;
  }): Promise<{
    cleanupTrendDays: number;
    cleanedTrendRows: number;
    cleanedDayRows: number;
    cleanedHourRows: number;
  }>;
}
