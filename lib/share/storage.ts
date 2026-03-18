import type { TrendPeriod, TrendResponse, TrendView, TrendYearPage, StoredShareV1 } from "@/lib/share/types";
import type { SubjectKind } from "@/lib/subject-kind";
import d1StorageBackend from "@/lib/share/storage-d1";

export function saveShare(record: StoredShareV1) {
  return d1StorageBackend.saveShare(record);
}

export function getShare(shareId: string) {
  return d1StorageBackend.getShare(shareId);
}

export function touchShare(shareId: string, now = Date.now()) {
  return d1StorageBackend.touchShare(shareId, now);
}

export function listAllShares() {
  return d1StorageBackend.listAllShares();
}

export function countAllShares() {
  return d1StorageBackend.countAllShares();
}

export function listSharesByPeriod(period: TrendPeriod) {
  return d1StorageBackend.listSharesByPeriod(period);
}

export function getAggregatedTrendResponse(params: {
  period: TrendPeriod;
  view: TrendView;
  kind: SubjectKind;
  overallPage: number;
  yearPage: TrendYearPage;
}): Promise<TrendResponse | null> {
  return d1StorageBackend.getAggregatedTrendResponse(params);
}

export function getTrendSampleSummary(period: TrendPeriod, kind: SubjectKind) {
  return d1StorageBackend.getTrendSampleSummary(period, kind);
}

export function getTrendSampleSummaryCache(
  period: TrendPeriod,
  kind: SubjectKind,
  options?: { allowExpired?: boolean }
) {
  return d1StorageBackend.getTrendSampleSummaryCache(period, kind, options);
}

export function setTrendSampleSummaryCache(
  period: TrendPeriod,
  kind: SubjectKind,
  value: { sampleCount: number; range: { from: number | null; to: number | null } },
  ttlSeconds = 3600
) {
  return d1StorageBackend.setTrendSampleSummaryCache(period, kind, value, ttlSeconds);
}

export function getTrendsCache(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  overallPage: number,
  yearPage: TrendYearPage,
  options?: { allowExpired?: boolean }
) {
  return d1StorageBackend.getTrendsCache(period, view, kind, overallPage, yearPage, options);
}

export function setTrendsCache(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  overallPage: number,
  yearPage: TrendYearPage,
  value: TrendResponse,
  ttlSeconds = 3600
) {
  return d1StorageBackend.setTrendsCache(period, view, kind, overallPage, yearPage, value, ttlSeconds);
}

export function upsertShareViewTotalCounts(
  rows: Array<{
    shareId: string;
    kind: SubjectKind;
    viewCount: number;
  }>,
  options?: { lastAggregatedAt?: number }
) {
  return d1StorageBackend.upsertShareViewTotalCounts(rows, options);
}

export function cleanupOldTrendCounts(params?: { cleanupTrendDays?: number }) {
  return d1StorageBackend.cleanupOldTrendCounts(params);
}
