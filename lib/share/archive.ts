import { archiveHotSharesToColdStorage } from "@/lib/share/storage";
import type { ColdStorageBucketLike } from "@/lib/share/cold-storage";

export type ShareArchiveConfig = {
  olderThanDays: number;
  batchSize: number;
  cleanupTrendDays: number;
};

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
}

export function getShareArchiveConfig(): ShareArchiveConfig {
  return {
    olderThanDays: parsePositiveInt(process.env.MY9_ARCHIVE_OLDER_THAN_DAYS, 30),
    batchSize: parsePositiveInt(process.env.MY9_ARCHIVE_BATCH_SIZE, 500),
    cleanupTrendDays: parsePositiveInt(process.env.MY9_ARCHIVE_CLEANUP_TREND_DAYS, 190),
  };
}

export async function runShareArchive(options?: {
  coldStorageBucket?: ColdStorageBucketLike | null;
  logLabel?: string;
}) {
  const config = getShareArchiveConfig();
  const result = await archiveHotSharesToColdStorage(config, {
    coldStorageBucket: options?.coldStorageBucket ?? null,
  });

  if (options?.logLabel) {
    console.log(`${options.logLabel} ${JSON.stringify({ config, result })}`);
  }

  return {
    config,
    result,
  };
}
