import { cleanupOldTrendCounts } from "@/lib/share/storage";
import { runShareViewRollup, type ShareViewRollupResult } from "@/lib/share/view-stats";

export type DailyShareMaintenanceResult = {
  trendCleanup?: Awaited<ReturnType<typeof cleanupOldTrendCounts>>;
  shareViews?: ShareViewRollupResult;
};

type WorkerEnvLike = Record<string, unknown> | undefined;

export async function runDailyShareMaintenance(options?: {
  env?: WorkerEnvLike;
  logLabel?: string;
}) {
  const result: DailyShareMaintenanceResult = {};
  const failures: string[] = [];

  try {
    result.trendCleanup = await cleanupOldTrendCounts();
  } catch (error) {
    failures.push(`trendCleanup=${error instanceof Error ? error.message : String(error)}`);
  }

  try {
    result.shareViews = await runShareViewRollup({
      env: options?.env,
      logLabel: options?.logLabel ? `${options.logLabel}:share-views` : undefined,
    });
  } catch (error) {
    failures.push(`shareViews=${error instanceof Error ? error.message : String(error)}`);
  }

  if (options?.logLabel) {
    console.log(`${options.logLabel} ${JSON.stringify(result)}`);
  }

  if (failures.length > 0) {
    throw new Error(failures.join("; "));
  }

  return result;
}
