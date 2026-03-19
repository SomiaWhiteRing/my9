import { runTrendRollup } from "@/lib/share/storage";

export async function runHourlyTrendMaintenance(options?: {
  logLabel?: string;
  nowMs?: number;
}) {
  const result = await runTrendRollup({
    nowMs: options?.nowMs,
  });

  if (options?.logLabel) {
    console.log(`${options.logLabel} ${JSON.stringify(result)}`);
  }

  return result;
}
