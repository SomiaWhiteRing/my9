#!/usr/bin/env node

import {
  loadLocalEnvFiles,
  parsePositiveIntFlag,
  queryD1,
  resolveTargetEnv,
} from "./migration-utils.mjs";

const TREND_DAY_TABLE = "my9_trend_subject_kind_day_v3";
const TREND_HOUR_TABLE = "my9_trend_subject_kind_hour_v3";
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;

function getBeijingDayStart(timestampMs) {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / DAY_MS) * DAY_MS - BEIJING_TZ_OFFSET_MS;
}

function toBeijingDayKey(timestampMs) {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

function toBeijingHourBucket(timestampMs) {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS);
}

const SUMMARY_SQL = `
SELECT 'day' AS table_name, COUNT(*) AS row_count FROM my9_trend_subject_kind_day_v3
UNION ALL
SELECT 'hour' AS table_name, COUNT(*) AS row_count FROM my9_trend_subject_kind_hour_v3;
`.trim();

async function queryD1WithRetry({ targetEnv, sql, attempts = 3, delayMs = 2000 }) {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await queryD1({ targetEnv, sql });
    } catch (error) {
      lastError = error;
      if (attempt >= attempts) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError ?? "queryD1 failed"));
}

async function main() {
  loadLocalEnvFiles();
  const targetEnv = resolveTargetEnv();
  const cleanupTrendDays = Math.max(180, parsePositiveIntFlag("cleanup-trend-days", 190));
  const nowMs = parsePositiveIntFlag("now-ms", Date.now());

  const cleanupBeforeDayKey = toBeijingDayKey(nowMs - cleanupTrendDays * DAY_MS);
  const cleanupBeforeHourBucket = toBeijingHourBucket(getBeijingDayStart(nowMs) - DAY_MS);

  console.log(
    `[cleanup-d1-trends] target=${targetEnv} cleanupTrendDays=${cleanupTrendDays} beforeDayKey=${cleanupBeforeDayKey} beforeHourBucket=${cleanupBeforeHourBucket}`
  );

  await queryD1WithRetry({
    targetEnv,
    sql: `
DELETE FROM ${TREND_DAY_TABLE} WHERE day_key < ${cleanupBeforeDayKey};
DELETE FROM ${TREND_HOUR_TABLE} WHERE hour_bucket < ${cleanupBeforeHourBucket};
    `.trim(),
  });

  const summary = await queryD1WithRetry({
    targetEnv,
    sql: SUMMARY_SQL,
  });
  console.log(`[cleanup-d1-trends] summary=${JSON.stringify(summary)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
