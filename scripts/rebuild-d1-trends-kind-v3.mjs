#!/usr/bin/env node

import {
  loadLocalEnvFiles,
  parsePositiveIntFlag,
  queryD1,
  resolveTargetEnv,
} from "./migration-utils.mjs";

const TREND_ALL_TABLE = "my9_trend_subject_kind_all_v3";
const TREND_DAY_TABLE = "my9_trend_subject_kind_day_v3";
const TREND_HOUR_TABLE = "my9_trend_subject_kind_hour_v3";
const TRENDS_CACHE_TABLE = "my9_trends_cache_v1";
const SLOT_TABLE = "my9_share_subject_slot_v1";

function sqlLiteral(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function executeStorageSql(targetEnv, label, sql) {
  console.log(`[rebuild-d1-trends] ${label}`);
  await queryD1WithRetry({
    targetEnv,
    sql,
  });
}

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

const SUMMARY_SQL = `
SELECT 'all' AS table_name, COUNT(*) AS row_count FROM my9_trend_subject_kind_all_v3
UNION ALL
SELECT 'day' AS table_name, COUNT(*) AS row_count FROM my9_trend_subject_kind_day_v3
UNION ALL
SELECT 'hour' AS table_name, COUNT(*) AS row_count FROM my9_trend_subject_kind_hour_v3
UNION ALL
SELECT 'cache' AS table_name, COUNT(*) AS row_count FROM my9_trends_cache_v1;
`.trim();

async function main() {
  loadLocalEnvFiles();
  const targetEnv = resolveTargetEnv();
  const nowMs = parsePositiveIntFlag("now-ms", Date.now());

  console.log(`[rebuild-d1-trends] target=${targetEnv} nowMs=${nowMs}`);
  await executeStorageSql(
    targetEnv,
    "clear cache",
    `DELETE FROM ${TRENDS_CACHE_TABLE};`
  );
  await executeStorageSql(
    targetEnv,
    "clear aggregate table",
    `DELETE FROM ${TREND_ALL_TABLE};`
  );
  await executeStorageSql(
    targetEnv,
    "clear day table",
    `DELETE FROM ${TREND_DAY_TABLE};`
  );
  await executeStorageSql(
    targetEnv,
    "clear hour table",
    `DELETE FROM ${TREND_HOUR_TABLE};`
  );

  const kindRows = await queryD1WithRetry({
    targetEnv,
    sql: `SELECT DISTINCT kind FROM ${SLOT_TABLE} ORDER BY kind`,
  });
  const kinds = kindRows?.[0]?.results?.map((row) => String(row.kind)).filter(Boolean) ?? [];

  for (const kind of kinds) {
    const kindLiteral = sqlLiteral(kind);

    await executeStorageSql(
      targetEnv,
      `rebuild all kind=${kind}`,
      `
INSERT INTO ${TREND_ALL_TABLE} (kind, subject_id, count, updated_at)
SELECT kind, subject_id, COUNT(*) AS count, ${nowMs} AS updated_at
FROM ${SLOT_TABLE}
WHERE kind = ${kindLiteral}
GROUP BY kind, subject_id
ON CONFLICT (kind, subject_id) DO UPDATE SET
  count = excluded.count,
  updated_at = excluded.updated_at;
      `.trim()
    );

    await executeStorageSql(
      targetEnv,
      `rebuild day kind=${kind}`,
      `
INSERT INTO ${TREND_DAY_TABLE} (kind, day_key, subject_id, count, updated_at)
SELECT kind, day_key, subject_id, COUNT(*) AS count, ${nowMs} AS updated_at
FROM ${SLOT_TABLE}
WHERE kind = ${kindLiteral}
GROUP BY kind, day_key, subject_id
ON CONFLICT (kind, day_key, subject_id) DO UPDATE SET
  count = excluded.count,
  updated_at = excluded.updated_at;
      `.trim()
    );

    await executeStorageSql(
      targetEnv,
      `rebuild hour kind=${kind}`,
      `
INSERT INTO ${TREND_HOUR_TABLE} (kind, hour_bucket, subject_id, count, updated_at)
SELECT kind, hour_bucket, subject_id, COUNT(*) AS count, ${nowMs} AS updated_at
FROM ${SLOT_TABLE}
WHERE kind = ${kindLiteral}
GROUP BY kind, hour_bucket, subject_id
ON CONFLICT (kind, hour_bucket, subject_id) DO UPDATE SET
  count = excluded.count,
  updated_at = excluded.updated_at;
      `.trim()
    );
  }

  const summary = await queryD1WithRetry({
    targetEnv,
    sql: SUMMARY_SQL,
  });

  console.log(`[rebuild-d1-trends] summary=${JSON.stringify(summary)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
