# Share Storage V2 Ops

## New env vars

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- Optional: `MY9_ENABLE_V1_FALLBACK=0` (default keeps `my9_shares_v1` read fallback)
- Optional: `MY9_ANALYTICS_ACCOUNT_ID` (runtime fallback for Analytics Engine SQL rollup; defaults to `CLOUDFLARE_ACCOUNT_ID` when synced)
- Optional: `MY9_ANALYTICS_API_TOKEN` (recommended: dedicated read token for Analytics Engine SQL rollup)
- Optional: `MY9_TREND_CLEANUP_DAYS` (default `190`, do not set below `180` if you still serve `180d` trends)
- Optional: `MY9_TRENDS_24H_SOURCE=day|hour` (default `day`, 24h data source switch on v3 day/hour tables)

## Migration

Run idempotent migration with checkpoint:

```bash
node scripts/migrate-shares-v1-to-v2.mjs
```

Useful flags:

- `node scripts/migrate-shares-v1-to-v2.mjs --batch-size=300`
- `node scripts/migrate-shares-v1-to-v2.mjs --max-rows=5000`

Checkpoint file: `scripts/.migrate-shares-v1.checkpoint.json`

Notes:

- This migration only materializes `my9_share_registry_v2`, `my9_share_alias_v1`, and `my9_subject_dim_v1`.
- Current online trend tables must be rebuilt separately with `node scripts/rebuild-trends-kind-v3.mjs`.

## Migration verify

Run migration consistency checks (`old`, `v2`, `alias`, `missing`):

```bash
node scripts/verify-shares-v2-migration.mjs
```

If you are removing the legacy cold-storage columns after deploying the hot-only runtime, verify first and then drop them:

```bash
node scripts/remove-cold-storage-columns.mjs --dry-run
node scripts/remove-cold-storage-columns.mjs
```

## Trend table rebuild (kind-grain v3)

Current online trend tables are:

- `my9_trend_subject_kind_all_v3`
- `my9_trend_subject_kind_day_v3`
- `my9_trend_subject_kind_hour_v3`

They store `kind + subject_id + count` to avoid cross-kind mixed counting.

Full rebuild from `my9_share_registry_v2.kind + hot_payload`:

```bash
node scripts/rebuild-trends-kind-v3.mjs
```

Useful flag:

- `node scripts/rebuild-trends-kind-v3.mjs --now-ms=<timestamp_ms>`
- `node scripts/rebuild-trends-kind-v3.mjs --max-attempts=30 --lock-timeout-ms=3000`

Cutover runbook:

1. Run rebuild once before switching app read/write to v3.
2. Deploy app code (trend read/write -> v3).
3. Run rebuild again immediately to fill deployment gap.
4. Legacy subject-grain trend tables were retired on `2026-03-16`; do not recreate them.

## DB usage monitor

```bash
node scripts/monitor-db-usage.mjs
```

Useful flags:

- `node scripts/monitor-db-usage.mjs --json`
- `node scripts/monitor-db-usage.mjs --max-mb=512 --warn-percent=70 --critical-percent=90`
- `node scripts/monitor-db-usage.mjs --top=15`
- `node scripts/monitor-db-usage.mjs --fail-on=warn` or `--fail-on=critical`
- `node scripts/monitor-db-usage.mjs --exact-counts` (slower, full table count)

## Trend cleanup

```bash
node scripts/cleanup-trend-counts.mjs
```

Useful flags:

- `node scripts/cleanup-trend-counts.mjs --cleanup-trend-days=190`

## Share view analytics rollup

The Worker logs share page document requests into Workers Analytics Engine and the daily cron writes absolute cumulative counts into:

- `my9_share_view_total_v1`

Current dataset bindings:

- production: `my9_share_views_v1`
- test: `my9_share_views_test_v1`

## Cloudflare Cron (daily)

- Scheduler entry: `worker.js` `scheduled()`
- Config file: `wrangler.jsonc`
- Current schedule: `5 16 * * *` (UTC, Beijing `00:05`, once per day)
- Scheduled job default behavior: clean up old trend rows, then roll up share view totals from Workers Analytics Engine into Postgres

Notes:

- Runtime share page tracking writes to the `MY9_SHARE_VIEW_ANALYTICS` Analytics Engine binding.
- Trend cleanup removes day/hour rows older than `MY9_TREND_CLEANUP_DAYS`.
- Postgres rollup stores one row per `share_id`; the cron recomputes totals for all closed Beijing natural days up to the previous day.
- For production hardening, prefer syncing a dedicated `MY9_ANALYTICS_API_TOKEN` instead of reusing the deployment token.
- Failed runs should be inspected in Worker logs and re-run manually when needed.
