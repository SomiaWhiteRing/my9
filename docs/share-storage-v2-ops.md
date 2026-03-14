# Share Storage V2 Ops

## New env vars

- Cloudflare Worker binding: `MY9_COLD_STORAGE` (R2 bucket binding used by the runtime)
- `R2_ENDPOINT`
- `R2_BUCKET`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- Optional: `R2_REGION` (default: `auto`)
- Optional: `MY9_ENABLE_V1_FALLBACK=0` (default keeps `my9_shares_v1` read fallback)
- Optional: `CRON_SECRET` (recommended in production, used by manual `/api/cron/archive` authorization header)
- Optional: `MY9_ARCHIVE_OLDER_THAN_DAYS` (default `30`)
- Optional: `MY9_ARCHIVE_BATCH_SIZE` (default `500`)
- Optional: `MY9_ARCHIVE_CLEANUP_TREND_DAYS` (default `190`)
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

## Migration verify

Run migration consistency checks (`old`, `v2`, `alias`, `missing`):

```bash
node scripts/verify-shares-v2-migration.mjs
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
4. Keep old tables (`my9_trend_subject_all_v2`, `my9_trend_subject_day_v2`, `my9_trend_subject_hour_v1`) for rollback observation, then delete manually after stability.

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

## Cold archive + trend cleanup

```bash
node scripts/archive-shares-cold.mjs
```

Useful flags:

- `node scripts/archive-shares-cold.mjs --older-than-days=30`
- `node scripts/archive-shares-cold.mjs --batch-size=500`
- `node scripts/archive-shares-cold.mjs --cleanup-trend-days=190`

## Cloudflare Cron (daily)

- Cron route: `/api/cron/archive`
- Scheduler entry: `worker.ts` `scheduled()`
- Config file: `wrangler.jsonc`
- Current schedule: `5 16 * * *` (UTC, Beijing `00:05`, once per day)
- Scheduled job default behavior: archive shares older than `30` days
- Manual route behavior: same as scheduled job, but protected by `CRON_SECRET` in production

Notes:

- Runtime cold storage reads/writes use the `MY9_COLD_STORAGE` R2 binding first.
- Existing Node scripts still use `R2_ENDPOINT` / `R2_BUCKET` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY`.
- Failed runs should be inspected in Worker logs and re-run manually when needed.

Recommended setup:

1. Bind `MY9_COLD_STORAGE` in `wrangler.jsonc`.
2. Set `CRON_SECRET` in Worker secrets if you want to keep the manual route protected.
3. Deploy so `wrangler.jsonc` cron is applied.
4. Verify route manually once:
   ```bash
   curl -H "Authorization: Bearer <CRON_SECRET>" https://<your-domain>/api/cron/archive
   ```
