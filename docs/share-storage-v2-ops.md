# Share Storage Ops

## Runtime shape

- Production and test both run D1-only share storage.
- Main tables:
  - `my9_share_registry_v2`
  - `my9_share_alias_v1`
  - `my9_subject_dim_v1`
  - `my9_subject_genre_dim_v1`
  - `my9_share_subject_slot_v1`
  - `my9_trend_subject_kind_all_v3`
  - `my9_trend_subject_kind_day_v3`
  - `my9_trend_subject_kind_hour_v3`
  - `my9_trends_cache_v1`
  - `my9_share_view_total_v1`

## Required env

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- Optional: `MY9_DB_WRANGLER_ENV` for local `getPlatformProxy()` environment selection
- Optional: `MY9_ANALYTICS_ACCOUNT_ID`
- Optional: `MY9_ANALYTICS_API_TOKEN`
- Optional: `MY9_TREND_CLEANUP_DAYS` (default `190`, do not set below `180`)
- Optional: `MY9_TRENDS_24H_SOURCE=day|hour`

## D1 schema

Apply migrations before first use of a new database:

```bash
npm run d1:migrate:local
npm run d1:migrate:remote
```

For the test environment:

```bash
npm run d1:migrate:local:test
npm run d1:migrate:remote:test
```

## Trend rebuild

Rebuild kind-grain trend tables from `my9_share_subject_slot_v1`:

```bash
npm run d1:rebuild-trends
```

Test environment:

```bash
npm run d1:rebuild-trends:test
```

## Trend cleanup

Delete expired day/hour trend rows:

```bash
npm run d1:cleanup-trends
```

Test environment:

```bash
npm run d1:cleanup-trends:test
```

## Share view analytics rollup

- Share page document requests are written into the `MY9_SHARE_VIEW_ANALYTICS` Workers Analytics Engine binding.
- Daily cron aggregates closed Beijing natural days into `my9_share_view_total_v1`.
- Rollup writes cumulative totals, one row per `share_id`.

## Cron

- Scheduler entry: `worker.js` `scheduled()`
- Config file: `wrangler.jsonc`
- Current schedule: `5 16 * * *` (UTC, Beijing `00:05`)
- Daily flow:
  1. clean old trend rows
  2. roll up Analytics Engine totals into D1
