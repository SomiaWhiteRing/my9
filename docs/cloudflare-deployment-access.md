# Cloudflare Deployment Access

## Local auth model

- Use `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`.
- Do not use global `CLOUDFLARE_API_KEY` for routine deployment.
- The token should be account-scoped and narrowed to the target account and the `shatranj.space` zone.

## Build-time site URL

- `NEXT_PUBLIC_SITE_URL` controls canonical metadata, `robots.txt`, `sitemap.xml`, and the footer hits badge host.
- `SITE_URL` is an optional server-side override. If omitted, the app falls back to `NEXT_PUBLIC_SITE_URL`.
- Before deploying the test environment, set:

```bash
NEXT_PUBLIC_SITE_URL=https://my9test.shatranj.space
SITE_URL=https://my9test.shatranj.space
```

## Worker config

- Production and `env.test` both bind D1 through `MY9_DB`.
- `env.test` intentionally leaves cron disabled so the test deployment does not run the production maintenance schedule.

## Verification flow

Run the read-only verifier before any deploy:

```bash
npm run cf:verify-access
```

Recommended deploy sequence for the test domain:

```bash
npm run cf:verify-access
npm run cf:build:test
npm run cf:deploy:test
```

## Secrets and vars to provision

Secrets:

- `BANGUMI_ACCESS_TOKEN`
- `BANGUMI_USER_AGENT`
- `MY9_ANALYTICS_ACCOUNT_ID`
- `MY9_ANALYTICS_API_TOKEN`

Non-secret vars:

- `NEXT_PUBLIC_SITE_URL`
- `SITE_URL`
- `NEXT_PUBLIC_GA_ID`
- `MY9_TREND_CLEANUP_DAYS`
- `MY9_TRENDS_24H_SOURCE`
- `MY9_SHARE_VIEW_ANALYTICS_DATASET`

Bindings:

- `MY9_DB`
- `MY9_SHARE_VIEW_ANALYTICS`
- `ASSETS`

Notes:

- `npm run cf:verify-access` is intentionally non-mutating. It does not prove secret write access or Custom Domain creation on its own.
- Neon credentials are no longer part of the Worker runtime.
