# Cloudflare WAF Edge Request Playbook

## Goal

Use Cloudflare WAF and rate limiting to reduce obvious noise before requests reach the Next.js runtime.

## Recommended Rules

1. `log` mode first, then `block` after 24h verification.
2. Rule: UUID scan on dynamic kind route.
3. Rule: `okhttp/4.11.0-SNAPSHOT` + `range=0` probing.
4. Rule: burst limiter for `/api/subjects/search`.

## Rule Drafts

### 1) UUID path probe on kind route

- Match path regex: `^/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
- Action: `block`

### 2) okhttp + range=0 probe

- Match path regex: `^/[^/]+$`
- Match query contains: `range=0`
- Match header `user-agent` contains: `okhttp/4.11.0-SNAPSHOT`
- Action: `block`

### 3) Search endpoint burst limiter

- Path: `/api/subjects/search`
- Action: `rate limit`
- Initial threshold: `60 requests / minute / IP`
- Exceed action: `block for 1 minute`

## Rollout

1. Enable all rules in `log` mode.
2. Observe Cloudflare security events for at least 24h.
3. Switch Rule 1 and Rule 2 to `block`.
4. Keep Rule 3 in rate-limit mode and adjust by error budget.
