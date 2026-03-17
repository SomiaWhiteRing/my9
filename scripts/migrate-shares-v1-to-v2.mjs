#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

function loadLocalEnvFiles() {
  const candidates = [".env.local", ".env"];
  for (const file of candidates) {
    try {
      process.loadEnvFile(resolve(process.cwd(), file));
    } catch {
      // ignore missing env file
    }
  }
}

loadLocalEnvFiles();

const SHARES_V1_TABLE = "my9_shares_v1";
const SHARES_V2_TABLE = "my9_share_registry_v2";
const SHARE_ALIAS_TABLE = "my9_share_alias_v1";
const SUBJECT_DIM_TABLE = "my9_subject_dim_v1";
const SHARES_V2_KIND_CREATED_IDX = `${SHARES_V2_TABLE}_kind_created_idx`;
const SHARE_ALIAS_TARGET_IDX = `${SHARE_ALIAS_TABLE}_target_idx`;

const CHECKPOINT_PATH = resolve(process.cwd(), "scripts/.migrate-shares-v1.checkpoint.json");

function readEnv(...names) {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function buildDatabaseUrlFromNeonParts() {
  const host = readEnv("NEON_DATABASE_PGHOST_UNPOOLED", "NEON_DATABASE_PGHOST");
  const user = readEnv("NEON_DATABASE_PGUSER");
  const password = readEnv("NEON_DATABASE_PGPASSWORD", "NEON_DATABASE_POSTGRES_PASSWORD");
  const database = readEnv("NEON_DATABASE_PGDATABASE", "NEON_DATABASE_POSTGRES_DATABASE");
  if (!host || !user || !password || !database) return null;

  let hostWithPort = host;
  const port = readEnv("NEON_DATABASE_PGPORT");
  if (port && !host.includes(":")) {
    hostWithPort = `${host}:${port}`;
  }

  const sslMode = readEnv("NEON_DATABASE_PGSSLMODE") ?? "require";
  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(
    password
  )}@${hostWithPort}/${encodeURIComponent(database)}?sslmode=${encodeURIComponent(sslMode)}`;
}

function parseArg(name, fallback) {
  const prefix = `--${name}=`;
  const withEquals = process.argv.find((arg) => arg.startsWith(prefix));
  if (withEquals) {
    const parsed = Number(withEquals.slice(prefix.length));
    return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
  }

  const index = process.argv.indexOf(`--${name}`);
  if (index === -1 || index + 1 >= process.argv.length) return fallback;
  const parsed = Number(process.argv[index + 1]);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function sanitizeText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeGenres(value) {
  if (!Array.isArray(value)) return undefined;
  const cleaned = value.map((item) => sanitizeText(item)).filter(Boolean).slice(0, 5);
  if (cleaned.length === 0) return undefined;
  return Array.from(new Set(cleaned));
}

function normalizeSubjectId(value) {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
  if (typeof value === "string" && value.trim()) return value.trim();
  return null;
}

function toCompactAndSubjects(games) {
  const payload = Array.from({ length: 9 }, () => null);
  const subjects = new Map();

  for (let i = 0; i < 9; i += 1) {
    const item = Array.isArray(games) ? games[i] : null;
    if (!item || typeof item !== "object") continue;

    const name = sanitizeText(item.name) || "untitled";
    const sid = normalizeSubjectId(item.id);
    if (!sid) continue;
    const comment = sanitizeText(item.comment);
    const spoiler = Boolean(item.spoiler);

    payload[i] = {
      sid,
      ...(comment ? { c: comment } : {}),
      ...(spoiler ? { s: 1 } : {}),
    };

    const localizedName = sanitizeText(item.localizedName);
    const current = subjects.get(sid) || {
      subjectId: sid,
      name,
      localizedName: undefined,
      cover: null,
      releaseYear: undefined,
      genres: undefined,
    };

    current.name = current.name || name;
    if (!current.localizedName && localizedName && localizedName !== name) current.localizedName = localizedName;
    if (!current.cover && typeof item.cover === "string" && item.cover.trim()) current.cover = item.cover.trim();
    if (
      current.releaseYear === undefined &&
      typeof item.releaseYear === "number" &&
      Number.isFinite(item.releaseYear)
    ) {
      current.releaseYear = Math.trunc(item.releaseYear);
    }
    if (!current.genres || current.genres.length === 0) {
      current.genres = normalizeGenres(item.genres);
    }
    subjects.set(sid, current);
  }

  return { payload, subjects };
}

function createContentHash(kind, creatorName, payload) {
  const canonical = JSON.stringify({
    kind,
    creatorName: creatorName || "",
    slots: payload.map((slot) => (slot ? { sid: slot.sid, c: slot.c || "", s: Boolean(slot.s) } : null)),
  });
  return createHash("sha256").update(canonical).digest("hex");
}

function loadCheckpoint() {
  try {
    return JSON.parse(readFileSync(CHECKPOINT_PATH, "utf8"));
  } catch {
    return { createdAt: 0, shareId: "" };
  }
}

function saveCheckpoint(value) {
  writeFileSync(CHECKPOINT_PATH, JSON.stringify(value, null, 2));
}

async function ensureV2Schema(sql) {
  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${SHARES_V2_TABLE} (
      share_id TEXT PRIMARY KEY,
      kind TEXT NOT NULL,
      creator_name TEXT,
      content_hash TEXT NOT NULL UNIQUE,
      hot_payload JSONB NOT NULL,
      created_at BIGINT NOT NULL,
      updated_at BIGINT NOT NULL,
      last_viewed_at BIGINT NOT NULL
    )
    `
  );
  await sql.query(
    `
    CREATE INDEX IF NOT EXISTS ${SHARES_V2_KIND_CREATED_IDX}
    ON ${SHARES_V2_TABLE} (kind, created_at DESC)
    `
  );
  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${SHARE_ALIAS_TABLE} (
      share_id TEXT PRIMARY KEY,
      target_share_id TEXT NOT NULL REFERENCES ${SHARES_V2_TABLE}(share_id) ON DELETE CASCADE,
      created_at BIGINT NOT NULL
    )
    `
  );
  await sql.query(
    `
    CREATE INDEX IF NOT EXISTS ${SHARE_ALIAS_TARGET_IDX}
    ON ${SHARE_ALIAS_TABLE} (target_share_id)
    `
  );

  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${SUBJECT_DIM_TABLE} (
      kind TEXT NOT NULL,
      subject_id TEXT NOT NULL,
      name TEXT NOT NULL,
      localized_name TEXT,
      cover TEXT,
      release_year INT,
      genres JSONB,
      updated_at BIGINT NOT NULL,
      PRIMARY KEY (kind, subject_id)
    )
    `
  );
}

async function main() {
  const batchSize = parseArg("batch-size", 300);
  const maxRows = parseArg("max-rows", 0);

  const databaseUrl = process.env.DATABASE_URL || buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  const sql = neon(databaseUrl);
  await ensureV2Schema(sql);

  const checkpoint = loadCheckpoint();
  let cursorCreatedAt = Number(checkpoint.createdAt || 0);
  let cursorShareId = String(checkpoint.shareId || "");
  let processed = 0;
  let inserted = 0;
  let aliased = 0;
  const startedAt = Date.now();

  while (true) {
    if (maxRows > 0 && processed >= maxRows) break;
    const size = maxRows > 0 ? Math.min(batchSize, maxRows - processed) : batchSize;
    const rows = await sql.query(
      `
      SELECT share_id, kind, creator_name, games, created_at, updated_at, last_viewed_at
      FROM ${SHARES_V1_TABLE}
      WHERE (created_at > $1) OR (created_at = $1 AND share_id > $2)
      ORDER BY created_at ASC, share_id ASC
      LIMIT $3
      `,
      [cursorCreatedAt, cursorShareId, size]
    );

    if (rows.length === 0) break;

    const aliasRows = [];
    const subjectMap = new Map();
    const shareUpsertRows = [];
    const preparedRows = [];
    const seenContentHash = new Set();

    for (const row of rows) {
      const kind = String(row.kind || "game");
      const creatorName = typeof row.creator_name === "string" ? row.creator_name : null;
      const createdAt = Number(row.created_at || Date.now());
      const updatedAt = Number(row.updated_at || createdAt);
      const lastViewedAt = Number(row.last_viewed_at || updatedAt);
      const games = Array.isArray(row.games) ? row.games : [];

      const { payload, subjects } = toCompactAndSubjects(games);
      const contentHash = createContentHash(kind, creatorName, payload);
      const isRepresentative = !seenContentHash.has(contentHash);

      if (isRepresentative) {
        seenContentHash.add(contentHash);
        shareUpsertRows.push({
          share_id: String(row.share_id),
          kind,
          creator_name: creatorName,
          content_hash: contentHash,
          hot_payload: payload,
          created_at: createdAt,
          updated_at: updatedAt,
          last_viewed_at: lastViewedAt,
        });
      }

      preparedRows.push({
        shareId: String(row.share_id),
        kind,
        contentHash,
        payload,
        subjects,
        createdAt,
        updatedAt,
        isRepresentative,
      });

      processed += 1;
      cursorCreatedAt = Number(row.created_at || cursorCreatedAt);
      cursorShareId = String(row.share_id || cursorShareId);
    }

    const upsertedRows = await sql.query(
      `
      WITH input_rows AS (
        SELECT
          share_id,
          kind,
          creator_name,
          content_hash,
          hot_payload,
          created_at,
          updated_at,
          last_viewed_at
        FROM jsonb_to_recordset($1::jsonb) AS s(
          share_id text,
          kind text,
          creator_name text,
          content_hash text,
          hot_payload jsonb,
          created_at bigint,
          updated_at bigint,
          last_viewed_at bigint
        )
      )
      INSERT INTO ${SHARES_V2_TABLE} (
        share_id, kind, creator_name, content_hash, hot_payload, created_at, updated_at, last_viewed_at
      )
      SELECT
        share_id, kind, creator_name, content_hash, hot_payload, created_at, updated_at, last_viewed_at
      FROM input_rows
      ON CONFLICT (content_hash) DO UPDATE
      SET
        updated_at = GREATEST(${SHARES_V2_TABLE}.updated_at, EXCLUDED.updated_at),
        last_viewed_at = GREATEST(${SHARES_V2_TABLE}.last_viewed_at, EXCLUDED.last_viewed_at)
      RETURNING content_hash, share_id, (xmax = 0) AS inserted
      `,
      [JSON.stringify(shareUpsertRows)]
    );

    const canonicalByHash = new Map();
    for (const row of upsertedRows) {
      canonicalByHash.set(String(row.content_hash), {
        shareId: String(row.share_id),
        inserted: Boolean(row.inserted),
      });
    }

    for (const item of preparedRows) {
      const canonical = canonicalByHash.get(item.contentHash);
      if (!canonical) {
        throw new Error(`missing canonical row for content_hash: ${item.contentHash}`);
      }

      if (canonical.shareId !== item.shareId) {
        aliasRows.push({
          share_id: item.shareId,
          target_share_id: canonical.shareId,
          created_at: item.createdAt,
        });
        aliased += 1;
      } else if (item.isRepresentative && canonical.inserted) {
        inserted += 1;

        for (const subject of item.subjects.values()) {
          const key = `${item.kind}::${subject.subjectId}`;
          const existing = subjectMap.get(key);
          if (!existing) {
            subjectMap.set(key, {
              kind: item.kind,
              subject_id: subject.subjectId,
              name: subject.name,
              localized_name: subject.localizedName ?? null,
              cover: subject.cover,
              release_year: subject.releaseYear ?? null,
              genres: subject.genres ?? null,
              updated_at: item.updatedAt,
            });
          } else {
            existing.name = existing.name || subject.name;
            existing.localized_name = existing.localized_name || subject.localizedName || null;
            existing.cover = existing.cover || subject.cover;
            existing.release_year = existing.release_year ?? subject.releaseYear ?? null;
            if (!existing.genres || existing.genres.length === 0) {
              existing.genres = subject.genres ?? null;
            }
            existing.updated_at = Math.max(existing.updated_at, item.updatedAt);
          }
        }

      }
    }

    if (aliasRows.length > 0) {
      await sql.query(
        `
        WITH input_rows AS (
          SELECT share_id, target_share_id, created_at
          FROM jsonb_to_recordset($1::jsonb) AS a(
            share_id text,
            target_share_id text,
            created_at bigint
          )
        )
        INSERT INTO ${SHARE_ALIAS_TABLE} (share_id, target_share_id, created_at)
        SELECT share_id, target_share_id, created_at
        FROM input_rows
        ON CONFLICT (share_id) DO NOTHING
        `,
        [JSON.stringify(aliasRows)]
      );
    }

    const subjectRows = Array.from(subjectMap.values());
    if (subjectRows.length > 0) {
      await sql.query(
        `
        WITH input_rows AS (
          SELECT kind, subject_id, name, localized_name, cover, release_year, genres, updated_at
          FROM jsonb_to_recordset($1::jsonb) AS s(
            kind text,
            subject_id text,
            name text,
            localized_name text,
            cover text,
            release_year int,
            genres jsonb,
            updated_at bigint
          )
        )
        INSERT INTO ${SUBJECT_DIM_TABLE} (kind, subject_id, name, localized_name, cover, release_year, genres, updated_at)
        SELECT kind, subject_id, name, localized_name, cover, release_year, genres, updated_at
        FROM input_rows
        ON CONFLICT (kind, subject_id) DO UPDATE SET
          name = EXCLUDED.name,
          localized_name = COALESCE(EXCLUDED.localized_name, ${SUBJECT_DIM_TABLE}.localized_name),
          cover = COALESCE(EXCLUDED.cover, ${SUBJECT_DIM_TABLE}.cover),
          release_year = COALESCE(EXCLUDED.release_year, ${SUBJECT_DIM_TABLE}.release_year),
          genres = COALESCE(EXCLUDED.genres, ${SUBJECT_DIM_TABLE}.genres),
          updated_at = EXCLUDED.updated_at
        `,
        [JSON.stringify(subjectRows)]
      );
    }

    saveCheckpoint({ createdAt: cursorCreatedAt, shareId: cursorShareId, processed });
    const elapsedSeconds = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
    const rate = (processed / elapsedSeconds).toFixed(2);
    console.log(`[migrate] processed=${processed} inserted=${inserted} aliased=${aliased} rate=${rate}/s`);
  }

  console.log(
    JSON.stringify(
      { ok: true, processed, inserted, aliased, checkpoint: { createdAt: cursorCreatedAt, shareId: cursorShareId } },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
