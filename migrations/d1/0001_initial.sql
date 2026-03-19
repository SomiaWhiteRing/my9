PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS my9_share_registry_v2 (
  share_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  creator_name TEXT,
  content_hash TEXT NOT NULL UNIQUE,
  hot_payload TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_viewed_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS my9_share_registry_v2_kind_created_idx
ON my9_share_registry_v2 (kind, created_at DESC);

CREATE TABLE IF NOT EXISTS my9_share_alias_v1 (
  share_id TEXT PRIMARY KEY,
  target_share_id TEXT NOT NULL REFERENCES my9_share_registry_v2(share_id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS my9_share_alias_v1_target_idx
ON my9_share_alias_v1 (target_share_id);

CREATE TABLE IF NOT EXISTS my9_subject_dim_v1 (
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  name TEXT NOT NULL,
  localized_name TEXT,
  cover TEXT,
  release_year INTEGER,
  genres TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, subject_id)
);

CREATE INDEX IF NOT EXISTS my9_subject_dim_v1_subject_idx
ON my9_subject_dim_v1 (subject_id);

CREATE TABLE IF NOT EXISTS my9_subject_genre_dim_v1 (
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  genre TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, subject_id, genre)
);

CREATE INDEX IF NOT EXISTS my9_subject_genre_dim_v1_kind_genre_idx
ON my9_subject_genre_dim_v1 (kind, genre, subject_id);

CREATE TABLE IF NOT EXISTS my9_share_subject_slot_v1 (
  share_id TEXT NOT NULL REFERENCES my9_share_registry_v2(share_id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  slot_index INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  day_key INTEGER NOT NULL,
  hour_bucket INTEGER NOT NULL,
  PRIMARY KEY (share_id, slot_index)
);

CREATE TABLE IF NOT EXISTS my9_trend_subject_kind_all_v3 (
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, subject_id)
);

CREATE INDEX IF NOT EXISTS my9_trend_subject_kind_all_v3_kind_count_idx
ON my9_trend_subject_kind_all_v3 (kind, count DESC, subject_id);

CREATE TABLE IF NOT EXISTS my9_trend_subject_kind_day_v3 (
  kind TEXT NOT NULL,
  day_key INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, day_key, subject_id)
);

CREATE INDEX IF NOT EXISTS my9_trend_subject_kind_day_v3_kind_day_count_idx
ON my9_trend_subject_kind_day_v3 (kind, day_key, count DESC, subject_id);

CREATE TABLE IF NOT EXISTS my9_trend_subject_kind_hour_v3 (
  kind TEXT NOT NULL,
  hour_bucket INTEGER NOT NULL,
  subject_id TEXT NOT NULL,
  count INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (kind, hour_bucket, subject_id)
);

CREATE INDEX IF NOT EXISTS my9_trend_subject_kind_hour_v3_kind_hour_count_idx
ON my9_trend_subject_kind_hour_v3 (kind, hour_bucket, count DESC, subject_id);

CREATE TABLE IF NOT EXISTS my9_trends_cache_v1 (
  cache_key TEXT PRIMARY KEY,
  period TEXT NOT NULL,
  view TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS my9_trends_cache_v1_expires_idx
ON my9_trends_cache_v1 (expires_at);

CREATE TABLE IF NOT EXISTS my9_system_checkpoint_v1 (
  checkpoint_key TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS my9_share_view_total_v1 (
  share_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  view_count INTEGER NOT NULL,
  last_aggregated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS my9_share_view_total_v1_kind_count_idx
ON my9_share_view_total_v1 (kind, view_count DESC, share_id);
