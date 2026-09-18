// Kept dependency-free so the one-off bootstrap uses the same SQL as the Worker.
export const DISCOVERY_SNAPSHOT_KEY = "system:share-discovery:popular:v1";
export const VIEW_ROLLUP_KEY = "system:share-view-rollup:v1";
export const DISCOVERY_INDEX = "my9_share_registry_v2_creator_created_share_idx";
export const DISCOVERY_PAGE_SIZE = 20;

export const readDiscoverySnapshotSql = `SELECT payload FROM my9_system_checkpoint_v1 WHERE checkpoint_key = ?`;
export const readViewWatermarkSql = `SELECT json_extract(payload, '$.rolledThroughMs') AS rolled_through
  FROM my9_system_checkpoint_v1 WHERE checkpoint_key = ?`;

export function searchDiscoverySql(hasCursor: boolean) {
  // Materialize the bounded page before looking up view counts. INDEXED BY also
  // prevents an accidental full-table scan when a deployment is missing its migration.
  return `WITH page AS MATERIALIZED (
    SELECT share_id, kind, creator_name, created_at
    FROM my9_share_registry_v2 INDEXED BY ${DISCOVERY_INDEX}
    WHERE creator_name = ? AND creator_name IS NOT NULL AND creator_name <> ''
      ${hasCursor ? "AND (created_at, share_id) < (?, ?)" : ""}
    ORDER BY created_at DESC, share_id DESC LIMIT 21
  ) SELECT p.*, COALESCE((SELECT v.view_count FROM my9_share_view_total_v1 v
    WHERE v.share_id = p.share_id), 0) AS view_count
    FROM page p ORDER BY p.created_at DESC, p.share_id DESC`;
}

export function refreshDiscoverySnapshotSql(kindCount: number) {
  return `WITH kinds(kind) AS (VALUES ${Array.from({ length: kindCount }, () => "(?)").join(",")}),
    candidates AS MATERIALIZED (
      SELECT v.share_id, v.kind, v.view_count FROM kinds k CROSS JOIN my9_share_view_total_v1 v
      WHERE v.rowid IN (
        SELECT v2.rowid FROM my9_share_view_total_v1 v2
        JOIN my9_share_registry_v2 r2 ON r2.share_id = v2.share_id
        WHERE v2.kind = k.kind ORDER BY v2.view_count DESC, v2.share_id ASC LIMIT 20
      )
    ), top_shares AS MATERIALIZED (
      SELECT * FROM candidates ORDER BY view_count DESC, share_id ASC LIMIT 20
    ), entries AS MATERIALIZED (
      SELECT t.share_id, r.kind, t.view_count, r.creator_name, r.created_at
      FROM top_shares t JOIN my9_share_registry_v2 r ON r.share_id = t.share_id
      ORDER BY t.view_count DESC, t.share_id ASC
    ), watermark AS (
      SELECT json_extract(payload, '$.rolledThroughMs') AS rolled_through
      FROM my9_system_checkpoint_v1 WHERE checkpoint_key = ?
    )
    INSERT INTO my9_system_checkpoint_v1 (checkpoint_key, payload, updated_at)
    SELECT ?, json_object('version', 1, 'items', json((
      SELECT json_group_array(json_object('shareId', share_id, 'kind', kind,
        'creatorName', creator_name, 'createdAt', created_at, 'viewCount', view_count))
      FROM entries
    )), 'nextCursor', NULL, 'viewsThrough',
      strftime('%Y-%m-%d', (rolled_through - 1) / 1000, 'unixepoch', '+8 hours')),
      CAST(unixepoch() * 1000 AS INTEGER)
    FROM watermark WHERE typeof(rolled_through) = 'integer' AND rolled_through > 0
    ON CONFLICT(checkpoint_key) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at`;
}
