import { checkedCount, parseSubjectSet } from "@/lib/share/cooccurrence-codec";
import { queryAll, type D1DatabaseLike } from "@/lib/share/storage-d1-runtime";
import type { SubjectKind } from "@/lib/subject-kind";

type SnapshotRow = {
  id: number | null;
  subject_id: string;
  matched: number;
  applied_seq: number;
  updated_at: number;
  top10: number[] | null;
  excluded_share: string | null;
};

// Resolve the share at each subject's aggregate watermark, not at wall-clock
// time. An unprocessed insert must not be subtracted from yesterday's counts.
export async function readCooccurrenceSnapshots(
  db: D1DatabaseLike,
  kind: SubjectKind,
  subjectIds: string[],
  excludeShareId?: string,
  includeRanking = false,
) {
  const ids = [...new Set(subjectIds)].slice(0, 9);
  if (!ids.length) return [];
  // A complete, valid baseline contains every historical subject. Missing
  // identities (or empty identities prepared for the next window) therefore
  // mean zero at this snapshot. Keep the control guard to distinguish failure.
  const rows = await queryAll<SnapshotRow>(db, `
    WITH requested AS (SELECT value AS subject_id FROM json_each(?2))
    ${excludeShareId ? `, excluded AS MATERIALIZED (
      SELECT COALESCE((SELECT target_share_id FROM my9_share_alias_v1 WHERE share_id = ?3), ?3) AS share_id
    ), future_share_events AS MATERIALIZED (
      SELECT e.seq, e.old_kind, e.old_ids,
        e.old_share_id IS NOT NULL AND e.new_share_id IS NOT NULL
          AND e.old_share_id != e.new_share_id AS renamed
      FROM my9_cooccurrence_event_v1 e
      WHERE e.seq > (SELECT cursor FROM my9_cooccurrence_control_v1 WHERE id = 1)
        AND (e.old_share_id = (SELECT share_id FROM excluded)
          OR e.new_share_id = (SELECT share_id FROM excluded))
    ), current_share AS MATERIALIZED (
      SELECT json_object('kind', r.kind, 'ids', json_array(
        json_extract(r.hot_payload, '$[0].sid'), json_extract(r.hot_payload, '$[1].sid'),
        json_extract(r.hot_payload, '$[2].sid'), json_extract(r.hot_payload, '$[3].sid'),
        json_extract(r.hot_payload, '$[4].sid'), json_extract(r.hot_payload, '$[5].sid'),
        json_extract(r.hot_payload, '$[6].sid'), json_extract(r.hot_payload, '$[7].sid'),
        json_extract(r.hot_payload, '$[8].sid'))) AS payload
      FROM my9_share_registry_v2 r WHERE r.share_id = (SELECT share_id FROM excluded)
    )` : ""}
    SELECT s.id, q.subject_id, COALESCE(s.matched, 0) AS matched,
      COALESCE(s.applied_seq, c.cursor) AS applied_seq,
      CASE WHEN s.ready = 1 THEN s.updated_at ELSE c.updated_at END AS updated_at,
      ${includeRanking ? "CASE WHEN s.ready = 1 THEN s.top10 ELSE X'' END" : "NULL"} AS top10,
      ${excludeShareId ? `COALESCE((
        SELECT json_object('kind', e.old_kind, 'ids', json(e.old_ids), 'renamed', e.renamed)
        FROM future_share_events e
        WHERE e.seq > MAX(COALESCE(s.applied_seq, c.cursor), c.cursor)
        ORDER BY e.seq LIMIT 1
      ), (SELECT payload FROM current_share))` : "NULL"} AS excluded_share
    FROM requested q CROSS JOIN my9_cooccurrence_control_v1 c
    LEFT JOIN my9_cooccurrence_subject_v1 s ON s.kind = ?1 AND s.subject_id = q.subject_id
    WHERE c.id = 1 AND c.ready = 1 AND c.valid = 1
      AND (s.id IS NULL OR s.ready = 1 OR (s.ready = 0 AND s.matched = 0))
  `, [kind, JSON.stringify(ids), ...(excludeShareId ? [excludeShareId] : [])]);

  return rows.map((row) => {
    const share = row.excluded_share ? JSON.parse(row.excluded_share) as { kind: string | null; ids: unknown; renamed?: number } : null;
    if (share?.renamed) throw new Error("Share rename has not reached the cooccurrence snapshot");
    const members = share?.kind === kind ? parseSubjectSet(JSON.stringify(share.ids)) : [];
    const excluded = members.includes(row.subject_id);
    return {
      ...row,
      matched: checkedCount(row.matched - (excluded ? 1 : 0)),
      excludedSubjectIds: excluded ? members.filter((id) => id !== row.subject_id) : [],
    };
  });
}
