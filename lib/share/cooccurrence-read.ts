import { getD1Database, queryAll, queryFirst } from "@/lib/share/storage-d1-runtime";
import { unpackCounters } from "@/lib/share/cooccurrence-codec";
import { parseSubjectKind } from "@/lib/subject-kind";

export async function handleRelatedSelectionsRequest(request: Request) {
  const url = new URL(request.url);
  const kind = parseSubjectKind(url.searchParams.get("kind"));
  const subjectId = url.searchParams.get("subjectId")?.trim();
  const unavailable = () => Response.json({ error: "共同构成统计正在更新，请稍后再试。" }, {
    status: 503, headers: { "Cache-Control": "no-store", "Retry-After": "60" },
  });
  if (!kind || !subjectId || subjectId.length > 512) {
    return Response.json({ error: "无效的作品" }, { status: 400 });
  }
  try {
    const db = await getD1Database();
    if (!db) return unavailable();
    const state = await queryFirst<{ ready: number; valid: number; updated_at: number }>(db,
      "SELECT ready, valid, updated_at FROM my9_cooccurrence_control_v1 WHERE id = 1");
    if (!state?.ready || !state.valid) return unavailable();
    // Do not select the complete vector on the user-facing path.
    const row = await queryFirst<{ top10: number[]; matched: number; ready: number; updated_at: number }>(db,
      "SELECT top10, matched, ready, updated_at FROM my9_cooccurrence_subject_v1 WHERE kind = ? AND subject_id = ?",
      [kind, subjectId]);
    // A newly submitted subject may not have entered the next daily window yet.
    if (!row?.ready) return unavailable();
    const top = unpackCounters(row.top10);
    const names = top.length ? await queryAll<{ id: number; subject_id: string; name: string }>(db, `
      SELECT s.id, s.subject_id,
        COALESCE((SELECT COALESCE(NULLIF(d.localized_name, ''), d.name)
          FROM my9_subject_dim_v1 d WHERE d.kind = s.kind AND d.subject_id = s.subject_id), s.subject_id) AS name
      FROM my9_cooccurrence_subject_v1 s WHERE s.id IN (${top.map(() => "?").join(",")})
    `, top.map(([id]) => id)) : [];
    const byId = new Map(names.map((item) => [item.id, item]));
    const items = top.map(([id, count]) => {
      const peer = byId.get(id);
      if (!peer) throw new Error("Missing cooccurrence identity");
      return { subjectId: peer.subject_id, name: peer.name, count };
    });
    return Response.json({ items, matchedShares: row.matched, updatedAt: row.updated_at }, {
      headers: { "Cache-Control": "public, max-age=300, s-maxage=3600" },
    });
  } catch (error) {
    console.error("[cooccurrence] read failed", error);
    return unavailable();
  }
}
