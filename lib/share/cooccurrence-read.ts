import { getD1Database, queryAll, queryFirst } from "@/lib/share/storage-d1-runtime";
import { checkedCount, unpackCounters, type Counter } from "@/lib/share/cooccurrence-codec";
import { readCooccurrenceSnapshots } from "@/lib/share/cooccurrence-snapshot";
import { normalizeShareId } from "@/lib/share/id";
import { parseSubjectKind } from "@/lib/subject-kind";

export async function handleRelatedSelectionsRequest(request: Request) {
  const url = new URL(request.url);
  const kind = parseSubjectKind(url.searchParams.get("kind"));
  const subjectId = url.searchParams.get("subjectId")?.trim();
  const rawExcludeShareId = url.searchParams.get("excludeShareId");
  const excludeShareId = normalizeShareId(rawExcludeShareId);
  const unavailable = () => Response.json({ error: "共同构成统计正在更新，请稍后再试。" }, {
    status: 503, headers: { "Cache-Control": "no-store", "Retry-After": "60" },
  });
  if (!kind || !subjectId || subjectId.length > 512 || (rawExcludeShareId !== null && !excludeShareId)) {
    return Response.json({ error: "无效的作品" }, { status: 400 });
  }
  try {
    const db = await getD1Database();
    if (!db) return unavailable();
    const state = await queryFirst<{
      ready: number; valid: number; count_ready: number | null; kind_shares: number;
    }>(db, `
      SELECT c.ready, c.valid, s.ready AS count_ready, COALESCE(k.share_count, 0) AS kind_shares
      FROM my9_cooccurrence_control_v1 c
      LEFT JOIN my9_share_count_state_v1 s ON s.id = 1
      LEFT JOIN my9_share_count_kind_v1 k ON k.kind = ?
      WHERE c.id = 1
    `, [kind]);
    if (!state?.ready || !state.valid || state.count_ready !== 1 ||
        !Number.isSafeInteger(state.kind_shares) || state.kind_shares < 0) return unavailable();
    const [row] = await readCooccurrenceSnapshots(db, kind, [subjectId], excludeShareId ?? undefined, true);
    // Missing subjects in a healthy snapshot have zero matches. A missing
    // snapshot still means unavailable, never a confirmed zero.
    if (!row?.top10) return unavailable();
    const originalTop = unpackCounters(row.top10);
    let top = originalTop;
    if (row.excludedSubjectIds.length) {
      const peers = await queryAll<{ id: number }>(db, `
        SELECT s.id FROM json_each(?) j CROSS JOIN my9_cooccurrence_subject_v1 s
        ON s.kind = ? AND s.subject_id = j.value
      `, [JSON.stringify(row.excludedSubjectIds), kind]);
      if (peers.length !== row.excludedSubjectIds.length) return unavailable();
      const excluded = new Set(peers.map(({ id }) => id));
      const compare = (a: Counter, b: Counter) => b[1] - a[1] || a[0] - b[0];
      const subtract = (entries: Counter[]) => {
        const ranked: Counter[] = [];
        for (const [id, count] of entries) {
          const adjusted = checkedCount(count - (excluded.has(id) ? 1 : 0));
          if (!adjusted) continue;
          const entry: Counter = [id, adjusted];
          const position = ranked.findIndex((other) => compare(entry, other) < 0);
          if (position >= 0) ranked.splice(position, 0, entry);
          else if (ranked.length < 10) ranked.push(entry);
          if (ranked.length > 10) ranked.pop();
        }
        return ranked;
      };
      top = subtract(originalTop);
      // Only fetch the full vector if an unseen candidate could enter the top
      // ten after subtraction. Preserve the aggregate's count/ID tie-break.
      if (originalTop.length === 10 && (top.length < 10 || compare(top[9], originalTop[9]) > 0)) {
        const complete = await queryFirst<{ counts: number[] }>(db, `
          SELECT counts FROM my9_cooccurrence_subject_v1
          WHERE id = ? AND applied_seq = ? AND ready = 1
        `, [row.id, row.applied_seq]);
        if (!complete) return unavailable();
        top = subtract(unpackCounters(complete.counts));
      }
    }
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
    return Response.json({ items, matchedShares: row.matched, kindShares: state.kind_shares, updatedAt: row.updated_at }, {
      headers: { "Cache-Control": "public, max-age=300, s-maxage=3600" },
    });
  } catch (error) {
    console.error("[cooccurrence] read failed", error);
    return unavailable();
  }
}
