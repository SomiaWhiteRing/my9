import {
  getD1Database, queryAll, queryFirst, execute, type D1DatabaseLike, type D1Scalar,
} from "@/lib/share/storage-d1-runtime";
import { checkedCount, mergeCounters, parseSubjectSet, type Counter } from "@/lib/share/cooccurrence-codec";

const CONTROL = "my9_cooccurrence_control_v1";
const SUBJECT = "my9_cooccurrence_subject_v1";
const EVENT = "my9_cooccurrence_event_v1";
const WORK = "my9_cooccurrence_work_v1";
const LEASE_MS = 120_000;
type Control = {
  ready: number; valid: number; cursor: number; window_end: number; window_at: number;
  target_seq: number; last_day: number; phase: "idle" | "prepare" | "apply";
};
type Event = { seq: number; old_kind: string | null; new_kind: string | null; old_ids: string; new_ids: string };
const guard = `EXISTS (SELECT 1 FROM ${CONTROL} WHERE id = 1 AND lease = ? AND valid = 1 AND ready = 1)`;
const identity = (kind: string, id: string) => JSON.stringify([kind, id]);

async function renew(db: D1DatabaseLike, lease: string) {
  if (!await execute(db, `UPDATE ${CONTROL} SET lease_until = ? WHERE id = 1 AND lease = ? AND valid = 1 AND ready = 1`,
    [Date.now() + LEASE_MS, lease])) throw new Error("Cooccurrence lease lost");
}

async function prepareWindow(db: D1DatabaseLike, state: Control, lease: string) {
  const events = await queryAll<Event>(db,
    `SELECT seq, old_kind, new_kind, old_ids, new_ids FROM ${EVENT} WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT 2000`,
    [state.cursor, state.window_end]);
  const sets = events.flatMap((event) => [
    { kind: event.old_kind, ids: parseSubjectSet(event.old_ids), sign: -1 },
    { kind: event.new_kind, ids: parseSubjectSet(event.new_ids), sign: 1 },
  ]).filter((item): item is { kind: string; ids: string[]; sign: number } => item.kind !== null);
  const keys = new Map<string, [string, string]>();
  for (const set of sets) for (const id of set.ids) keys.set(identity(set.kind, id), [set.kind, id]);
  const allKeys = [...keys.values()];
  const ids = new Map<string, number>();
  // Bounded JSON binds avoid one network/database statement per identity.
  for (let start = 0; start < allKeys.length; start += 500) {
    await renew(db, lease);
    const chunk = JSON.stringify(allKeys.slice(start, start + 500));
    await execute(db, `INSERT INTO ${SUBJECT} (kind, subject_id)
      SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?)
      WHERE ${guard} ON CONFLICT(kind, subject_id) DO NOTHING`, [chunk, lease]);
    const rows = await queryAll<{ id: number; kind: string; subject_id: string }>(db, `
      SELECT s.id, s.kind, s.subject_id FROM json_each(?) j CROSS JOIN ${SUBJECT} s
      ON s.kind = json_extract(j.value, '$[0]') AND s.subject_id = json_extract(j.value, '$[1]')`, [chunk]);
    for (const row of rows) ids.set(identity(row.kind, row.subject_id), checkedCount(row.id));
  }
  const deltas = new Map<number, { matched: number; peers: Map<number, number> }>();
  for (const set of sets) {
    const members = set.ids.map((external) => {
      const id = ids.get(identity(set.kind, external));
      if (!id) throw new Error("Cooccurrence identity missing or lease lost");
      return id;
    });
    for (const id of members) {
      const delta = deltas.get(id) ?? { matched: 0, peers: new Map<number, number>() };
      delta.matched += set.sign;
      for (const peer of members) if (peer !== id) delta.peers.set(peer, (delta.peers.get(peer) ?? 0) + set.sign);
      deltas.set(id, delta);
    }
  }
  const rows = [...deltas];
  for (let start = 0; start < rows.length;) {
    await renew(db, lease);
    const payload: string[] = [];
    let bytes = 2;
    while (start < rows.length && payload.length < 256) {
      const [id, delta] = rows[start];
      const item = JSON.stringify([id, delta.matched, [...delta.peers].filter(([, n]) => n !== 0)]);
      if (item.length > 900_000) throw new Error("Cooccurrence work item exceeds batch limit");
      if (payload.length && bytes + item.length + 1 > 900_000) break;
      payload.push(item);
      bytes += item.length + 1;
      start++;
    }
    await execute(db, `
      INSERT INTO ${WORK} (id, window_end, matched_delta, delta)
      SELECT json_extract(value, '$[0]'), ?, json_extract(value, '$[1]'), json_extract(value, '$[2]')
      FROM json_each(?) WHERE ${guard} ON CONFLICT(id) DO NOTHING
    `, [state.window_end, `[${payload.join(",")}]`, lease]);
  }
  await execute(db, `UPDATE ${CONTROL} SET phase = 'apply' WHERE id = 1 AND lease = ? AND valid = 1 AND ready = 1`, [lease]);
  return { phase: "prepared", subjects: rows.length, events: events.length };
}

// Every call fits within the Paid D1 subrequest limit, bounds memory, and resumes
// from committed work. A failed prepare is deterministic and safe to repeat.
export async function runCooccurrenceMaintenance() {
  const db = await getD1Database();
  if (!db) return { phase: "unavailable" };
  const now = Date.now();
  const day = Math.floor((now + 8 * 3600_000) / 86400_000);
  const before = await queryFirst<Control>(db, `SELECT ready, valid, cursor, window_end, window_at,
    target_seq, last_day, phase FROM ${CONTROL} WHERE id = 1`);
  if (!before?.ready || !before.valid) return { phase: "not-ready" };
  if (before.phase === "idle" && before.cursor >= before.target_seq && before.last_day >= day) return { phase: "idle" };
  const lease = crypto.randomUUID();
  if (!await execute(db, `UPDATE ${CONTROL} SET lease = ?, lease_until = ?
    WHERE id = 1 AND lease_until < ? AND valid = 1 AND ready = 1`, [lease, now + LEASE_MS, now])) return { phase: "busy" };
  try {
    const state = await queryFirst<Control>(db, `SELECT ready, valid, cursor, window_end, window_at,
      target_seq, last_day, phase FROM ${CONTROL} WHERE id = 1`);
    if (!state) throw new Error("Missing cooccurrence control");
    // Another invocation may have finished while this invocation acquired its lease.
    if (state.phase === "idle" && state.cursor >= state.target_seq && state.last_day >= day) return { phase: "idle" };
    if (state.phase === "idle") {
      if (state.cursor >= state.target_seq) {
        const end = await queryFirst<{ seq: number }>(db, `SELECT COALESCE(MAX(seq), ?) AS seq FROM ${EVENT}`, [state.cursor]);
        state.target_seq = end?.seq ?? state.cursor;
        await execute(db, `UPDATE ${CONTROL} SET target_seq = ?, window_at = ?, last_day = ? WHERE id = 1 AND lease = ?`,
          [state.target_seq, now, day, lease]);
        state.window_at = now;
      }
      const events = await queryAll<{ seq: number }>(db,
        `SELECT seq FROM ${EVENT} WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT 2000`, [state.cursor, state.target_seq]);
      if (!events.length) return { phase: "idle" };
      state.window_end = events.at(-1)!.seq;
      state.phase = "prepare";
      await execute(db, `UPDATE ${CONTROL} SET window_end = ?, phase = 'prepare' WHERE id = 1 AND lease = ?`, [state.window_end, lease]);
    }
    if (state.phase === "prepare") return await prepareWindow(db, state, lease);

    let processed = 0;
    // 180 updates + 180 work deletions, bounded reads/renewals; no full-history query.
    for (let batch = 0; batch < 9; batch++) {
      await renew(db, lease);
      const work = await queryAll<{ id: number; matched_delta: number; delta: string }>(db,
        `SELECT id, matched_delta, delta FROM ${WORK} ORDER BY id LIMIT 20`);
      if (!work.length) {
        await execute(db, `UPDATE ${CONTROL} SET cursor = window_end, phase = 'idle', updated_at = window_at
          WHERE id = 1 AND lease = ? AND valid = 1 AND NOT EXISTS (SELECT 1 FROM ${WORK})`, [lease]);
        // Only processed events older than seven days can be removed. The
        // AUTOINCREMENT sequence survives cleanup and is never reused.
        await execute(db, `DELETE FROM ${EVENT} WHERE seq IN (
          SELECT seq FROM ${EVENT} WHERE seq <= ? AND recorded_at < ? ORDER BY seq LIMIT 1000
        ) AND ${guard}`, [state.window_end, now - 7 * 86400_000, lease]);
        return { phase: "complete", processed, through: state.window_end };
      }
      // Do not fetch vectors collectively: the largest 20 rows could exceed memory limits.
      const statements = [];
      for (const item of work) {
        const row = await queryFirst<{ counts: number[]; matched: number; applied_seq: number }>(db,
          `SELECT counts, matched, applied_seq FROM ${SUBJECT} WHERE id = ?`, [item.id]);
        if (!row) throw new Error("Missing cooccurrence state");
        if (row.applied_seq < state.window_end) {
          const merged = mergeCounters(row.counts, JSON.parse(item.delta) as Counter[]);
          const values: D1Scalar[] = [merged.counts, merged.top10, checkedCount(row.matched + item.matched_delta),
            state.window_end, state.window_at, item.id, row.applied_seq, lease];
          statements.push(db.prepare(`UPDATE ${SUBJECT} SET counts = ?, top10 = ?, matched = ?,
            applied_seq = ?, updated_at = ?, ready = 1 WHERE id = ? AND applied_seq = ? AND ${guard}`).bind(...values));
        }
        statements.push(db.prepare(`DELETE FROM ${WORK} WHERE id = ? AND window_end = ? AND ${guard}
          AND EXISTS (SELECT 1 FROM ${SUBJECT} WHERE id = ? AND applied_seq >= ?)`)
          .bind(item.id, state.window_end, lease, item.id, state.window_end));
      }
      // D1 batch is atomic. Never use the generic helper that splits batches.
      await db.batch(statements);
      processed += work.length;
    }
    return { phase: "apply", processed };
  } finally {
    await execute(db, `UPDATE ${CONTROL} SET lease = NULL, lease_until = 0 WHERE id = 1 AND lease = ?`, [lease]);
  }
}
