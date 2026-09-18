// Temporary operational Worker. Deploy only with a short-lived admin secret;
// remove the Worker after initialization. No arbitrary SQL endpoint.
import { checkedCount, packCounters, unpackCounters, type Counter } from "../lib/share/cooccurrence-codec";
import type { D1DatabaseLike } from "../lib/share/storage-d1-runtime";

type Env = { MY9_DB: D1DatabaseLike; IMPORT_TOKEN: string };
type Row = { id: number; kind: string; subject_id: string; matched: number; counts: string };
const CONTROL = "my9_cooccurrence_control_v1";
const SUBJECT = "my9_cooccurrence_subject_v1";

async function first<T>(db: D1DatabaseLike, sql: string, params: (number | string)[] = []) {
  const result = await db.prepare(sql).bind(...params).all<T>();
  return (Array.isArray(result) ? result : result.results ?? [])[0];
}

const importer = {
  async fetch(request: Request, env: Env): Promise<Response> {
    const supplied = request.headers.get("Authorization") ?? "";
    // Compare hashes to avoid a secret-dependent prefix comparison.
    const hash = async (value: string) => new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)));
    const a = await hash(supplied);
    const b = await hash(`Bearer ${env.IMPORT_TOKEN}`);
    if (!env.IMPORT_TOKEN || a.reduce((diff, byte, i) => diff | (byte ^ b[i]), 0)) return new Response(null, { status: 403 });
    try {
      if (request.method === "POST" && new URL(request.url).pathname === "/import-batches") {
        if (Number(request.headers.get("Content-Length")) > 8_000_000) throw new Error("Import exceeds byte limit");
        const text = await request.text();
        if (new TextEncoder().encode(text).length > 8_000_000) throw new Error("Import exceeds byte limit");
        const batches = JSON.parse(text) as unknown[];
        if (!Array.isArray(batches) || !batches.length || batches.length > 5) throw new Error("Invalid batch count");
        // Amortize network latency; each transaction retains its independent
        // 100-row/1-MiB limit and resumable checkpoint. At most 515 D1 statements.
        const results: unknown[] = [];
        for (const batch of batches) {
          const response = await importer.fetch(new Request(new URL("/import", request.url), {
            method: "POST", headers: { Authorization: supplied, "Content-Type": "application/json" }, body: JSON.stringify(batch),
          }), env);
          const result = await response.json();
          if (!response.ok) return Response.json({ results, error: result }, { status: response.status });
          results.push(result);
        }
        return Response.json({ results });
      }
      const db = env.MY9_DB;
      const state = await first<{ ready: number; valid: number; baseline_seq: number | null; imported_id: number; imported_hash: string | null }>(db,
        `SELECT ready, valid, baseline_seq, imported_id, imported_hash FROM ${CONTROL} WHERE id = 1`);
      if (!state?.valid) throw new Error("Invalid change log; initialization must stop");
      if (request.method === "GET") return Response.json(state, { headers: { "Cache-Control": "no-store" } });
      if (request.method !== "POST" || new URL(request.url).pathname !== "/import") return new Response(null, { status: 404 });
      if (state.ready || state.baseline_seq === null) throw new Error("Initialization is not active");
      if (Number(request.headers.get("Content-Length")) > 2_000_000) throw new Error("Import exceeds byte limit");
      const text = await request.text();
      if (new TextEncoder().encode(text).length > 2_000_000) throw new Error("Import exceeds byte limit");
      const digest = [...await hash(text)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
      const input = JSON.parse(text) as { baseline: number; updatedAt: number; previous: number; rows: Row[] };
      if (!Array.isArray(input.rows) || !input.rows.length || input.rows.length > 100 ||
          input.baseline !== state.baseline_seq || !Number.isSafeInteger(input.updatedAt)) throw new Error("Invalid import batch");
      const last = input.rows.at(-1)!.id;
      if (state.imported_id === last && state.imported_hash === digest) return Response.json({ through: last, duplicate: true });
      if (input.previous !== state.imported_id) throw new Error("Import checkpoint conflict");
      let bytes = 0;
      const statements = input.rows.map((row, index) => {
        if (row.id !== input.previous + index + 1 || !row.kind || !row.subject_id || row.subject_id.length > 512) throw new Error("Invalid identity order");
        checkedCount(row.id);
        checkedCount(row.matched);
        const raw = Uint8Array.from(atob(row.counts), (char) => char.charCodeAt(0)).buffer;
        bytes += raw.byteLength;
        if (bytes > 1_048_576 || raw.byteLength > 1_900_000) throw new Error("Import exceeds vector limit");
        const entries = unpackCounters(raw);
        let previous = 0;
        const top: Counter[] = [];
        for (const entry of entries) {
          if (entry[0] <= previous || entry[0] === row.id || entry[1] > row.matched) throw new Error("Invalid ordered vector");
          previous = entry[0];
          const pos = top.findIndex(([id, count]) => count < entry[1] || (count === entry[1] && id > entry[0]));
          if (pos >= 0) top.splice(pos, 0, entry);
          else if (top.length < 10) top.push(entry);
          if (top.length > 10) top.pop();
        }
        return db.prepare(`INSERT INTO ${SUBJECT} (id, kind, subject_id, ready, applied_seq, matched, counts, top10, updated_at)
          SELECT ?, ?, ?, 1, ?, ?, ?, ?, ? WHERE EXISTS (
            SELECT 1 FROM ${CONTROL} WHERE id = 1 AND imported_id = ? AND ready = 0 AND valid = 1 AND baseline_seq = ?)
        `).bind(row.id, row.kind, row.subject_id, input.baseline, row.matched, raw, packCounters(top), input.updatedAt, input.previous, input.baseline);
      });
      statements.push(db.prepare(`UPDATE ${CONTROL} SET imported_id = ?, imported_hash = ?
        WHERE id = 1 AND imported_id = ? AND ready = 0 AND valid = 1 AND baseline_seq = ?`)
        .bind(last, digest, input.previous, input.baseline));
      const result = await db.batch(statements);
      const check = await first<{ imported_id: number; imported_hash: string }>(db, `SELECT imported_id, imported_hash FROM ${CONTROL} WHERE id = 1`);
      if (check.imported_id !== last || check.imported_hash !== digest) throw new Error("Import lost checkpoint race");
      const meta = result.reduce<{ rows_read: number; rows_written: number }>((sum, item) => {
        const m = (item as { meta?: { rows_read?: number; rows_written?: number } }).meta;
        sum.rows_read += m?.rows_read ?? 0;
        sum.rows_written += m?.rows_written ?? 0;
        return sum;
      // Include the control reads before/after the atomic import batch.
      }, { rows_read: 2, rows_written: 0 });
      return Response.json({ through: last, bytes, ...meta });
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : "Import failed" }, { status: 409 });
    }
  },
};

export default importer;
