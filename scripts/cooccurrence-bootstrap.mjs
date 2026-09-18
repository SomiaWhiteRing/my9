#!/usr/bin/env node
// Operational commands: scan, import, verify, publish. Artifacts MUST be outside
// the repository. All D1 metering is appended to the run's usage ledger.
import fs from "node:fs";
import path from "node:path";

for (const file of [".env.local", ".env"]) { try { process.loadEnvFile(file); } catch {} }
const [command, directory] = process.argv.slice(2);
if (!directory || !["migrate", "scan", "import", "verify", "publish"].includes(command)) throw new Error("Usage: node scripts/cooccurrence-bootstrap.mjs migrate|scan|import|verify|publish ABSOLUTE_DIRECTORY");
const root = path.resolve(directory);
if (!path.isAbsolute(directory) || !path.relative(process.cwd(), root).startsWith("..")) throw new Error("Use an absolute artifact directory outside this repository");
fs.mkdirSync(root, { recursive: true });
const database = process.env.MY9_COOCCURRENCE_DB_ID;
if (!database) throw new Error("Set MY9_COOCCURRENCE_DB_ID explicitly");
const endpoint = `https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${database}/query`;
const readToken = process.env.MY9_SQL_API_TOKEN;
const writeToken = process.env.MY9_SQL_MIGRATION_TOKEN;
const manifestPath = path.join(root, "snapshot.json");
const ledgerPath = path.join(root, "usage.ndjson");
let totalRead = 0, totalWritten = 0, meteredThrough = 0;
if (fs.existsSync(ledgerPath)) for (const line of fs.readFileSync(ledgerPath, "utf8").trim().split("\n")) {
  if (!line) continue;
  const item = JSON.parse(line); totalRead += item.rows_read ?? 0; totalWritten += item.rows_written ?? 0;
  meteredThrough = Math.max(meteredThrough, item.through ?? 0);
}
function meter(meta, operation) {
  totalRead += meta.rows_read ?? 0; totalWritten += meta.rows_written ?? 0;
  fs.appendFileSync(ledgerPath, JSON.stringify({ at: Date.now(), operation, ...meta }) + "\n");
}
function budget() {
  if (totalRead >= 1_950_000 || totalWritten >= 590_000) throw new Error("Initialization budget guard reached; stop and review usage ledger");
}
async function query(sql, params = [], write = false) {
  budget();
  const token = write ? writeToken : readToken;
  if (!token) throw new Error(write ? "Set the temporary MY9_SQL_MIGRATION_TOKEN" : "Set MY9_SQL_API_TOKEN for reads");
  const response = await fetch(endpoint, { method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ sql, params }), signal: AbortSignal.timeout(60_000) });
  const json = await response.json();
  if (!response.ok || !json.success) throw new Error(`D1 ${response.status}: ${JSON.stringify(json.errors)}`);
  const result = json.result[0];
  meter(result.meta ?? {}, sql.slice(0, 55));
  return result.results;
}
function save(file, value) {
  fs.writeFileSync(file + ".tmp", JSON.stringify(value));
  fs.renameSync(file + ".tmp", file);
}
let manifest = fs.existsSync(manifestPath) ? JSON.parse(fs.readFileSync(manifestPath, "utf8")) : null;
if (manifest && manifest.database !== database) throw new Error("Artifact/database mismatch");
const projection = `json_array(${Array.from({ length: 9 }, (_, i) => `json_extract(hot_payload, '$[${i}].sid')`).join(",")})`;

if (command === "migrate") {
  const name = "0004_cooccurrence.sql";
  const existing = await query("SELECT name FROM d1_migrations WHERE name = ?", [name]);
  if (existing.length) {
    console.log("Cooccurrence migration already applied");
  } else {
    if (!writeToken) throw new Error("Set the temporary MY9_SQL_MIGRATION_TOKEN");
    const { unstable_splitSqlQuery } = await import("wrangler");
    // Normalize comments/newlines while preserving complete compound triggers.
    const statements = unstable_splitSqlQuery(fs.readFileSync(path.resolve("migrations/d1", name), "utf8"))
      .map((sql) => ({ sql, params: [] }));
    statements.push({ sql: `INSERT INTO d1_migrations (name) VALUES ('${name}')`, params: [] });
    const response = await fetch(endpoint, { method: "POST", headers: { Authorization: `Bearer ${writeToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ sql: statements.map(({ sql }) => sql.replace(/\s+/g, " ")).join(";\n") + ";", params: [] }), signal: AbortSignal.timeout(60_000) });
    const data = await response.json();
    if (!response.ok || !data.success) throw new Error(`Migration ${response.status}: ${JSON.stringify(data.errors)}`);
    for (const result of data.result) meter(result.meta ?? {}, "migration " + name);
    console.log(JSON.stringify({ migration: name, statements: statements.length, totalRead, totalWritten }));
  }
} else if (command === "scan") {
  if (!manifest) {
    const [snapshot] = await query(`SELECT valid, ready, baseline_seq,
      COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'my9_cooccurrence_event_v1'), 0) AS h0,
      COALESCE((SELECT MAX(rowid) FROM my9_share_registry_v2), 0) AS upper,
      unixepoch() * 1000 AS at FROM my9_cooccurrence_control_v1 WHERE id = 1`);
    if (!snapshot.valid || snapshot.ready || snapshot.baseline_seq !== null) throw new Error("Database already initialized or baseline is owned by another run");
    manifest = { database, ...snapshot, cursor: 0, pages: 0, shares: 0 };
    save(manifestPath, manifest);
  }
  await query(`UPDATE my9_cooccurrence_control_v1 SET baseline_seq = ?, cursor = ?
    WHERE id = 1 AND ready = 0 AND valid = 1 AND baseline_seq IS NULL`, [manifest.h0, manifest.h0], true);
  const [control] = await query("SELECT valid, baseline_seq FROM my9_cooccurrence_control_v1 WHERE id = 1");
  if (!control.valid || control.baseline_seq !== manifest.h0) throw new Error("Baseline ownership lost");
  fs.mkdirSync(path.join(root, "pages"), { recursive: true });
  while (manifest.cursor < manifest.upper) {
    const file = path.join(root, "pages", String(manifest.pages).padStart(6, "0") + ".json");
    const rows = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : await query(`
      SELECT rowid AS position, share_id, kind, ${projection} AS ids
      FROM my9_share_registry_v2 WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT 4000`, [manifest.cursor, manifest.upper]);
    if (!rows.length) { manifest.cursor = manifest.upper; save(manifestPath, manifest); break; }
    save(file, rows);
    manifest.cursor = rows.at(-1).position; manifest.pages++; manifest.shares += rows.length;
    save(manifestPath, manifest);
    console.log(JSON.stringify({ pages: manifest.pages, shares: manifest.shares, totalRead, totalWritten }));
  }
  if (manifest.h1 === undefined) {
    const [end] = await query(`SELECT valid, COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'my9_cooccurrence_event_v1'), 0) AS h1
      FROM my9_cooccurrence_control_v1 WHERE id = 1`);
    if (!end.valid) throw new Error("Snapshot invalidated by an untracked conflict");
    manifest.h1 = end.h1; manifest.eventCursor = manifest.h0; save(manifestPath, manifest);
  }
  fs.mkdirSync(path.join(root, "events"), { recursive: true });
  while (manifest.eventCursor < manifest.h1) {
    const rows = await query(`SELECT seq, old_share_id, new_share_id, old_kind, new_kind, old_ids, new_ids
      FROM my9_cooccurrence_event_v1 WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT 2000`, [manifest.eventCursor, manifest.h1]);
    if (!rows.length) throw new Error("Missing snapshot correction events");
    save(path.join(root, "events", String(manifest.eventCursor).padStart(12, "0") + ".json"), rows);
    manifest.eventCursor = rows.at(-1).seq; save(manifestPath, manifest);
  }
  manifest.scanComplete = true; save(manifestPath, manifest);
  console.log("Scan complete. Run python scripts/cooccurrence-aggregate.py with the same directory.");
} else if (command === "import") {
  const stats = JSON.parse(fs.readFileSync(path.join(root, "aggregate.json"), "utf8"));
  const url = process.env.MY9_COOCCURRENCE_IMPORT_URL;
  const secret = process.env.MY9_COOCCURRENCE_IMPORT_TOKEN;
  if (!url?.startsWith("https://") || !secret || stats.baseline !== manifest?.h0) throw new Error("Missing importer configuration or baseline mismatch");
  const headers = { Authorization: `Bearer ${secret}`, "Content-Type": "application/json" };
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(60_000) });
  const state = await response.json();
  if (!response.ok || !state.valid || state.ready || state.baseline_seq !== manifest.h0) throw new Error("Importer state mismatch");
  const batches = fs.readdirSync(path.join(root, "import")).sort().map((file) => ({
    file, batch: JSON.parse(fs.readFileSync(path.join(root, "import", file), "utf8")),
  }));
  for (const { file, batch } of batches) {
    const through = batch.rows.at(-1).id;
    if (through > meteredThrough && through <= state.imported_id) {
      // A committed response can be lost on interruption. Reserve its measured
      // per-row cost rather than silently undercounting the resumed run.
      meter({ through, rows_read: 2 * batch.rows.length + 3, rows_written: 2 * batch.rows.length + 1, estimatedAfterLostResponse: true }, "import " + file);
    }
  }
  const pending = batches.filter(({ batch }) => batch.rows.at(-1).id > state.imported_id);
  for (let start = 0; start < pending.length; start += 5) {
    const chunk = pending.slice(start, start + 5);
    budget();
    const result = await fetch(`${url.replace(/\/$/, "")}/import-batches`, { method: "POST", headers,
      body: JSON.stringify(chunk.map(({ batch }) => batch)), signal: AbortSignal.timeout(60_000) });
    const json = await result.json();
    for (const [index, item] of (json.results ?? []).entries()) meter(item, "import " + chunk[index].file);
    if (!result.ok) throw new Error(JSON.stringify(json));
    console.log(JSON.stringify({ through: json.results.at(-1).through, totalRead, totalWritten }));
  }
} else if (command === "verify") {
  const stats = JSON.parse(fs.readFileSync(path.join(root, "aggregate.json"), "utf8"));
  const [row] = await query(`SELECT COUNT(*) AS subjects, SUM(matched) AS matched, SUM(length(counts)) AS bytes,
    MAX(length(counts)) AS maxBytes, MIN(applied_seq) AS minSeq, MAX(applied_seq) AS maxSeq,
    SUM(ready) AS ready, MAX(id) AS maxId FROM my9_cooccurrence_subject_v1`);
  for (const key of ["subjects", "matched", "bytes", "maxBytes"]) if (row[key] !== stats[key]) throw new Error(`Verification mismatch: ${key}`);
  if (row.minSeq !== manifest.h0 || row.maxSeq !== manifest.h0 || row.ready !== row.subjects || row.maxId !== row.subjects) throw new Error("Incomplete import");
  const [valid] = await query("SELECT valid, ready, imported_id FROM my9_cooccurrence_control_v1 WHERE id = 1");
  if (!valid.valid || valid.ready || valid.imported_id !== row.subjects) throw new Error("Import control mismatch");
  save(path.join(root, "verified.json"), { ...row, baseline: manifest.h0, verifiedAt: Date.now(), totalRead, totalWritten });
  console.log(JSON.stringify({ ...row, totalRead, totalWritten }));
} else {
  const verification = JSON.parse(fs.readFileSync(path.join(root, "verified.json"), "utf8"));
  if (verification.baseline !== manifest.h0) throw new Error("No verified baseline");
  await query(`UPDATE my9_cooccurrence_control_v1 SET ready = 1, updated_at = ?, last_day = 0
    WHERE id = 1 AND ready = 0 AND valid = 1 AND baseline_seq = ? AND imported_id = ?`,
    [manifest.at, manifest.h0, verification.subjects], true);
  const [state] = await query("SELECT ready, valid, cursor FROM my9_cooccurrence_control_v1 WHERE id = 1");
  if (!state.ready || !state.valid) throw new Error("Publication rejected");
  console.log(JSON.stringify({ ...state, totalRead, totalWritten }));
}
