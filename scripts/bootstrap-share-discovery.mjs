#!/usr/bin/env node
// Fixed-scope production/test operations. Keep metering outside the repository.
import fs from "node:fs";
import path from "node:path";
import ts from "typescript";

for (const file of [".env.local", ".env"]) { try { process.loadEnvFile(file); } catch {} }
const [operation, environment, ledgerDirectory] = process.argv.slice(2);
if (!["migrate", "refresh", "inspect"].includes(operation) || !["production", "test"].includes(environment) ||
    !ledgerDirectory || !path.isAbsolute(ledgerDirectory) || !path.relative(process.cwd(), ledgerDirectory).startsWith("..")) {
  throw new Error("Usage: node scripts/bootstrap-share-discovery.mjs migrate|refresh|inspect production|test ABSOLUTE_LEDGER_DIRECTORY_OUTSIDE_REPO");
}
fs.mkdirSync(ledgerDirectory, { recursive: true });
const ledgerPath = path.join(ledgerDirectory, `${environment}-usage.ndjson`);
const config = JSON.parse(fs.readFileSync("wrangler.jsonc", "utf8"));
const database = (environment === "test" ? config.env.test : config).d1_databases.find(db => db.binding === "MY9_DB");
const endpoint = `https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${database.database_id}/query`;
async function sourceModule(file) {
  const output = ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(output).toString("base64")}`);
}
const queries = await sourceModule("lib/share/discovery-queries.ts");
const { SUBJECT_KIND_ORDER: kinds } = await sourceModule("lib/subject-kind.ts");
const usage = { rowsRead: 0, rowsWritten: 0 };
if (fs.existsSync(ledgerPath)) for (const line of fs.readFileSync(ledgerPath, "utf8").trim().split("\n")) {
  if (!line) continue;
  const record = JSON.parse(line);
  if (record.database !== database.database_id) throw new Error("Ledger database mismatch");
  usage.rowsRead += record.meta?.rows_read ?? 0;
  usage.rowsWritten += record.meta?.rows_written ?? 0;
}
async function query(label, sql, params = [], write = false) {
  if (usage.rowsRead >= 1_950_000 || usage.rowsWritten >= 990_000) throw new Error("Initialization budget reached; review the ledger before continuing");
  const token = write ? process.env.MY9_SQL_MIGRATION_TOKEN : process.env.MY9_SQL_API_TOKEN;
  if (!token) throw new Error(write ? "Temporary MY9_SQL_MIGRATION_TOKEN is required" : "MY9_SQL_API_TOKEN is required");
  const response = await fetch(endpoint, {
    method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ sql, params }), signal: AbortSignal.timeout(60_000),
  });
  const data = await response.json();
  if (!response.ok || !data.success) throw new Error(`${label}: ${response.status} ${JSON.stringify(data.errors)}`);
  for (const result of data.result) {
    const record = { at: new Date().toISOString(), database: database.database_id, label, meta: result.meta };
    fs.appendFileSync(ledgerPath, JSON.stringify(record) + "\n");
    usage.rowsRead += result.meta?.rows_read ?? 0;
    usage.rowsWritten += result.meta?.rows_written ?? 0;
    console.log(JSON.stringify(record));
  }
  return data.result.flatMap(result => result.results ?? []);
}

if (operation === "migrate") {
  const migration = "0005_share_discovery.sql";
  const applied = await query("migration-status", "SELECT name FROM d1_migrations WHERE name = ?", [migration]);
  const index = await query("index-status", "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?", [queries.DISCOVERY_INDEX]);
  const ddl = fs.readFileSync(path.join("migrations/d1", migration), "utf8").trim();
  const normalizeSql = value => value.replace(/\s+/g, " ").replace(/;$/, "").trim();
  if (index.length && normalizeSql(index[0].sql) !== normalizeSql(ddl)) throw new Error("Existing discovery index has an unexpected definition");
  if (applied.length && !index.length) throw new Error("Migration registered without its index");
  if (!applied.length) {
    if (index.length) throw new Error("Index exists without migration record; inspect before resuming");
    // One D1 request installs the index and records the migration together.
    await query("create-index-and-register", `${ddl}\nINSERT INTO d1_migrations (name) VALUES ('${migration}');`, [], true);
  }
  console.log(JSON.stringify({ migration, alreadyApplied: !!applied.length, usage }));
} else if (operation === "refresh") {
  await query("refresh-snapshot", queries.refreshDiscoverySnapshotSql(kinds.length),
    [...kinds, queries.VIEW_ROLLUP_KEY, queries.DISCOVERY_SNAPSHOT_KEY], true);
  const rows = await query("snapshot-check", queries.readDiscoverySnapshotSql, [queries.DISCOVERY_SNAPSHOT_KEY]);
  if (!rows.length) throw new Error("Snapshot missing; a completed view rollup is required");
  const snapshot = JSON.parse(rows[0].payload);
  console.log(JSON.stringify({ items: snapshot.items.length, viewsThrough: snapshot.viewsThrough, bytes: Buffer.byteLength(rows[0].payload), usage }));
} else {
  // Do not re-scan all index entries: CREATE INDEX's D1 read metering already
  // includes internal work as well as the source scan. Keep validation bounded.
  const info = await query("index-definition", "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?", [queries.DISCOVERY_INDEX]);
  const rows = await query("snapshot-check", queries.readDiscoverySnapshotSql, [queries.DISCOVERY_SNAPSHOT_KEY]);
  const plan = await query("search-plan", "EXPLAIN QUERY PLAN " + queries.searchDiscoverySql(true), ["阿菜", Date.now(), "ffffffffffffffff"]);
  const snapshot = rows.length ? JSON.parse(rows[0].payload) : null;
  console.log(JSON.stringify({ info, plan, snapshot: snapshot ? { items: snapshot.items.length, viewsThrough: snapshot.viewsThrough, bytes: Buffer.byteLength(rows[0].payload) } : null, usage }));
}
