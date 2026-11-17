import { env } from "cloudflare:workers";
import { MAX_BATCH_SIZE, chunkArray } from "@/lib/share/storage-common";

export type D1Scalar = string | number | null | ArrayBuffer;

export type D1PreparedStatementLike = {
  bind: (...values: D1Scalar[]) => D1PreparedStatementLike;
  all: <T = Record<string, unknown>>() => Promise<{ results?: T[] } | T[]>;
  run: () => Promise<{ meta?: { changes?: number } } | { changes?: number } | unknown>;
};

export type D1DatabaseLike = {
  prepare: (query: string) => D1PreparedStatementLike;
  batch: (statements: D1PreparedStatementLike[]) => Promise<unknown[]>;
  exec: (query: string) => Promise<unknown>;
};

export type StatementInput = {
  sql: string;
  params?: D1Scalar[];
};

export function getD1Database(): D1DatabaseLike {
  return env.MY9_DB;
}

export function buildPlaceholders(count: number): string {
  return Array.from({ length: count }, () => "?").join(", ");
}

export async function queryAll<T = Record<string, unknown>>(
  db: D1DatabaseLike,
  sql: string,
  params: D1Scalar[] = []
): Promise<T[]> {
  const prepared = db.prepare(sql).bind(...params.map((value) => (value === undefined ? null : value)));
  const result = await prepared.all<T>();
  if (Array.isArray(result)) {
    return result;
  }
  return Array.isArray(result.results) ? result.results : [];
}

export async function queryFirst<T = Record<string, unknown>>(
  db: D1DatabaseLike,
  sql: string,
  params: D1Scalar[] = []
): Promise<T | null> {
  const rows = await queryAll<T>(db, sql, params);
  return rows[0] ?? null;
}

export function readChangeCount(result: unknown): number {
  if (!result || typeof result !== "object") return 0;
  const record = result as { meta?: { changes?: number }; changes?: number };
  if (typeof record.meta?.changes === "number") return Math.trunc(record.meta.changes);
  if (typeof record.changes === "number") return Math.trunc(record.changes);
  return 0;
}

export async function execute(db: D1DatabaseLike, sql: string, params: D1Scalar[] = []): Promise<number> {
  const result = await db.prepare(sql).bind(...params.map((value) => (value === undefined ? null : value))).run();
  return readChangeCount(result);
}

export async function executeBatch(db: D1DatabaseLike, statements: StatementInput[]): Promise<number> {
  let changes = 0;
  for (const chunk of chunkArray(statements, MAX_BATCH_SIZE)) {
    const result = await db.batch(chunk.map((statement) => db.prepare(statement.sql).bind(...(statement.params ?? []))));
    for (const item of result) {
      changes += readChangeCount(item);
    }
  }
  return changes;
}
