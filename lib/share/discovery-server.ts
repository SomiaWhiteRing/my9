import { SUBJECT_KIND_ORDER, parseSubjectKind } from "@/lib/subject-kind";
import type { ShareDiscoveryItem, ShareDiscoveryPage } from "@/lib/share/discovery";
import { execute, getD1Database, queryAll, queryFirst } from "@/lib/share/storage-d1-runtime";
import {
  DISCOVERY_PAGE_SIZE, DISCOVERY_SNAPSHOT_KEY, VIEW_ROLLUP_KEY,
  readDiscoverySnapshotSql, readViewWatermarkSql, refreshDiscoverySnapshotSql, searchDiscoverySql,
} from "@/lib/share/discovery-queries";

type Cursor = { name: string; createdAt: number; shareId: string };
type SearchRow = { share_id: string; kind: string; creator_name: string; created_at: number; view_count: number };

function validItem(value: unknown): value is ShareDiscoveryItem {
  if (!value || typeof value !== "object") return false;
  const item = value as ShareDiscoveryItem;
  return typeof item.shareId === "string" && /^[a-f0-9]{16}$/.test(item.shareId) && !!parseSubjectKind(item.kind) &&
    (item.creatorName === null || typeof item.creatorName === "string") &&
    Number.isSafeInteger(item.createdAt) && item.createdAt > 0 && item.createdAt <= 8.64e15 &&
    Number.isSafeInteger(item.viewCount) && item.viewCount >= 0;
}

function decodeCursor(raw: string, name: string): Cursor | null {
  if (raw.length > 512 || !/^[A-Za-z0-9_-]+$/.test(raw)) return null;
  try {
    const value = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")) as Cursor;
    return value && value.name === name && Number.isSafeInteger(value.createdAt) && value.createdAt > 0 &&
      value.createdAt <= 8.64e15 && typeof value.shareId === "string" && /^[a-f0-9]{16}$/.test(value.shareId)
      ? value : null;
  } catch {
    return null;
  }
}

function viewsThrough(rolledThrough: number) {
  if (!Number.isSafeInteger(rolledThrough) || rolledThrough <= 0 || rolledThrough > 8.64e15 - 8 * 3600_000) {
    throw new Error("Share view watermark unavailable");
  }
  return new Date(rolledThrough - 1 + 8 * 3600_000).toISOString().slice(0, 10);
}

export async function refreshShareDiscoverySnapshot() {
  const db = await getD1Database();
  if (!db) throw new Error("Share discovery database unavailable");
  // One statement publishes the list and its view watermark together. No request
  // path calls this function, so a missing snapshot cannot cause a refresh stampede.
  const changes = await execute(db, refreshDiscoverySnapshotSql(SUBJECT_KIND_ORDER.length),
    [...SUBJECT_KIND_ORDER, VIEW_ROLLUP_KEY, DISCOVERY_SNAPSHOT_KEY]);
  if (!changes) throw new Error("Share discovery snapshot requires a completed view rollup");
}

export async function handleShareDiscoveryRequest(request: Request) {
  const params = new URL(request.url).searchParams;
  const name = (params.get("query") ?? "").trim();
  const rawCursor = params.get("cursor");
  const cursor = rawCursor ? decodeCursor(rawCursor, name) : null;
  const headers = { "Cache-Control": "no-store" };
  if (name.length > 40 || (rawCursor !== null && (!name || !cursor))) {
    return Response.json({ error: "无效的名称或分页参数。" }, { status: 400, headers });
  }
  try {
    const db = await getD1Database();
    if (!db) throw new Error("Share discovery database unavailable");
    if (!name) {
      const row = await queryFirst<{ payload: string }>(db, readDiscoverySnapshotSql, [DISCOVERY_SNAPSHOT_KEY]);
      const snapshot = row ? JSON.parse(row.payload) as ShareDiscoveryPage & { version: number } : null;
      if (!snapshot || snapshot.version !== 1 || !Array.isArray(snapshot.items) ||
          snapshot.items.length > DISCOVERY_PAGE_SIZE || !snapshot.items.every(validItem) ||
          typeof snapshot.viewsThrough !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(snapshot.viewsThrough)) {
        throw new Error("Share discovery snapshot unavailable");
      }
      return Response.json({ items: snapshot.items, nextCursor: null, viewsThrough: snapshot.viewsThrough }, { headers });
    }
    const rows = await queryAll<SearchRow>(db, searchDiscoverySql(!!cursor),
      cursor ? [name, cursor.createdAt, cursor.shareId] : [name]);
    const watermark = await queryFirst<{ rolled_through: number }>(db, readViewWatermarkSql, [VIEW_ROLLUP_KEY]);
    const items = rows.slice(0, DISCOVERY_PAGE_SIZE).map(row => ({
      shareId: row.share_id, kind: parseSubjectKind(row.kind)!, creatorName: row.creator_name,
      createdAt: row.created_at, viewCount: row.view_count,
    }));
    if (!items.every(validItem)) throw new Error("Invalid share discovery row");
    const last = items.at(-1);
    const nextCursor = rows.length > DISCOVERY_PAGE_SIZE && last
      ? Buffer.from(JSON.stringify({ name, createdAt: last.createdAt, shareId: last.shareId })).toString("base64url") : null;
    return Response.json({ items, nextCursor, viewsThrough: viewsThrough(watermark?.rolled_through ?? 0) }, { headers });
  } catch (error) {
    console.error("[share-discovery] read failed", error);
    return Response.json({ error: "暂时无法读取构成，请稍后重试。" }, {
      status: 503, headers: { ...headers, "Retry-After": "60" },
    });
  }
}
