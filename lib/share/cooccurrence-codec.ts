// Version 1: little-endian (uint32 subject ID, uint32 share count) pairs.
export type Counter = [number, number];
const UINT32_MAX = 0xffffffff;

export function checkedCount(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0 || value > UINT32_MAX) {
    throw new Error("Cooccurrence v1 counter overflow or underflow");
  }
  return value;
}

export function unpackCounters(raw: ArrayBuffer | number[]): Counter[] {
  const bytes = raw instanceof ArrayBuffer ? raw : Uint8Array.from(raw).buffer;
  if (bytes.byteLength % 8) throw new Error("Invalid cooccurrence v1 vector");
  const view = new DataView(bytes);
  const result: Counter[] = [];
  for (let offset = 0; offset < bytes.byteLength; offset += 8) {
    const id = view.getUint32(offset, true);
    const count = view.getUint32(offset + 4, true);
    if (!id || !count) throw new Error("Invalid cooccurrence v1 entry");
    result.push([id, count]);
  }
  return result;
}

export function packCounters(entries: Counter[]): ArrayBuffer {
  const buffer = new ArrayBuffer(entries.length * 8);
  const view = new DataView(buffer);
  entries.forEach(([id, count], index) => {
    if (!checkedCount(id) || !checkedCount(count)) throw new Error("Invalid cooccurrence v1 entry");
    view.setUint32(index * 8, id, true);
    view.setUint32(index * 8 + 4, count, true);
  });
  // Leave room for the other columns within D1's 2 MB row limit.
  if (buffer.byteLength > 1_900_000) throw new Error("Cooccurrence vector requires schema upgrade");
  return buffer;
}

export function mergeCounters(raw: ArrayBuffer | number[], delta: Counter[]) {
  const counts = new Map(unpackCounters(raw));
  for (const [id, change] of delta) {
    const count = checkedCount((counts.get(id) ?? 0) + change);
    if (count) counts.set(id, count);
    else counts.delete(id);
  }
  const entries = [...counts].sort((a, b) => a[0] - b[0]);
  const top: Counter[] = [];
  for (const entry of entries) {
    const position = top.findIndex(([id, count]) => count < entry[1] || (count === entry[1] && id > entry[0]));
    if (position >= 0) top.splice(position, 0, entry);
    else if (top.length < 10) top.push(entry);
    if (top.length > 10) top.pop();
  }
  return { counts: packCounters(entries), top10: packCounters(top) };
}

export function parseSubjectSet(raw: string): string[] {
  const values: unknown = JSON.parse(raw);
  if (!Array.isArray(values) || values.length > 9) throw new Error("Invalid share subject set");
  return [...new Set(values.flatMap((value) => {
    if (value === null) return [];
    if (typeof value !== "string" || !value.trim() || value.length > 512) throw new Error("Invalid subject ID");
    return [value.trim()];
  }))];
}
