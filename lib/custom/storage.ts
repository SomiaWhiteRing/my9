import { CustomDraftSnapshot, normalizeCustomEntries } from "@/lib/custom/types";

const DB_NAME = "my9-custom-local";
const DB_VERSION = 1;
const STORE_NAME = "drafts";
const DRAFT_KEY = "custom-draft:v1";

type DraftRecord = {
  key: string;
  value: CustomDraftSnapshot;
};

let dbPromise: Promise<IDBDatabase> | null = null;

function openDatabase(): Promise<IDBDatabase> {
  if (typeof window === "undefined" || !("indexedDB" in window)) {
    return Promise.reject(new Error("indexeddb_unavailable"));
  }

  if (dbPromise) {
    return dbPromise;
  }

  dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
    const request = window.indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "key" });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("indexeddb_open_failed"));
  });

  return dbPromise;
}

function runStoreRequest<T>(
  mode: IDBTransactionMode,
  operation: (store: IDBObjectStore, resolve: (value: T) => void, reject: (error: unknown) => void) => void
): Promise<T> {
  return openDatabase().then((db) => new Promise<T>((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, mode);
    const store = tx.objectStore(STORE_NAME);
    operation(store, resolve, reject);
    tx.onerror = () => reject(tx.error || new Error("indexeddb_tx_failed"));
  }));
}

export async function readCustomDraftSnapshot(): Promise<CustomDraftSnapshot | null> {
  try {
    const record = await runStoreRequest<DraftRecord | null>("readonly", (store, resolve, reject) => {
      const request = store.get(DRAFT_KEY);
      request.onsuccess = () => {
        resolve((request.result as DraftRecord | undefined) ?? null);
      };
      request.onerror = () => reject(request.error || new Error("indexeddb_get_failed"));
    });

    if (!record?.value) {
      return null;
    }

    return {
      entries: normalizeCustomEntries(record.value.entries),
    };
  } catch {
    return null;
  }
}

export async function writeCustomDraftSnapshot(snapshot: CustomDraftSnapshot): Promise<boolean> {
  try {
    await runStoreRequest<void>("readwrite", (store, resolve, reject) => {
      const request = store.put({
        key: DRAFT_KEY,
        value: {
          entries: normalizeCustomEntries(snapshot.entries),
        },
      } satisfies DraftRecord);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error || new Error("indexeddb_put_failed"));
    });
    return true;
  } catch {
    return false;
  }
}
