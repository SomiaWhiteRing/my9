-- Apply this entire file as ONE D1 transaction (Wrangler migrations apply).
-- One covering-index scan of the registry; no rewrite/index of historical rows.
CREATE TABLE my9_share_count_kind_v1 (
  kind TEXT PRIMARY KEY,
  share_count INTEGER NOT NULL CHECK (share_count >= 0)
) WITHOUT ROWID;

CREATE TABLE my9_share_count_state_v1 (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  ready INTEGER NOT NULL CHECK (ready IN (0, 1))
);
INSERT INTO my9_share_count_state_v1 (id, ready) VALUES (1, 0);

-- SQLite REPLACE can silently delete conflicting rows when recursive_triggers
-- is off. Invalidate on conflicts before they happen, regardless of that pragma.
-- A rejected plain INSERT/UPDATE rolls this back. IGNORE/UPSERT/REPLACE may
-- commit the invalidation; readers then use the exact registry query instead.
CREATE TRIGGER my9_share_count_insert_guard_v1
BEFORE INSERT ON my9_share_registry_v2
WHEN EXISTS (
  SELECT 1 FROM my9_share_registry_v2
  WHERE share_id = NEW.share_id OR content_hash = NEW.content_hash
)
BEGIN
  UPDATE my9_share_count_state_v1 SET ready = 0 WHERE id = 1 AND ready = 1;
END;

CREATE TRIGGER my9_share_count_update_guard_v1
BEFORE UPDATE OF share_id, content_hash ON my9_share_registry_v2
WHEN EXISTS (
  SELECT 1 FROM my9_share_registry_v2
  WHERE (share_id = NEW.share_id OR content_hash = NEW.content_hash)
    AND rowid != OLD.rowid
)
BEGIN
  UPDATE my9_share_count_state_v1 SET ready = 0 WHERE id = 1 AND ready = 1;
END;

CREATE TRIGGER my9_share_count_insert_v1
AFTER INSERT ON my9_share_registry_v2
WHEN (SELECT ready FROM my9_share_count_state_v1 WHERE id = 1) = 1
BEGIN
  INSERT INTO my9_share_count_kind_v1 (kind, share_count) VALUES (NEW.kind, 1)
  ON CONFLICT (kind) DO UPDATE SET share_count = share_count + 1;
END;

CREATE TRIGGER my9_share_count_delete_v1
AFTER DELETE ON my9_share_registry_v2
WHEN (SELECT ready FROM my9_share_count_state_v1 WHERE id = 1) = 1
BEGIN
  UPDATE my9_share_count_kind_v1 SET share_count = share_count - 1 WHERE kind = OLD.kind;
END;

CREATE TRIGGER my9_share_count_kind_update_v1
AFTER UPDATE OF kind ON my9_share_registry_v2
WHEN OLD.kind != NEW.kind
  AND (SELECT ready FROM my9_share_count_state_v1 WHERE id = 1) = 1
BEGIN
  UPDATE my9_share_count_kind_v1 SET share_count = share_count - 1 WHERE kind = OLD.kind;
  INSERT INTO my9_share_count_kind_v1 (kind, share_count) VALUES (NEW.kind, 1)
  ON CONFLICT (kind) DO UPDATE SET share_count = share_count + 1;
END;

INSERT INTO my9_share_count_kind_v1 (kind, share_count)
SELECT kind, COUNT(*) FROM my9_share_registry_v2 GROUP BY kind;

UPDATE my9_share_count_state_v1 SET ready = 1 WHERE id = 1;
