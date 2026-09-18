-- Empty tables only: no scan or index of historical registry/slot rows.
-- Apply as one transaction. Install the log BEFORE taking the baseline.
CREATE TABLE my9_cooccurrence_subject_v1 (
  id INTEGER PRIMARY KEY CHECK(id > 0 AND id <= 4294967295),
  kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  ready INTEGER NOT NULL DEFAULT 0 CHECK(ready IN (0,1)),
  applied_seq INTEGER NOT NULL DEFAULT 0,
  matched INTEGER NOT NULL DEFAULT 0 CHECK(matched >= 0 AND matched <= 4294967295),
  counts BLOB NOT NULL DEFAULT X'',
  top10 BLOB NOT NULL DEFAULT X'',
  updated_at INTEGER NOT NULL DEFAULT 0,
  UNIQUE(kind, subject_id)
);
CREATE TABLE my9_cooccurrence_event_v1 (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  old_share_id TEXT,
  new_share_id TEXT,
  old_kind TEXT,
  new_kind TEXT,
  old_ids TEXT NOT NULL,
  new_ids TEXT NOT NULL,
  recorded_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000)
);
CREATE TABLE my9_cooccurrence_work_v1 (
  id INTEGER PRIMARY KEY,
  window_end INTEGER NOT NULL,
  matched_delta INTEGER NOT NULL,
  delta TEXT NOT NULL
);
CREATE TABLE my9_cooccurrence_control_v1 (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  ready INTEGER NOT NULL DEFAULT 0,
  valid INTEGER NOT NULL DEFAULT 1,
  baseline_seq INTEGER,
  cursor INTEGER NOT NULL DEFAULT 0,
  window_end INTEGER NOT NULL DEFAULT 0,
  window_at INTEGER NOT NULL DEFAULT 0,
  target_seq INTEGER NOT NULL DEFAULT 0,
  phase TEXT NOT NULL DEFAULT 'idle' CHECK(phase IN ('idle','prepare','apply')),
  lease TEXT,
  lease_until INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0,
  last_day INTEGER NOT NULL DEFAULT 0,
  imported_id INTEGER NOT NULL DEFAULT 0,
  imported_hash TEXT
);
INSERT INTO my9_cooccurrence_control_v1 (id) VALUES (1);

-- REPLACE can delete conflicts without firing DELETE triggers. Fail closed.
-- Ordinary rejected inserts roll this change back with their transaction.
CREATE TRIGGER my9_cooccurrence_insert_guard_v1
BEFORE INSERT ON my9_share_registry_v2
WHEN EXISTS (SELECT 1 FROM my9_share_registry_v2
  WHERE share_id = NEW.share_id OR content_hash = NEW.content_hash)
BEGIN
  UPDATE my9_cooccurrence_control_v1 SET valid = 0 WHERE id = 1 AND valid = 1;
END;
CREATE TRIGGER my9_cooccurrence_update_guard_v1
BEFORE UPDATE OF rowid, share_id, content_hash ON my9_share_registry_v2
WHEN OLD.rowid != NEW.rowid OR EXISTS (SELECT 1 FROM my9_share_registry_v2
  WHERE (share_id = NEW.share_id OR content_hash = NEW.content_hash) AND rowid != OLD.rowid)
BEGIN
  UPDATE my9_cooccurrence_control_v1 SET valid = 0 WHERE id = 1 AND valid = 1;
END;

CREATE TRIGGER my9_cooccurrence_insert_v1 AFTER INSERT ON my9_share_registry_v2
BEGIN
  INSERT INTO my9_cooccurrence_event_v1 (new_share_id, new_kind, old_ids, new_ids)
  VALUES (NEW.share_id, NEW.kind, '[]', json_array(json_extract(NEW.hot_payload, '$[0].sid'), json_extract(NEW.hot_payload, '$[1].sid'), json_extract(NEW.hot_payload, '$[2].sid'), json_extract(NEW.hot_payload, '$[3].sid'), json_extract(NEW.hot_payload, '$[4].sid'), json_extract(NEW.hot_payload, '$[5].sid'), json_extract(NEW.hot_payload, '$[6].sid'), json_extract(NEW.hot_payload, '$[7].sid'), json_extract(NEW.hot_payload, '$[8].sid')));
END;
CREATE TRIGGER my9_cooccurrence_delete_v1 AFTER DELETE ON my9_share_registry_v2
BEGIN
  INSERT INTO my9_cooccurrence_event_v1 (old_share_id, old_kind, old_ids, new_ids)
  VALUES (OLD.share_id, OLD.kind, json_array(json_extract(OLD.hot_payload, '$[0].sid'), json_extract(OLD.hot_payload, '$[1].sid'), json_extract(OLD.hot_payload, '$[2].sid'), json_extract(OLD.hot_payload, '$[3].sid'), json_extract(OLD.hot_payload, '$[4].sid'), json_extract(OLD.hot_payload, '$[5].sid'), json_extract(OLD.hot_payload, '$[6].sid'), json_extract(OLD.hot_payload, '$[7].sid'), json_extract(OLD.hot_payload, '$[8].sid')), '[]');
END;
CREATE TRIGGER my9_cooccurrence_update_v1
AFTER UPDATE OF share_id, kind, hot_payload ON my9_share_registry_v2
WHEN OLD.share_id != NEW.share_id OR OLD.kind != NEW.kind OR json_array(json_extract(OLD.hot_payload, '$[0].sid'), json_extract(OLD.hot_payload, '$[1].sid'), json_extract(OLD.hot_payload, '$[2].sid'), json_extract(OLD.hot_payload, '$[3].sid'), json_extract(OLD.hot_payload, '$[4].sid'), json_extract(OLD.hot_payload, '$[5].sid'), json_extract(OLD.hot_payload, '$[6].sid'), json_extract(OLD.hot_payload, '$[7].sid'), json_extract(OLD.hot_payload, '$[8].sid')) != json_array(json_extract(NEW.hot_payload, '$[0].sid'), json_extract(NEW.hot_payload, '$[1].sid'), json_extract(NEW.hot_payload, '$[2].sid'), json_extract(NEW.hot_payload, '$[3].sid'), json_extract(NEW.hot_payload, '$[4].sid'), json_extract(NEW.hot_payload, '$[5].sid'), json_extract(NEW.hot_payload, '$[6].sid'), json_extract(NEW.hot_payload, '$[7].sid'), json_extract(NEW.hot_payload, '$[8].sid'))
BEGIN
  INSERT INTO my9_cooccurrence_event_v1 (old_share_id, new_share_id, old_kind, new_kind, old_ids, new_ids)
  VALUES (OLD.share_id, NEW.share_id, OLD.kind, NEW.kind, json_array(json_extract(OLD.hot_payload, '$[0].sid'), json_extract(OLD.hot_payload, '$[1].sid'), json_extract(OLD.hot_payload, '$[2].sid'), json_extract(OLD.hot_payload, '$[3].sid'), json_extract(OLD.hot_payload, '$[4].sid'), json_extract(OLD.hot_payload, '$[5].sid'), json_extract(OLD.hot_payload, '$[6].sid'), json_extract(OLD.hot_payload, '$[7].sid'), json_extract(OLD.hot_payload, '$[8].sid')), json_array(json_extract(NEW.hot_payload, '$[0].sid'), json_extract(NEW.hot_payload, '$[1].sid'), json_extract(NEW.hot_payload, '$[2].sid'), json_extract(NEW.hot_payload, '$[3].sid'), json_extract(NEW.hot_payload, '$[4].sid'), json_extract(NEW.hot_payload, '$[5].sid'), json_extract(NEW.hot_payload, '$[6].sid'), json_extract(NEW.hot_payload, '$[7].sid'), json_extract(NEW.hot_payload, '$[8].sid')));
END;
