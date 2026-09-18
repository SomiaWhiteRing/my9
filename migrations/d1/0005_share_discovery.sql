CREATE INDEX my9_share_registry_v2_creator_created_share_idx
ON my9_share_registry_v2 (creator_name, created_at DESC, share_id DESC)
WHERE creator_name IS NOT NULL AND creator_name <> '';
