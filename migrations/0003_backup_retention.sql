CREATE INDEX IF NOT EXISTS idx_backups_created_at_id
  ON backups(created_at, id);
