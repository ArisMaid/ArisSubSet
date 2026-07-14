CREATE INDEX IF NOT EXISTS idx_subtitle_files_status_id
  ON subtitle_files(last_status, id DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status_id
  ON jobs(status, id DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_mode_id
  ON jobs(mode, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_one_active_per_subtitle
  ON jobs(subtitle_id) WHERE status IN ('queued', 'running');
