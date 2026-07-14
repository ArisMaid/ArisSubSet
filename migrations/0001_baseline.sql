CREATE TABLE IF NOT EXISTS font_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  size INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  quick_hash TEXT NOT NULL,
  full_hash TEXT NOT NULL,
  format TEXT NOT NULL,
  status TEXT NOT NULL,
  error TEXT,
  indexed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS font_faces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id INTEGER NOT NULL REFERENCES font_files(id) ON DELETE CASCADE,
  ttc_index INTEGER NOT NULL,
  family TEXT,
  full_name TEXT,
  postscript_name TEXT,
  subfamily TEXT,
  version TEXT,
  weight INTEGER NOT NULL DEFAULT 400,
  italic INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS font_names (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  face_id INTEGER NOT NULL REFERENCES font_faces(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  normalized TEXT NOT NULL,
  kind TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_font_names_norm ON font_names(normalized);
CREATE INDEX IF NOT EXISTS idx_font_names_face ON font_names(face_id);
CREATE INDEX IF NOT EXISTS idx_font_faces_file ON font_faces(file_id);

CREATE TABLE IF NOT EXISTS subtitle_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  root_label TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  size INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  last_config_hash TEXT,
  last_status TEXT,
  last_processed_at TEXT,
  missing_fonts TEXT,
  error TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subtitle_id INTEGER NOT NULL REFERENCES subtitle_files(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'subset',
  status TEXT NOT NULL,
  queued_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  message TEXT,
  missing_fonts TEXT,
  stats TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_mode ON jobs(mode);

CREATE TABLE IF NOT EXISTS backups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subtitle_id INTEGER,
  source_path TEXT NOT NULL,
  backup_path TEXT NOT NULL UNIQUE,
  source_sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watch_dirs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
