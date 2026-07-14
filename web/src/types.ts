export type StatusResponse = {
  version?: string;
  fonts: {
    files?: number;
    faces?: number;
    errors?: number;
  };
  subtitles: {
    files?: number;
  };
  jobs: Record<string, number | undefined>;
  backups?: number;
  metrics?: {
    uptime_seconds?: number;
    cache?: {
      hits?: number;
      misses?: number;
      hit_rate_percent?: number;
      files?: number;
      bytes?: number;
      max_bytes?: number;
      evictions?: number;
      evicted_bytes?: number;
    };
    queue?: {
      samples?: number;
      average_ms?: number;
      p50_ms?: number;
      p95_ms?: number;
      max_ms?: number;
    };
    conversions?: {
      started?: number;
      succeeded?: number;
      failed?: number;
      duration?: {
        samples?: number;
        average_ms?: number;
        p50_ms?: number;
        p95_ms?: number;
        max_ms?: number;
      };
    };
    workers?: {
      requests?: number;
      restarts?: number;
    };
  };
  capabilities?: {
    font_subset_map?: boolean;
    draw_table_v27?: boolean;
    strip_embedded?: boolean;
    safe_strip_keeps_unrestorable_fonts?: boolean;
    variable_fonts?: boolean;
  };
  config: {
    auth_required?: boolean;
    font_dirs?: string[];
    watch_dirs?: string[];
    watch_dir_items?: Array<{
      path: string;
      removable?: boolean;
    }>;
    backup_dir?: string;
    data_dir?: string;
    scan_interval_seconds?: number;
    backup_retention_days?: number;
    max_concurrent_jobs?: number;
    max_index_concurrency?: number;
    max_scan_concurrency?: number;
    max_conversion_memory_mb?: number;
    subset_cache_max_mb?: number;
    controls?: {
      scan_paused?: boolean;
      scan_cancel_requested?: boolean;
      conversion_paused?: boolean;
      conversion_cancel_requested?: boolean;
      conversion_parallelism?: number;
      scan_running?: boolean;
      index_running?: boolean;
      scan_progress?: {
        stage?: string;
        current?: number;
        total?: number;
        seen?: number;
        ready?: number;
        queued?: number;
        skipped?: number;
        failed?: number;
        started_at?: string | null;
        updated_at?: string | null;
      };
    };
    options?: Record<string, boolean | undefined>;
  };
};

export type LoginResponse = {
  ok: boolean;
  csrf: string;
};

export type Job = {
  id: number;
  subtitle_id?: number;
  path: string;
  mode?: "subset" | "strip_embedded" | string;
  status: string;
  queued_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  message?: string | null;
  missing_fonts?: unknown;
  stats?: {
    embedded_count?: number;
    missing_count?: number;
    drawing_count?: number;
    embedded_removed_count?: number;
    random_names_restored?: number;
    drawings_restored?: number;
    draw_fonts_created?: number;
    original_size?: number;
    output_size?: number;
  } | null;
};

export type JobsResponse = {
  jobs: Job[];
  next_cursor?: number | null;
};

export type SubtitleFile = {
  id: number;
  path: string;
  root_label: string;
  relative_path: string;
  size: number;
  mtime: number;
  last_status?: string | null;
  last_processed_at?: string | null;
  missing_fonts?: unknown;
  error?: string | null;
  analysis?: {
    drawing_count?: number;
    third_party_fonts?: string[];
    system_fonts?: string[];
    embedded_fonts?: string[];
    char_count?: number;
  } | null;
};

export type FilesResponse = {
  files: SubtitleFile[];
  next_cursor?: number | null;
};

export type FileAnalysisResponse = {
  analysis: NonNullable<SubtitleFile["analysis"]> | null;
  cached?: boolean;
};

export type Backup = {
  id: number;
  subtitle_id?: number | null;
  source_path: string;
  backup_path: string;
  source_sha256: string;
  created_at: string;
};

export type BackupsResponse = {
  backups: Backup[];
  next_cursor?: number | null;
};

export type EventPayload = {
  ts: string;
  kind: string;
  level: string;
  message: string;
};
