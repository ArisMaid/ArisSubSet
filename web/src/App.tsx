import { ChangeEvent, FormEvent, ReactNode, useEffect, useMemo, useRef, useState } from "react";
import {
  ArchiveRestore,
  Clock3,
  CheckCircle2,
  ChevronDown,
  Database,
  Download,
  Eye,
  FileText,
  Filter,
  FolderPlus,
  KeyRound,
  Loader2,
  Pause,
  Play,
  RefreshCw,
  RotateCcw,
  ScanSearch,
  ShieldCheck,
  Sparkles,
  Trash2,
  Undo2,
  Upload,
  X,
} from "lucide-react";
import { apiRequest, ApiError } from "./api";
import type {
  Backup,
  BackupsResponse,
  EventPayload,
  FileAnalysisResponse,
  FilesResponse,
  Job,
  JobsResponse,
  StatusResponse,
  SubtitleFile,
} from "./types";

const CSRF_KEY = "ass-subset-csrf";

const optionLabels: Record<string, string> = {
  embed_external_fonts: "嵌入外部字体",
  embed_system_fonts: "嵌入系统字体",
  include_ascii: "保留 ASCII",
  multi_weight: "多字重",
  randomize_font_names: "随机字体名",
  draw_subset: "绘图字体",
  full_font_embed: "完整嵌入",
  fallback_full_font_embed: "失败回退",
  variable_fonts: "可变字体",
};

const optionTips: Record<string, string> = {
  embed_external_fonts: "把字体库中匹配到的非系统字体嵌入字幕。",
  embed_system_fonts: "是否也嵌入常见系统字体，默认关闭。",
  include_ascii: "子集字体保留 ASCII 字符，提升兼容性。",
  multi_weight: "按粗体、斜体等样式分别选择候选字体。",
  randomize_font_names: "使用随机字体名并写入可还原映射。",
  draw_subset: "把 ASS 绘图转换为可还原的 draw 表字体。",
  full_font_embed: "嵌入完整字体而不是子集，通常不建议开启。",
  fallback_full_font_embed: "子集化失败时自动改用完整字体嵌入。",
  variable_fonts: "可变字体支持，当前默认关闭。",
};

const statusLabels: Record<string, string> = {
  new: "未处理",
  queued: "排队",
  running: "运行中",
  success: "完成",
  partial: "部分完成",
  failed: "失败",
  cancelled: "已取消",
};

const modeLabels: Record<string, string> = {
  subset: "转换",
  strip_embedded: "清理还原",
};

type ActionName = string;

export default function App() {
  const [csrf, setCsrf] = useState(() => localStorage.getItem(CSRF_KEY) ?? "");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [files, setFiles] = useState<SubtitleFile[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [backups, setBackups] = useState<Backup[]>([]);
  const [events, setEvents] = useState<EventPayload[]>([]);
  const [busy, setBusy] = useState<ActionName | "login" | null>(null);
  const [error, setError] = useState("");
  const [jobFilter, setJobFilter] = useState("all");
  const [restoreTarget, setRestoreTarget] = useState<Backup | null>(null);
  const [watchDir, setWatchDir] = useState("");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [scheduleMinutes, setScheduleMinutes] = useState("0");
  const [selectedFileId, setSelectedFileId] = useState<number | null>(null);
  const [fileAnalyses, setFileAnalyses] = useState<Record<number, SubtitleFile["analysis"]>>({});
  const [jobNextCursor, setJobNextCursor] = useState<number | null>(null);
  const [fileNextCursor, setFileNextCursor] = useState<number | null>(null);
  const [backupNextCursor, setBackupNextCursor] = useState<number | null>(null);
  const refreshPromise = useRef<Promise<void> | null>(null);
  const refreshQueued = useRef(false);
  const refreshTimer = useRef<number | null>(null);
  const jobFilterRef = useRef(jobFilter);
  const analysisKeys = useRef<Record<number, string>>({});

  const authRequired = status?.config.auth_required ?? true;
  const loggedIn = !authRequired || Boolean(csrf);
  const recentFile = files[0];
  const selectedFile = files.find((file) => file.id === selectedFileId) ?? recentFile;
  const detailFile = selectedFile
    ? { ...selectedFile, analysis: fileAnalyses[selectedFile.id] }
    : undefined;
  const jobCounts = status?.jobs ?? {};
  const controls = status?.config.controls;
  const conversionParallelism = controls?.conversion_parallelism ?? status?.config.max_concurrent_jobs ?? 1;

  useEffect(() => {
    if (status?.config.scan_interval_seconds !== undefined) {
      setScheduleMinutes(String(Math.round((status.config.scan_interval_seconds ?? 0) / 60)));
    }
  }, [status?.config.scan_interval_seconds]);

  jobFilterRef.current = jobFilter;

  async function refreshOnce(action: ActionName, quiet: boolean) {
    if (!quiet) {
      setBusy(action);
      setError("");
    }
    const nextStatus = await apiRequest<StatusResponse>("/api/status");
    setStatus(nextStatus);
    if (nextStatus.config.auth_required && !csrf) {
      setJobs([]);
      setBackups([]);
      setFiles([]);
      return;
    }
    const activeFilter = jobFilterRef.current;
    const jobParams = new URLSearchParams({ limit: "100" });
    if (activeFilter in statusLabels && activeFilter !== "new") {
      jobParams.set("status", activeFilter);
    } else if (activeFilter in modeLabels) {
      jobParams.set("mode", activeFilter);
    }
    const [nextFiles, nextJobs, nextBackups] = await Promise.all([
      apiRequest<FilesResponse>("/api/files?limit=50"),
      apiRequest<JobsResponse>(`/api/jobs?${jobParams.toString()}`),
      apiRequest<BackupsResponse>("/api/backups?limit=100"),
    ]);
    setFiles(nextFiles.files);
    setSelectedFileId((current) => (
      current && nextFiles.files.some((file) => file.id === current)
        ? current
        : nextFiles.files[0]?.id ?? null
    ));
    setJobs(nextJobs.jobs);
    setJobNextCursor(nextJobs.next_cursor ?? null);
    setFileNextCursor(nextFiles.next_cursor ?? null);
    setBackups(nextBackups.backups);
    setBackupNextCursor(nextBackups.next_cursor ?? null);
  }

  async function loadAll(action: ActionName = "refresh", quiet = false) {
    if (refreshPromise.current) {
      refreshQueued.current = true;
      await refreshPromise.current;
      return;
    }
    const run = async () => {
      do {
        refreshQueued.current = false;
        try {
          await refreshOnce(action, quiet);
        } catch (err) {
          if (err instanceof ApiError && err.status === 401) {
            localStorage.removeItem(CSRF_KEY);
            setCsrf("");
            setError("需要登录后查看服务控制台。");
          } else if (!quiet) {
            setError(readError(err));
          }
        }
      } while (refreshQueued.current);
    };
    refreshPromise.current = run();
    try {
      await refreshPromise.current;
    } finally {
      refreshPromise.current = null;
      if (!quiet) {
        setBusy(null);
      }
    }
  }

  useEffect(() => {
    void loadAll();
    const timer = window.setInterval(() => void loadAll("refresh", true), 15000);
    return () => window.clearInterval(timer);
  }, [csrf, jobFilter]);

  useEffect(() => {
    if (!loggedIn) {
      return;
    }
    const source = new EventSource("/api/events", { withCredentials: true });
    const pushEvent = (event: MessageEvent) => {
      const payload = JSON.parse(event.data) as EventPayload;
      setEvents((items) => [payload, ...items].slice(0, 80));
      if (payload.kind === "scan" && payload.level === "info") {
        if (refreshTimer.current !== null) {
          window.clearTimeout(refreshTimer.current);
        }
        refreshTimer.current = window.setTimeout(() => {
          refreshTimer.current = null;
          void apiRequest<StatusResponse>("/api/status").then(setStatus).catch(() => undefined);
        }, 500);
      } else if (payload.level === "ok" || payload.level === "err") {
        if (refreshTimer.current !== null) {
          window.clearTimeout(refreshTimer.current);
        }
        refreshTimer.current = window.setTimeout(() => {
          refreshTimer.current = null;
          void loadAll("refresh", true);
        }, 750);
      }
    };
    source.onmessage = pushEvent;
    for (const name of ["job", "scan", "index", "backup", "upload", "config"]) {
      source.addEventListener(name, (event) => pushEvent(event as MessageEvent));
    }
    return () => {
      source.close();
      if (refreshTimer.current !== null) {
        window.clearTimeout(refreshTimer.current);
        refreshTimer.current = null;
      }
    };
  }, [loggedIn, jobFilter]);

  useEffect(() => {
    if (!loggedIn || !selectedFile || uploadFile) {
      return;
    }
    const key = `${selectedFile.id}:${selectedFile.size}:${selectedFile.mtime}`;
    if (analysisKeys.current[selectedFile.id] === key) {
      return;
    }
    analysisKeys.current[selectedFile.id] = key;
    let cancelled = false;
    void apiRequest<FileAnalysisResponse>(`/api/files/${selectedFile.id}/analysis`)
      .then((result) => {
        if (!cancelled) {
          setFileAnalyses((items) => ({ ...items, [selectedFile.id]: result.analysis }));
        }
      })
      .catch((err) => {
        delete analysisKeys.current[selectedFile.id];
        if (!cancelled) {
          setError(readError(err));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [loggedIn, selectedFile?.id, selectedFile?.size, selectedFile?.mtime, uploadFile]);

  async function login(event: FormEvent) {
    event.preventDefault();
    setBusy("login");
    setError("");
    try {
      const res = await apiRequest<{ csrf: string }>("/api/auth/login", {
        method: "POST",
        body: { password },
      });
      localStorage.setItem(CSRF_KEY, res.csrf);
      setCsrf(res.csrf);
      setPassword("");
    } catch {
      setError("登录失败，请检查管理员密码。");
    } finally {
      setBusy(null);
    }
  }

  async function runAction(action: ActionName, path: string) {
    setBusy(action);
    setError("");
    try {
      await apiRequest(path, { method: "POST", csrf });
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function addWatchDir(event: FormEvent) {
    event.preventDefault();
    if (!watchDir.trim()) {
      return;
    }
    setBusy("add-watch");
    setError("");
    try {
      await apiRequest("/api/watch-dirs", {
        method: "POST",
        csrf,
        body: { path: watchDir.trim() },
      });
      setWatchDir("");
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function removeWatchDir(path: string) {
    setBusy(`remove-watch-${path}`);
    setError("");
    try {
      await apiRequest("/api/watch-dirs/remove", {
        method: "POST",
        csrf,
        body: { path },
      });
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function setOption(key: string, value: boolean) {
    setBusy(`option-${key}`);
    setError("");
    try {
      await apiRequest("/api/options", {
        method: "POST",
        csrf,
        body: { key, value },
      });
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function setSchedule(minutes: number) {
    setBusy("schedule");
    setError("");
    try {
      await apiRequest("/api/schedule", {
        method: "POST",
        csrf,
        body: { interval_seconds: Math.max(0, Math.round(minutes * 60)) },
      });
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function setParallelism(value: number) {
    const next = Math.max(1, Math.min(32, Math.round(value)));
    setBusy("parallelism");
    setError("");
    try {
      await apiRequest("/api/conversion/parallelism", {
        method: "POST",
        csrf,
        body: { value: next },
      });
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function loadMoreJobs() {
    if (!jobNextCursor) {
      return;
    }
    setBusy("jobs-more");
    setError("");
    try {
      const params = new URLSearchParams({ limit: "100", cursor: String(jobNextCursor) });
      if (jobFilter in statusLabels && jobFilter !== "new") {
        params.set("status", jobFilter);
      } else if (jobFilter in modeLabels) {
        params.set("mode", jobFilter);
      }
      const page = await apiRequest<JobsResponse>(`/api/jobs?${params.toString()}`);
      setJobs((items) => mergeById(items, page.jobs));
      setJobNextCursor(page.next_cursor ?? null);
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function loadMoreFiles() {
    if (!fileNextCursor) {
      return;
    }
    setBusy("files-more");
    setError("");
    try {
      const page = await apiRequest<FilesResponse>(`/api/files?limit=50&cursor=${fileNextCursor}`);
      setFiles((items) => mergeById(items, page.files));
      setFileNextCursor(page.next_cursor ?? null);
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  async function loadMoreBackups() {
    if (!backupNextCursor) {
      return;
    }
    setBusy("backups-more");
    setError("");
    try {
      const page = await apiRequest<BackupsResponse>(`/api/backups?limit=100&cursor=${backupNextCursor}`);
      setBackups((items) => mergeById(items, page.backups));
      setBackupNextCursor(page.next_cursor ?? null);
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  function submitSchedule(event: FormEvent) {
    event.preventDefault();
    const minutes = Number(scheduleMinutes);
    if (!Number.isFinite(minutes) || minutes < 0) {
      setError("定时扫描间隔必须是不小于 0 的分钟数。");
      return;
    }
    void setSchedule(minutes);
  }

  async function uploadSubtitle(event: FormEvent) {
    event.preventDefault();
    if (!uploadFile) {
      return;
    }
    setBusy("upload");
    setError("");
    try {
      const form = new FormData();
      form.set("file", uploadFile);
      await apiRequest("/api/upload", { method: "POST", csrf, body: form });
      setUploadFile(null);
      await loadAll("refresh");
    } catch (err) {
      setError(readError(err));
    } finally {
      setBusy(null);
    }
  }

  function pickUploadFile(event: ChangeEvent<HTMLInputElement>) {
    setUploadFile(event.target.files?.[0] ?? null);
  }

  const filteredJobs = useMemo(() => {
    if (jobFilter === "all") {
      return jobs;
    }
    return jobs.filter((job) => job.status === jobFilter || job.mode === jobFilter);
  }, [jobFilter, jobs]);

  return (
    <main className="app-shell">
      <div className="aurora" aria-hidden="true" />

      <header className="topbar glass-surface">
        <div>
          <p className="eyebrow">ASS/SSA subset service {status?.version ? `v${status.version}` : ""}</p>
          <h1>控制台</h1>
          <p className="subtitle">ArisNAS字幕子集化自动化服务</p>
        </div>
        <div className="top-actions">
          <IconButton label="刷新" title="重新读取状态、文件、作业和备份列表。" icon={<RefreshCw size={18} />} busy={busy === "refresh"} onClick={() => void loadAll()} />
          <IconButton label="扫描" title="扫描所有监听目录，并将未处理字幕加入队列。" icon={<ScanSearch size={18} />} busy={busy === "scan"} onClick={() => void runAction("scan", "/api/scan")} disabled={!loggedIn || controls?.scan_running} />
          <IconButton label="重建索引" title="重新扫描字体库并刷新 SQLite 字体索引。" icon={<Database size={18} />} busy={busy === "rebuild"} onClick={() => void runAction("rebuild", "/api/index/rebuild")} disabled={!loggedIn || controls?.index_running} />
        </div>
      </header>

      {error ? <div className="notice error">{error}</div> : null}

      {!loggedIn ? (
        <section className="login-panel glass-surface">
          <div>
            <p className="eyebrow">Admin</p>
            <h2>管理员登录</h2>
          </div>
          <form onSubmit={(event) => void login(event)}>
            <KeyRound size={18} />
            <input value={password} onChange={(event) => setPassword(event.target.value)} type="password" placeholder="管理员密码" />
            <IconButton label="登录" title="验证管理员密码并保存会话。" icon={<ShieldCheck size={18} />} busy={busy === "login"} type="submit" />
          </form>
        </section>
      ) : null}

      <section className="steps">
        <StepPanel number="1" title="字体索引" icon={<Database size={20} />}>
          <MetricGrid>
            <Metric label="字体文件" value={status?.fonts.files ?? 0} />
            <Metric label="字体 face" value={status?.fonts.faces ?? 0} />
            <Metric label="索引错误" value={status?.fonts.errors ?? 0} tone={status?.fonts.errors ? "bad" : "ok"} />
            <Metric label="并发索引" value={status?.config.max_index_concurrency ?? 0} />
          </MetricGrid>
          <MetricGrid>
            <Metric label="缓存占用" value={formatBytes(status?.metrics?.cache?.bytes)} />
            <Metric label="缓存命中" value={`${(status?.metrics?.cache?.hit_rate_percent ?? 0).toFixed(1)}%`} />
            <Metric label="队列 P95" value={formatDurationMs(status?.metrics?.queue?.p95_ms)} />
            <Metric label="Worker 重启" value={status?.metrics?.workers?.restarts ?? 0} tone={status?.metrics?.workers?.restarts ? "bad" : "ok"} />
          </MetricGrid>
          <PathList title="字体目录" items={status?.config.font_dirs ?? []} />
        </StepPanel>

        <StepPanel number="2" title="字幕扫描" icon={<FileText size={20} />}>
          <div className="drop-zone">
            <ScanSearch size={30} />
            <div>
              <strong>{status?.subtitles.files ?? 0} 个字幕文件</strong>
              <span>扫描已配置的容器内目录，未处理字幕会自动入队。</span>
            </div>
            <IconButton label="扫描目录" title="立即扫描所有监听目录。" icon={<Play size={16} />} busy={busy === "scan"} onClick={() => void runAction("scan", "/api/scan")} disabled={!loggedIn || controls?.scan_running} />
            <IconButton
              label={controls?.scan_paused ? "继续扫描" : "暂停扫描"}
              title={controls?.scan_paused ? "继续当前扫描步骤。" : "暂停正在进行的扫描步骤。"}
              icon={controls?.scan_paused ? <Play size={16} /> : <Pause size={16} />}
              busy={busy === "scan-control"}
              onClick={() => void runAction("scan-control", controls?.scan_paused ? "/api/scan/resume" : "/api/scan/pause")}
              disabled={!loggedIn || !controls?.scan_running}
            />
            <IconButton label="取消扫描" title="取消当前扫描步骤。" icon={<X size={16} />} busy={busy === "scan-cancel"} onClick={() => void runAction("scan-cancel", "/api/scan/cancel")} disabled={!loggedIn || !controls?.scan_running} danger />
          </div>
          <ScanProgress progress={controls?.scan_progress} running={Boolean(controls?.scan_running)} />
          <form className="schedule-card" onSubmit={submitSchedule}>
            <div>
              <Clock3 size={16} />
              <strong>定时扫描</strong>
              <span>{formatSchedule(status?.config.scan_interval_seconds ?? 0)}</span>
            </div>
            <div className="schedule-presets">
              {[0, 5, 15, 60, 360].map((minutes) => (
                <button
                  key={minutes}
                  type="button"
                  className="preset-button"
                  data-active={Math.round((status?.config.scan_interval_seconds ?? 0) / 60) === minutes ? "true" : "false"}
                  data-tooltip={minutes === 0 ? "关闭自动扫描。" : `每 ${minutes} 分钟扫描一次监听目录。`}
                  onClick={() => void setSchedule(minutes)}
                  disabled={!loggedIn || busy === "schedule"}
                >
                  {minutes === 0 ? "关闭" : minutes < 60 ? `${minutes} 分钟` : `${minutes / 60} 小时`}
                </button>
              ))}
            </div>
            <label className="cron-input" data-tooltip="输入分钟数后保存，0 表示关闭。">
              <span>@every</span>
              <input value={scheduleMinutes} onChange={(event) => setScheduleMinutes(event.target.value)} inputMode="numeric" />
              <span>分钟</span>
            </label>
            <IconButton label="保存定时" title="保存定时扫描间隔。" icon={<Clock3 size={16} />} busy={busy === "schedule"} disabled={!loggedIn} type="submit" />
          </form>
          <form className="inline-form" onSubmit={(event) => void addWatchDir(event)}>
            <input value={watchDir} onChange={(event) => setWatchDir(event.target.value)} placeholder="添加容器内监听目录，例如 /watch2" />
            <IconButton label="添加目录" title="添加一个额外监听目录。主机目录需先挂载到容器内。" icon={<FolderPlus size={16} />} busy={busy === "add-watch"} disabled={!loggedIn || !watchDir.trim()} type="submit" />
          </form>
          <WatchDirList items={status?.config.watch_dir_items ?? (status?.config.watch_dirs ?? []).map((path) => ({ path, removable: false }))} busy={busy} onRemove={(path) => void removeWatchDir(path)} />
          <p className="hint">多个目录会合并扫描；新增目录必须是容器内可访问路径。</p>
        </StepPanel>

        <StepPanel number="3" title="选择操作" icon={<Sparkles size={20} />}>
          <div className="option-grid">
            {Object.entries(status?.config.options ?? {}).map(([key, enabled]) => (
              <button
                className="option-pill"
                key={key}
                type="button"
                data-enabled={enabled ? "true" : "false"}
                data-tooltip={optionTips[key] ?? key}
                onClick={() => void setOption(key, !enabled)}
                disabled={!loggedIn || busy === `option-${key}`}
              >
                <span>{optionLabels[key] ?? key}</span>
                <b>{enabled ? "ON" : "OFF"}</b>
              </button>
            ))}
          </div>

          <form className="upload-box" onSubmit={(event) => void uploadSubtitle(event)}>
            <label className="file-picker" data-tooltip="选择一个本地 ASS/SSA 文件，上传后立即创建转换作业。">
              <Upload size={18} />
              <input type="file" accept=".ass,.ssa" onChange={pickUploadFile} />
              <span>{uploadFile ? uploadFile.name : "单独上传字幕转换"}</span>
            </label>
            <IconButton label="上传并转换" title="上传选中的字幕文件并加入转换队列。" icon={<Play size={16} />} busy={busy === "upload"} disabled={!loggedIn || !uploadFile} type="submit" />
          </form>

          <div className="action-strip">
            <IconButton label="转换最新文件" title={recentFile ? "对最近扫描或上传的字幕执行字体子集化。" : "请先扫描目录或上传字幕文件。"} icon={<Play size={16} />} disabled={!recentFile} busy={recentFile ? busy === `process-${recentFile.id}` : false} onClick={() => recentFile && void runAction(`process-${recentFile.id}`, `/api/files/${recentFile.id}/process`)} />
            <IconButton label="清理并还原" title={recentFile ? "移除可还原的内嵌字体，并恢复随机字体名和绘图指令。" : "请先扫描目录或上传字幕文件。"} icon={<Trash2 size={16} />} disabled={!recentFile || !status?.capabilities?.strip_embedded} busy={recentFile ? busy === `strip-${recentFile.id}` : false} onClick={() => recentFile && void runAction(`strip-${recentFile.id}`, `/api/files/${recentFile.id}/strip-embedded`)} />
          </div>
          <SubtitleDetails file={detailFile} uploadFile={uploadFile} />
          <FileTable files={files} selectedId={selectedFile?.id} canModify={loggedIn} busy={busy} onSelect={setSelectedFileId} onAction={runAction} />
          {fileNextCursor ? <div className="page-actions"><IconButton label="加载更多字幕" title="继续读取较早的字幕记录。" icon={<ChevronDown size={16} />} busy={busy === "files-more"} onClick={() => void loadMoreFiles()} /></div> : null}
        </StepPanel>

        <StepPanel number="4" title="转换与恢复" icon={<ArchiveRestore size={20} />}>
          <div className="queue-summary">
            <Metric label="排队" value={jobCounts.queued ?? 0} />
            <Metric label="运行" value={jobCounts.running ?? 0} />
            <Metric label="完成" value={(jobCounts.success ?? 0) + (jobCounts.partial ?? 0)} />
            <Metric label="失败" value={jobCounts.failed ?? 0} tone={jobCounts.failed ? "bad" : "ok"} />
          </div>
          <div className="action-strip">
            <IconButton
              label={controls?.conversion_paused ? "继续转换" : "暂停转换"}
              title={controls?.conversion_paused ? "继续启动排队中的转换任务。" : "暂停启动新的转换任务。"}
              icon={controls?.conversion_paused ? <Play size={16} /> : <Pause size={16} />}
              busy={busy === "conversion-control"}
              onClick={() => void runAction("conversion-control", controls?.conversion_paused ? "/api/conversion/resume" : "/api/conversion/pause")}
              disabled={!loggedIn}
            />
            <IconButton label="取消队列" title="取消尚未开始的转换任务。" icon={<X size={16} />} busy={busy === "conversion-cancel"} onClick={() => void runAction("conversion-cancel", "/api/conversion/cancel")} disabled={!loggedIn} danger />
            <IconButton label="保存错误日志" title="将当前失败作业的文件名、路径和错误原因保存到数据目录的 error-logs 文件夹。" icon={<FileText size={16} />} busy={busy === "failed-log"} onClick={() => void runAction("failed-log", "/api/jobs/failed-log")} disabled={!loggedIn || !(jobCounts.failed ?? 0)} />
            <div className="stepper-field">
              <span className="stepper-label">并行</span>
              <div className="stepper" data-tooltip="调整同时运行的转换任务数。">
                <button type="button" onClick={() => void setParallelism(conversionParallelism - 1)} disabled={!loggedIn || busy === "parallelism" || conversionParallelism <= 1}>-</button>
                <span>{conversionParallelism}</span>
                <button type="button" onClick={() => void setParallelism(conversionParallelism + 1)} disabled={!loggedIn || busy === "parallelism" || conversionParallelism >= 32}>+</button>
              </div>
            </div>
          </div>
          <div className="table-tools">
            <Filter size={16} />
            <select value={jobFilter} onChange={(event) => setJobFilter(event.target.value)}>
              <option value="all">全部作业</option>
              <option value="subset">转换</option>
              <option value="strip_embedded">清理还原</option>
              <option value="queued">排队</option>
              <option value="running">运行中</option>
              <option value="success">已完成</option>
              <option value="failed">失败</option>
              <option value="partial">部分完成</option>
              <option value="cancelled">已取消</option>
            </select>
          </div>
          <JobTable jobs={filteredJobs} busy={busy} onRetry={(job) => void runAction(`retry-${job.id}`, `/api/jobs/${job.id}/retry`)} />
          {jobNextCursor ? <div className="page-actions"><IconButton label="加载更多作业" title="继续读取较早的作业记录。" icon={<ChevronDown size={16} />} busy={busy === "jobs-more"} onClick={() => void loadMoreJobs()} /></div> : null}
          <BackupTable backups={backups} busy={busy} onRestore={setRestoreTarget} />
          {backupNextCursor ? <div className="page-actions"><IconButton label="加载更多备份" title="继续读取较早的备份记录。" icon={<ChevronDown size={16} />} busy={busy === "backups-more"} onClick={() => void loadMoreBackups()} /></div> : null}
        </StepPanel>
      </section>

      <section className="log-panel glass-surface">
        <div className="log-title">
          <div>
            <span className="step-number">L</span>
            <h2>运行日志</h2>
          </div>
          <span>Live events</span>
        </div>
        <div className="log-progress"><i /></div>
        <EventLog events={events} />
      </section>

      {restoreTarget ? (
        <div className="modal-backdrop" onMouseDown={() => setRestoreTarget(null)}>
          <section className="modal glass-surface" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
            <h2>恢复备份</h2>
            <p>将覆盖当前字幕文件。</p>
            <code>{restoreTarget.source_path}</code>
            <div className="modal-actions">
              <IconButton label="取消" title="关闭确认窗口。" icon={<RotateCcw size={16} />} onClick={() => setRestoreTarget(null)} />
              <IconButton
                label="确认恢复"
                title="用备份文件覆盖当前字幕。"
                icon={<Undo2 size={16} />}
                busy={busy === `restore-${restoreTarget.id}`}
                danger
                onClick={() => {
                  const id = restoreTarget.id;
                  setRestoreTarget(null);
                  void runAction(`restore-${id}`, `/api/backups/${id}/restore`);
                }}
              />
            </div>
          </section>
        </div>
      ) : null}
    </main>
  );
}

function StepPanel({ number, title, icon, children }: { number: string; title: string; icon: ReactNode; children: ReactNode }) {
  return (
    <section className="step-panel glass-surface">
      <div className="step-heading">
        <span className="step-number">{number}</span>
        {icon}
        <h2>{title}</h2>
      </div>
      {children}
    </section>
  );
}

function MetricGrid({ children }: { children: ReactNode }) {
  return <div className="metric-grid">{children}</div>;
}

function Metric({ label, value, tone = "default" }: { label: string; value: number | string; tone?: "default" | "ok" | "bad" }) {
  return (
    <div className="metric" data-tone={tone}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function PathList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="path-list">
      <span>{title}</span>
      {items.length ? items.map((item) => <code key={item}>{item}</code>) : <code>未配置</code>}
    </div>
  );
}

function WatchDirList({ items, busy, onRemove }: { items: Array<{ path: string; removable?: boolean }>; busy: string | null; onRemove: (path: string) => void }) {
  return (
    <div className="path-list watch-list">
      <span>监听目录</span>
      {items.length ? items.map((item) => (
        <div className="path-row" key={item.path}>
          <code>{item.path}</code>
          {item.removable ? (
            <button type="button" className="mini-icon" data-tooltip="移除这个额外监听目录。" onClick={() => onRemove(item.path)} disabled={busy === `remove-watch-${item.path}`}>
              {busy === `remove-watch-${item.path}` ? <Loader2 className="spin" size={14} /> : <X size={14} />}
            </button>
          ) : <span className="lock-chip">默认</span>}
        </div>
      )) : <code>未配置</code>}
    </div>
  );
}

function ScanProgress({
  progress,
  running,
}: {
  progress?: NonNullable<StatusResponse["config"]["controls"]>["scan_progress"];
  running: boolean;
}) {
  if (!progress?.stage) {
    return null;
  }
  const stageLabels: Record<string, string> = {
    discovering: "发现文件",
    filtering: "筛选任务",
    enqueuing: "加入队列",
    completed: "扫描完成",
    cancelled: "扫描已取消",
    failed: "扫描失败",
  };
  const total = progress.total ?? 0;
  const current = progress.current ?? 0;
  const percent = total > 0 ? Math.min(100, Math.round((current / total) * 100)) : 0;
  return (
    <div className="scan-progress" data-running={running ? "true" : "false"}>
      <div>
        <strong>{stageLabels[progress.stage] ?? progress.stage}</strong>
        <span>{total > 0 ? `${current} / ${total}` : `${progress.seen ?? 0}`}</span>
      </div>
      <div className="progress-track" data-indeterminate={running && total === 0 ? "true" : "false"}>
        <i style={{ width: total > 0 ? `${percent}%` : running ? "28%" : "100%" }} />
      </div>
      <dl>
        <div><dt>发现</dt><dd>{progress.seen ?? 0}</dd></div>
        <div><dt>待转换</dt><dd>{progress.ready ?? 0}</dd></div>
        <div><dt>已入队</dt><dd>{progress.queued ?? 0}</dd></div>
        <div><dt>跳过</dt><dd>{progress.skipped ?? 0}</dd></div>
        <div><dt>失败</dt><dd>{progress.failed ?? 0}</dd></div>
      </dl>
    </div>
  );
}

function SubtitleDetails({ file, uploadFile }: { file?: SubtitleFile; uploadFile: File | null }) {
  const serviceFile = uploadFile ? undefined : file;
  const analysis = serviceFile?.analysis;
  const thirdParty = analysis?.third_party_fonts ?? [];
  const systemFonts = analysis?.system_fonts ?? [];
  const embedded = analysis?.embedded_fonts ?? [];
  const name = uploadFile?.name ?? serviceFile?.relative_path ?? serviceFile?.path ?? "等待字幕";
  const size = uploadFile?.size ?? serviceFile?.size ?? 0;
  const uploadModified = uploadFile ? new Date(uploadFile.lastModified).toLocaleString() : "";
  return (
    <section className="analysis-card">
      <div className="analysis-head">
        <div>
          <span className="step-number">A</span>
          <h3>字幕文件详情</h3>
        </div>
        <span>Analysis</span>
      </div>
      <div className="file-summary">
        <FileText size={18} />
        <div>
          <strong>{name}</strong>
          <span>
            {formatBytes(size)}
            {uploadFile ? ` · 本地待上传 · ${uploadModified}` : ""}
            {serviceFile?.last_status ? ` · ${statusLabels[serviceFile.last_status] ?? serviceFile.last_status}` : ""}
          </span>
        </div>
      </div>
      <div className="analysis-metrics">
        <Metric label="绘图块" value={analysis ? analysis.drawing_count ?? 0 : "-"} />
        <Metric label="第三方字体" value={analysis ? thirdParty.length : "-"} />
        <Metric label="系统字体" value={analysis ? systemFonts.length : "-"} />
        <Metric label="已内嵌字体" value={analysis ? embedded.length : "-"} />
      </div>
      <DetailGroup title="绘图指令" badge={analysis ? `${analysis.drawing_count ?? 0} 个绘图块` : uploadFile ? "上传后自动分析" : "等待分析"} items={analysis && (analysis.drawing_count ?? 0) > 0 ? ["检测到 ASS 绘图指令"] : []} empty="未发现绘图指令" />
      <DetailGroup title="第三方字体" badge={analysis ? `${analysis.char_count ?? 0} 字符` : "等待分析"} items={thirdParty} empty="未发现第三方字体" />
      <DetailGroup title="系统字体" badge={analysis ? `${systemFonts.length} 个字体` : "等待分析"} items={systemFonts} empty="未发现系统字体" />
      <DetailGroup title="已内嵌字体" badge={analysis ? `${embedded.length} 个字体` : "等待分析"} items={embedded} empty="无已内嵌字体" />
    </section>
  );
}

function DetailGroup({ title, badge, items, empty }: { title: string; badge: string; items: string[]; empty: string }) {
  return (
    <details className="detail-group" open>
      <summary>
        <span><ChevronDown size={15} />{title}</span>
        <b>{badge}</b>
      </summary>
      {items.length ? items.map((item) => <code key={item}>{item}</code>) : <p>{empty}</p>}
    </details>
  );
}

function FileTable({
  files,
  selectedId,
  canModify,
  busy,
  onSelect,
  onAction,
}: {
  files: SubtitleFile[];
  selectedId?: number;
  canModify: boolean;
  busy: string | null;
  onSelect: (id: number) => void;
  onAction: (action: ActionName, path: string) => Promise<void>;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>字幕</th>
            <th>状态</th>
            <th>大小</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {files.length ? files.map((file) => (
            <tr key={file.id} data-selected={file.id === selectedId ? "true" : "false"}>
              <td><code>{file.relative_path || file.path}</code></td>
              <td><StatusBadge status={file.last_status ?? "new"} /></td>
              <td>{formatBytes(file.size)}</td>
              <td className="row-actions">
                <IconButton label="详情" title="在上方查看这个字幕的按需分析结果。" icon={<Eye size={14} />} compact busy={busy === `detail-${file.id}`} onClick={() => onSelect(file.id)} />
                <IconButton label="转换" title="对这个字幕执行字体子集化。" icon={<Play size={14} />} compact disabled={!canModify} busy={busy === `process-${file.id}`} onClick={() => void onAction(`process-${file.id}`, `/api/files/${file.id}/process`)} />
                <IconButton label="清理" title="清理内嵌字体并尽量还原随机名、绘图指令。" icon={<Trash2 size={14} />} compact disabled={!canModify} busy={busy === `strip-${file.id}`} onClick={() => void onAction(`strip-${file.id}`, `/api/files/${file.id}/strip-embedded`)} />
                <a className="icon-link" href={`/api/files/${file.id}/download`} data-tooltip="下载当前版本字幕。">
                  <Download size={14} />
                  下载
                </a>
              </td>
            </tr>
          )) : (
            <tr><td colSpan={4} className="empty">暂无字幕。请扫描目录或单独上传字幕。</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function JobTable({ jobs, busy, onRetry }: { jobs: Job[]; busy: string | null; onRetry: (job: Job) => void }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>作业</th>
            <th>模式</th>
            <th>状态</th>
            <th>统计</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {jobs.length ? jobs.map((job) => (
            <tr key={job.id}>
              <td>
                <span className="job-id">#{job.id}</span>
                <code>{job.path}</code>
                {Array.isArray(job.missing_fonts) && job.missing_fonts.length ? <small>{job.missing_fonts.join(", ")}</small> : null}
                {job.status === "failed" && job.message ? <small className="error-text">{job.message}</small> : null}
              </td>
              <td>{modeLabels[job.mode ?? "subset"] ?? job.mode}</td>
              <td><StatusBadge status={job.status} /></td>
              <td>{summarizeStats(job)}</td>
              <td className="row-actions">
                <IconButton label="重试" title="用相同模式重新创建作业。" icon={<RefreshCw size={14} />} compact disabled={job.status !== "failed" && job.status !== "partial"} busy={busy === `retry-${job.id}`} onClick={() => onRetry(job)} />
              </td>
            </tr>
          )) : (
            <tr><td colSpan={5} className="empty">暂无作业</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function BackupTable({ backups, busy, onRestore }: { backups: Backup[]; busy: string | null; onRestore: (backup: Backup) => void }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>备份</th>
            <th>时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {backups.length ? backups.slice(0, 8).map((backup) => (
            <tr key={backup.id}>
              <td><code>{backup.backup_path}</code></td>
              <td>{formatTime(backup.created_at)}</td>
              <td className="row-actions">
                <IconButton label="恢复" title="从这个备份恢复原字幕。" icon={<Undo2 size={14} />} compact danger busy={busy === `restore-${backup.id}`} onClick={() => onRestore(backup)} />
              </td>
            </tr>
          )) : (
            <tr><td colSpan={3} className="empty">暂无备份</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function EventLog({ events }: { events: EventPayload[] }) {
  return (
    <div className="event-log">
      {events.length ? events.slice(0, 18).map((event, index) => {
        const parts = formatEventParts(event);
        return (
          <div key={`${event.ts}-${index}`} data-level={event.level}>
            <time>{formatTime(event.ts)}</time>
            <b>{eventKindLabel(event.kind)}</b>
            <p>{parts.summary}</p>
            {parts.detail ? <code>{parts.detail}</code> : null}
          </div>
        );
      }) : <div className="empty">暂无日志</div>}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const ok = status === "success";
  return (
    <span className="status-badge" data-status={status}>
      {ok ? <CheckCircle2 size={14} /> : null}
      {statusLabels[status] ?? status}
    </span>
  );
}

function IconButton({
  label,
  title,
  icon,
  busy,
  danger,
  compact,
  disabled,
  type = "button",
  onClick,
}: {
  label: string;
  title: string;
  icon: ReactNode;
  busy?: boolean;
  danger?: boolean;
  compact?: boolean;
  disabled?: boolean;
  type?: "button" | "submit";
  onClick?: () => void;
}) {
  return (
    <button className="icon-button" data-tooltip={title} data-danger={danger ? "true" : "false"} data-compact={compact ? "true" : "false"} type={type} onClick={onClick} disabled={disabled || busy}>
      {busy ? <Loader2 className="spin" size={16} /> : icon}
      <span>{label}</span>
    </button>
  );
}

function summarizeStats(job: Job) {
  const stats = job.stats ?? {};
  if (job.mode === "strip_embedded") {
    return `${stats.embedded_removed_count ?? 0} 移除 / ${stats.random_names_restored ?? 0} 名称 / ${stats.drawings_restored ?? 0} 绘图`;
  }
  return `${stats.embedded_count ?? 0} 字体 / ${stats.missing_count ?? 0} 缺失 / ${stats.draw_fonts_created ?? 0} 绘图字体`;
}

function mergeById<T extends { id: number }>(current: T[], next: T[]): T[] {
  const ids = new Set(current.map((item) => item.id));
  return [...current, ...next.filter((item) => !ids.has(item.id))];
}

function eventKindLabel(kind: string) {
  return {
    scan: "扫描",
    job: "作业",
    index: "索引",
    backup: "备份",
    cache: "缓存",
    upload: "上传",
    config: "配置",
  }[kind] ?? kind;
}

function formatSchedule(seconds?: number) {
  if (!seconds) {
    return "已关闭";
  }
  if (seconds < 60) {
    return `每 ${seconds} 秒`;
  }
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) {
    return `每 ${minutes} 分钟`;
  }
  if (minutes % 60 === 0) {
    return `每 ${minutes / 60} 小时`;
  }
  return `每 ${minutes} 分钟`;
}

function formatEventParts(event: EventPayload): { summary: string; detail?: string } {
  const text = event.message;
  const scan = text.match(/manual scan finished: ScanSummary \{ seen: (\d+), queued: (\d+), skipped: (\d+), failed: (\d+) \}/);
  if (scan) {
    return { summary: `扫描完成：发现 ${scan[1]}，入队 ${scan[2]}，跳过 ${scan[3]}，失败 ${scan[4]}。` };
  }
  if (text === "manual scan started") {
    return { summary: "开始扫描监听目录。" };
  }
  if (text === "manual rebuild started") {
    return { summary: "开始重建字体索引。" };
  }
  if (text.startsWith("manual rebuild finished")) {
    return { summary: "字体索引重建完成。" };
  }
  if (text.includes("scheduled scan disabled")) {
    return { summary: "定时扫描已关闭。" };
  }
  if (text.startsWith("queued ")) {
    return { summary: "字幕已加入处理队列。", detail: text.replace(/^queued\s+/, "") };
  }
  const normalized = text.replace(/^manual /, "");
  const split = normalized.match(/^(.{2,40}?[：:])(.+)$/);
  if (split && /[\\/]|\.(ass|ssa|ttf|otf|ttc|otc)\b/i.test(split[2])) {
    return { summary: split[1].trim(), detail: split[2].trim() };
  }
  if (normalized.length > 90) {
    return { summary: `${normalized.slice(0, 72).trimEnd()}...`, detail: normalized };
  }
  return { summary: normalized };
}

function formatBytes(value?: number) {
  if (!value) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB"];
  let n = value;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
}

function formatDurationMs(value?: number) {
  if (!value) {
    return "0 ms";
  }
  if (value < 1000) {
    return `${value} ms`;
  }
  return `${(value / 1000).toFixed(1)} s`;
}

function formatTime(value?: string) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function readError(err: unknown) {
  if (err instanceof Error) {
    return err.message;
  }
  return "请求失败。";
}
