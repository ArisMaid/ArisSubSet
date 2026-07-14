# ArisSubSet

ArisSubSet 是一个可部署在 Docker/NAS 上的 ASS/SSA 字幕字体子集化服务。它以 MontageSubs/ass-subset 的字体子集化思路为主线，结合 SQLite 字体索引、定时扫描、Web UI、备份恢复和上传转换，面向大字体库场景运行。

## 功能

- 扫描只读字体库并缓存字体 name 表索引到 SQLite。
- 定时扫描 `/watch` 下的 `.ass/.ssa`，只处理当前配置下未成功处理的字幕。
- 支持单独上传字幕转换，适合临时文件处理。
- 字体匹配支持 Family、Full、PostScript、Typographic/WWS 等索引名称。
- 支持字体子集、ASCII 保留、多字重、随机字体名、绘图字体 draw 表、已有内嵌字体清理还原。
- 默认替换原字幕文件，并在写回前保存原文件到 `/backups`。
- Web UI 支持字体索引、扫描目录、定时扫描、作业、日志、缺字详情和备份恢复。

## Docker Compose 部署

安装 `argon2-cffi` 并生成 Argon2id 管理员密码哈希：

```bash
python -m pip install argon2-cffi
python -c "from argon2 import PasswordHasher; import getpass; print(PasswordHasher().hash(getpass.getpass()))"
```

旧的 `sha256:<hex>` 配置仍可继续使用，但新部署应使用 Argon2id。

编辑 `docker-compose.yml`，替换 `ADMIN_PASSWORD_HASH`，然后启动：

```bash
docker compose up -d --build
```

默认访问地址：

```text
http://<NAS-IP>:8080/
```

## 目录

- `/fonts`：字体库，只读挂载。
- `/watch`：字幕目录，可写挂载；服务会原地替换处理后的字幕。
- `/backups`：原字幕备份目录。
- `/data`：SQLite、子集缓存和运行时配置。

本地 compose 默认映射：

- `./Fonts:/fonts:ro`
- `./watch:/watch`
- `./backups:/backups`
- `./data:/data`

也可以通过 `.env` 或环境变量改主机路径：

```env
ARIS_HTTP_PORT=8080
ARIS_FONT_DIR=/volume1/fonts
ARIS_WATCH_DIR=/volume1/video
ARIS_BACKUP_DIR=/volume1/aris-subset/backups
ARIS_DATA_DIR=/volume1/docker/aris-subset/data
```

## N100 NAS 推荐配置

`docker-compose.yml` 的默认值按低功耗 NAS 保守配置：

- `MAX_CONCURRENT_JOBS=1`：一次只处理一个字幕，避免大量 fontTools 任务争抢 CPU。
- `MAX_FONT_WORKERS=2`：保留两个长期 Python worker，兼顾响应和内存占用。
- `MAX_INDEX_CONCURRENCY=16`：字体索引阶段有限并发，适合大字体库的热索引和增量扫描。
- `JOB_QUEUE_SIZE=256`：队列足够日常使用，不无限膨胀。
- `SUBSET_CACHE_MAX_MB=2048`：限制子集缓存占用，适合系统盘空间有限的 NAS。
- 日志轮转为 `10m * 3`，避免 NAS 系统盘被 Docker 日志撑满。

如果 NAS 还承担转码、下载或媒体库任务，建议保持默认值。若只跑本服务，可以把 `MAX_INDEX_CONCURRENCY` 调到 `24` 或 `32` 试测字体索引速度。

## 环境变量

- `ADMIN_PASSWORD_HASH`：推荐使用 Argon2id PHC 字符串；兼容旧的 `sha256:<hex>`。
- `SECURE_COOKIES`：HTTPS 反向代理部署时设为 `true`。
- `FONT_DIRS`：容器内字体目录，可用逗号或分号分隔多个目录。
- `WATCH_DIRS`：容器内字幕扫描目录，可用逗号或分号分隔多个目录。
- `BACKUP_DIR`：备份根目录。
- `DATA_DIR`：数据库和缓存根目录。
- `SCAN_CRON`：支持 `disabled`、`@every 30s`、`@every 15m`、`@every 1h`。
- `MAX_CONCURRENT_JOBS`：字幕处理并发数。
- `MAX_FONT_WORKERS`：长期 Python/fontTools worker 数。
- `MAX_INDEX_CONCURRENCY`：字体索引并发数。
- `MAX_SCAN_CONCURRENCY`：字幕筛选阶段并发数。
- `MAX_CONVERSION_MEMORY_MB`：转换任务估算工作集的总内存预算。
- `SUBSET_CACHE_MAX_MB`：子集缓存容量上限，默认 2048 MiB；超过后按最近使用时间淘汰，设为 `0` 表示不限制。
- `FONT_WORKER_TIMEOUT_SECONDS`：单次 fontTools 请求超时，超时后 Worker 会重启并重试一次。
- `BACKUP_RETENTION_DAYS`：默认 `0`，不自动清理备份；大于 `0` 时会在启动后及每 24 小时清理过期备份。

处理选项：

- `EMBED_EXTERNAL_FONTS`
- `EMBED_SYSTEM_FONTS`
- `INCLUDE_ASCII`
- `MULTI_WEIGHT`
- `RANDOMIZE_FONT_NAMES`
- `DRAW_SUBSET`
- `FULL_FONT_EMBED`
- `FALLBACK_FULL_FONT_EMBED`
- `VARIABLE_FONTS`

这些选项也可以在 Web UI 中运行时切换，切换结果会写入 `/data` 的 SQLite 配置。

## 性能说明

字体索引借鉴 FontLoaderSub 的缓存方向：索引阶段不全量读取 60G 字体文件，也不计算完整哈希。服务优先比较路径、大小、mtime；只有变更文件才解析 sfnt/name 等必要表，完整 SHA-256 延迟到字体实际用于子集缓存时计算。

目录遍历在专用阻塞线程中执行，慢速 NAS 挂载不会占住异步 API 线程；字体索引按固定批次提交 SQLite，避免超大字体库形成长事务和过高内存峰值。子集缓存使用文件修改时间记录最近使用，启动后及运行期间自动执行容量维护。控制台会显示缓存命中率、队列 P95 延迟和 Worker 重启数，便于定位性能退化。

当前测试字体库：

- 字体文件：18,836。
- OpenType faces：20,731。
- 热启动索引：约 10 秒级，全部未变更文件直接跳过。

## 授权说明

- MontageSubs/ass-subset 为 MIT，本项目参考其算法方向和规范。
- yzwduck/FontLoaderSub 为 GPL-2.0，本项目只参考数据库索引设计思路，不复制其 GPL 源码。
