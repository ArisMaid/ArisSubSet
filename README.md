# ArisSubSet

面向 Docker / NAS 的 ASS、SSA 字幕字体子集化服务。通过只读字体库索引，将字幕所需字体嵌入文件，减少对播放设备字体环境的依赖。

- **批量与临时处理**：定时扫描目录，也可通过 Web 上传单个字幕。
- **大字体库支持**：SQLite 增量索引、子集缓存和可调并发，适合低功耗 NAS。
- **可追踪、可恢复**：查看作业、缺失字体和日志；原地写回前备份原字幕。

## 快速部署

下载仓库中的 [docker-compose.yml](docker-compose.yml)，在同目录创建 `.env`：

```dotenv
ARIS_IMAGE=ghcr.io/arismaid/arissubset:v0.3.1
ARIS_HTTP_PORT=8080
ARIS_FONT_DIR=/volume1/fonts
ARIS_WATCH_DIR=/volume1/video
ARIS_BACKUP_DIR=/volume1/aris-subset/backups
ARIS_DATA_DIR=/volume1/docker/aris-subset/data
ADMIN_PASSWORD_HASH='替换为生成的 Argon2id 哈希'
```

按实际路径修改。生成密码哈希后填入 `.env`，保留单引号，避免 `$` 被 Compose 当作变量展开：

```bash
python -m pip install argon2-cffi
python -c "from argon2 import PasswordHasher; import getpass; print(PasswordHasher().hash(getpass.getpass()))"
docker compose pull
docker compose up -d
```

打开 `http://<NAS-IP>:8080`，使用生成哈希时输入的密码登录。服务应放在可信内网；使用 HTTPS 反向代理时设置 `SECURE_COOKIES=true`。

| 容器目录 | 用途 |
| --- | --- |
| `/fonts` | 字体库，只读 |
| `/watch` | 字幕目录，**备份后原地替换** |
| `/backups` | 原字幕备份 |
| `/data` | 数据库、运行配置、上传文件及子集缓存 |

## 使用与恢复

1. 下载、整理完成且字体索引就绪后，再扫描转换；处理期间不要让其他程序修改同一字幕。
2. 默认配置适合 N100 类 NAS：转换并发 1、字体 Worker 2、子集缓存 2 GiB。更多参数见 [Compose 注释](docker-compose.yml)。
3. 队列清空后再改高级选项。Web 设置会持久保存并优先于环境变量默认值；任务采用执行时配置，不会自动重做已有子集。
4. “部分完成”需要检查缺失字体。补齐字体、更新索引后，**从原始备份重新处理**；已有内嵌字体禁止直接重试。没有原始备份时先人工检查。

**恢复顺序：**关闭定时扫描 → 等待扫描结束 → 暂停新任务 → 取消待执行队列并等待运行任务结束 → 选择原始备份恢复。恢复会重置状态和分析缓存，随后可手动转换并继续队列；重新开启扫描也会再次处理恢复的原字幕。

- 暂停、取消队列都不影响已运行任务，也不关闭定时扫描；重启后会恢复未完成任务，暂停状态不保留。
- 自动扫描跳过已有子集标记。外部文件显示“已跳过／未验证”，已有记录保留原结果。“清理并还原”不等于原始备份恢复。
- 缺失字体表示未匹配到字体名称，**不代表完整字形检测**。整字体兜底会提示体积可能增大，特殊字幕请用实际播放器抽查。
- 浏览历史时暂停列表刷新，顶部状态继续更新；点击“刷新”返回最新记录。

## 升级与维护

修改 `.env` 的 `ARIS_IMAGE` 为目标版本，再执行 `docker compose pull` 和 `docker compose up -d`。升级前备份 `/data` 与 `/backups`，保留原目录挂载。

备份默认永久保留，上传文件也不会下载后自动删除。通过 NAS 容量提醒和人工维护管理空间；`BACKUP_RETENTION_DAYS` 可显式设置备份保留天数。子集缓存配额不限制整个服务磁盘占用，`JOB_QUEUE_SIZE` 也只是通知通道容量，不是总任务上限。

## 项目参考

- MontageSubs/ass-subset（MIT）：字体子集化算法方向与规范。
- yzwduck/FontLoaderSub（GPL-2.0）：仅参考索引设计思路，不复制其 GPL 源码。
