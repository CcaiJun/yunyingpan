# Yingpan (影盘) - 虚拟 WebDAV 块设备硬盘

Yingpan 是一个基于 Python 开发的高性能虚拟硬盘管理工具。它将 WebDAV 云存储（如 123云盘、阿里云盘、Alist 等）抽象为底层的**块设备**，支持通过 **FUSE** 或 **NBD (Network Block Device)** 技术映射为本地磁盘。

与传统的 WebDAV 挂载工具（如 rclone）不同，Yingpan 采用**分块存储方案**，将云端视为一个巨大的硬盘，支持直接在上面进行 `mkfs` 格式化（推荐使用 ext4），并像使用普通物理硬盘一样进行随机读写、运行数据库或作为 Docker 卷使用。

## 核心特性

- **🚀 双驱动模式支持**:
  - **FUSE 模式**: 生成虚拟 `.img` 镜像文件，通过 Loop 设备挂载，兼容性极佳，无需特殊内核模块。
  - **NBD 模式**: 高性能块设备驱动，直接与 Linux 内核块层通信，减少用户态切换，效率更高。
- **📦 分块存储管理 (Block-based)**:
  - 将磁盘切分为固定大小（默认 4MB）的块。
  - 仅在访问时按需下载对应的数据块，大幅节省流量和等待时间。
- **🧠 智能本地缓存 (LRU)**:
  - 采用 SQLite 管理元数据，记录数据块状态。
  - 支持热点数据本地缓存，可自定义最大缓存容量，自动清理最久未访问的数据块。
- **⚡ 异步后台上传 (Write-Back)**:
  - 写入操作首先存入本地“脏块”区域并立即返回，由后台线程池并发异步上传至云端，确保本地 IO 体验。
- **📉 数据压缩支持**:
  - 内置支持 **Zstd** 和 **LZ4** 压缩算法。
  - 开启后可显著减少 WebDAV 云端占用空间，降低流量消耗，并提升弱网环境下的响应速度。
- **🖥️ 图形化管理控制台**:
  - 内置基于 [FastAPI](file:///root/yingpan/server.py) 的 Web 界面。
  - 实时监控：上传/下载速度、队列状态、缓存占用率、IOPS 等。
- **🛠️ 系统级集成**:
  - 提供 `systemd` 服务支持，实现开机自启、故障自恢复。

## 快速部署

### 环境要求
- **操作系统**: Linux (推荐 Debian 11+, Ubuntu 20.04+, CentOS 7+)
- **依赖工具**: `fuse3`, `nbd-client` (如需使用 NBD 模式), `e2fsprogs`, `python3.8+`
- **内核模块**: `fuse`, `nbd`

### 一键安装
1. 克隆或上传本项目到服务器。
2. 进入目录并执行部署脚本：
   ```bash
   cd yingpan
   sudo ./deploy.sh
   ```
   脚本会自动安装系统依赖、创建虚拟环境、配置内核模块并启动 `yingpan` 服务。

## 使用说明

### 1. 访问控制台
部署完成后，通过浏览器访问：
`http://服务器IP:8001`

### 2. 配置磁盘
在 Web 界面中点击“新增磁盘”，填写以下参数：
- **磁盘名称**: 标识该磁盘，挂载点将基于此名称生成。
- **驱动模式**: 
    - `fuse`: 推荐大多数场景使用。
    - `nbd`: 追求极致性能时使用。
- **WebDAV 配置**: 填写 URL、用户名和密码。
- **压缩算法**: 
    - `none`: 高 CPU 性能。
    - `zstd`: 最佳压缩比（推荐）。
    - `lz4`: 极速压缩。
- **磁盘容量**: 虚拟硬盘的总容量（如 `1024GB`）。
- **缓存上限**: 本地允许占用的最大磁盘空间。

### 3. 系统挂载
磁盘启动后，状态变为 `running`。应用会自动将其挂载到：
`/mnt/v_disks/[磁盘名]`

> **注意**: 首次创建大容量磁盘时，系统会自动进行格式化 (`mkfs.ext4`)，可能需要 1-3 分钟，请耐心等待状态更新。

## 备份与恢复

Yingpan 设计时充分考虑了系统重装场景，支持快速恢复挂载状态。

### 1. 备份 (重装前)
执行备份脚本：
```bash
./backup.sh
```
这会生成 `yingpan_backup_日期.tar.gz`。请将其保存到安全位置。

### 2. 恢复 (重装后)
1. 将备份包上传并解压：
   ```bash
   tar -xzf yingpan_backup_xxxx.tar.gz
   cd yingpan
   ```
2. 重新执行部署脚本：
   ```bash
   sudo ./deploy.sh
   ```
3. **自动恢复**: 脚本完成后，服务会自动读取 `disks_config.json` 并恢复所有磁盘挂载。

## 项目结构

- [server.py](file:///root/yingpan/server.py): FastAPI 后端，负责 API 路由与磁盘生命周期管理。
- [block_manager.py](file:///root/yingpan/block_manager.py): 核心块管理逻辑，负责缓存控制、SQLite 元数据与 WebDAV 同步。
- [v_drive.py](file:///root/yingpan/v_drive.py): FUSE 驱动实现，将块设备模拟为 `.img` 文件。
- [nbd_server.py](file:///root/yingpan/nbd_server.py): NBD 驱动服务端实现，直接提供块设备接口。
- [index.html](file:///root/yingpan/index.html): 现代化的 Web 管理控制台前端。
- [deploy.sh](file:///root/yingpan/deploy.sh): 自动化部署与环境配置脚本。
- [backup.sh](file:///root/yingpan/backup.sh): 配置与程序备份脚本。

## 管理维护

- **服务状态**: `sudo systemctl status yingpan`
- **停止服务**: `sudo systemctl stop yingpan`
- **查看日志**: `tail -f server.log`
- **手动清理缓存**: 可在 Web 界面点击“清理缓存”或手动删除 `/var/cache/v_drive/` 下的内容。

## 注意事项
- **安全性**: Web 控制台默认无登录认证，建议仅在内网访问或通过 Nginx 配置 Basic Auth。
- **内核模块**: NBD 模式依赖 `nbd` 内核模块，部分轻量级容器（如 OpenVZ）可能不支持。
- **性能优化**: 建议将缓存目录设置在 SSD 上以获得最佳体验。
