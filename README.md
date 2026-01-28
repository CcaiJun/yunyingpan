# Yingpan (影盘) - 虚拟 WebDAV 块设备硬盘

Yingpan 是一个基于 Python 开发的高性能虚拟硬盘管理工具。它将 WebDAV 云存储（如 123云盘、阿里云盘等）抽象为底层的**块设备**，支持通过 **FUSE** 或 **NBD (Network Block Device)** 技术映射为本地磁盘。

与传统的 WebDAV 挂载工具不同，Yingpan 采用分块存储方案，将云端视为一个巨大的硬盘，支持直接在上面进行 `mkfs` 格式化（推荐 ext4），并像使用普通物理硬盘一样进行读写、运行程序或作为 Docker 卷使用。

## 核心特性

- **双驱动模式支持**:
  - **FUSE 模式**: 生成虚拟 `.img` 镜像文件，通过 Loop 设备挂载，兼容性好。
  - **NBD 模式**: 高性能块设备驱动，直接与 Linux 内核块层通信，效率更高。
- **分块存储管理 (Block-based)**:
  - 将磁盘切分为固定大小（默认 4MB）的块。
  - 仅在访问时按需下载对应的数据块，大幅节省流量和等待时间。
- **智能本地缓存 (LRU)**:
  - 采用 SQLite 管理元数据，支持热点数据本地缓存。
  - 可自定义最大缓存容量，自动清理最久未访问的数据块。
- **异步后台上传 (Write-Back)**:
  - 写入操作首先存入本地“脏块”区域并立即返回，由后台线程池并发异步上传至云端，确保本地 IO 体验。
- **图形化管理控制台**:
  - 内置基于 FastAPI 的 Web 界面。
  - 实时监控：上传/下载速度、队列状态、缓存占用率等。
- **系统级集成**:
  - 提供 `systemd` 服务支持，实现开机自启、故障自恢复。

## 快速部署

### 环境要求
- **操作系统**: Linux (推荐 Debian/Ubuntu/CentOS)
- **依赖工具**: `fuse3`, `nbd-client` (如需使用 NBD 模式), `e2fsprogs`
- **Python**: 3.8+

### 一键安装
1. 将本项目复制到服务器。
2. 执行部署脚本进行初始化和环境配置：
   ```bash
   cd yingpan
   sudo ./deploy.sh
   ```

## 备份与恢复 (重装系统)

Yingpan 支持在重装系统后快速恢复磁盘挂载状态。

### 1. 备份 (重装系统前)
在重装系统前，执行备份脚本打包配置和程序：
```bash
./backup.sh
```
这会生成一个 `yingpan_backup_日期.tar.gz` 文件。请将此文件下载并保存到安全的地方（如 U 盘或云盘）。

### 2. 恢复 (重装系统后)
1. 将备份包上传至新系统并解压：
   ```bash
   tar -xzf yingpan_backup_xxxx.tar.gz
   cd yingpan
   ```
2. 重新执行部署脚本：
   ```bash
   sudo ./deploy.sh
   ```
3. **自动恢复**: 部署脚本完成后，服务会自动启动并读取 `disks_config.json`，自动恢复之前所有的磁盘挂载，无需手动重新添加。

## 使用说明

### 1. 访问控制台
部署完成后，通过浏览器访问管理页面：
`http://服务器IP:8001`

### 2. 配置磁盘
在 Web 界面中“新增磁盘”，关键参数说明：
- **驱动模式**: 选择 `fuse` (更通用) 或 `nbd` (更高效)。
- **WebDAV URL**: 云盘 WebDAV 终端地址。
- **磁盘容量**: 虚拟出的硬盘总大小（例如 2048G）。
- **缓存上限**: 本地磁盘允许占用的最大缓存空间。
- **并发数**: WebDAV 上传/下载的并发线程数，建议 4-8。

### 3. 系统挂载
磁盘状态变为 `running` 后，应用会自动将其挂载到：
`/mnt/v_disks/[磁盘名]`
你可以直接在这个目录下读写文件。

## 管理服务

- **启动服务**: `sudo systemctl start yingpan`
- **停止服务**: `sudo systemctl stop yingpan`
- **查看实时日志**: `tail -f server.log`

## 项目结构
- [server.py](file:///root/yingpan/server.py): FastAPI 后端，负责 API 路由与磁盘生命周期管理。
- [block_manager.py](file:///root/yingpan/block_manager.py): 核心块管理逻辑，负责缓存控制与 WebDAV 同步。
- [v_drive.py](file:///root/yingpan/v_drive.py): FUSE 驱动实现。
- [nbd_server.py](file:///root/yingpan/nbd_server.py): NBD 驱动服务端实现。
- [index.html](file:///root/yingpan/index.html): 现代化的 Web 管理控制台。

## 注意事项
- **首次格式化**: 大容量磁盘在首次挂载时会进行 `mkfs.ext4`，可能需要 1-3 分钟，请稍后刷新状态。
- **安全性**: 建议在受信任的网络环境中使用，或为 Web 控制台添加反向代理认证。
- **NBD 模式**: 使用 NBD 模式前，请确保内核已加载 nbd 模块 (`modprobe nbd`)。
