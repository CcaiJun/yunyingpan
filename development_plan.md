# 虚拟硬盘扩展系统 (Virtual Drive Extension System) 开发规划

## 1. 项目概述 (Project Overview)

### 1.1 目标
构建一个基于 Linux (如 Debian) 的高性能虚拟文件系统，将海量外部存储（如 S3 对象存储、WebDAV、FTP 或其他网络存储）挂载为本地标准磁盘路径。

### 1.2 核心痛点解决
- **存储扩展**: 突破服务器物理硬盘限制，实现 PB 级存储空间。
- **访问速度**: 通过智能本地缓存算法，保证热点数据的本地磁盘级读取速度。
- **兼容性**: 基于 POSIX 标准，现有应用程序（如 Nginx, Plex, Docker 等）无需修改即可直接使用。
- **可编程控制**: 提供 API 接口，允许程序主动控制数据的预热（下载）和释放（删除），实现精细化存储管理。

---

## 2. 系统架构设计 (System Architecture)

系统采用 **FUSE (Filesystem in Userspace)** 技术实现，运行在用户态，安全性高且易于扩展。

### 2.1 架构分层
```mermaid
graph TD
    UserApp[用户应用程序] --> Kernel[Linux Kernel (VFS)]
    Kernel --> FUSE[FUSE Module]
    FUSE --> VFS_Interface[虚拟文件系统接口层]
    
    subgraph "Virtual Drive Application (Daemon)"
        VFS_Interface --> MetaMgr[元数据管理 (Metadata Manager)]
        VFS_Interface --> DataMgr[数据读写管理 (Data Manager)]
        
        DataMgr --> CacheMgr[缓存控制器 (Cache Manager)]
        CacheMgr --> LocalDisk[本地高速磁盘 (SSD/NVMe)]
        
        DataMgr --> StorageAdapter[后端存储适配器]
        MetaMgr --> LocalDB[本地元数据索引 (SQLite/LevelDB)]
    end
    
    StorageAdapter --> ExternalStore[外部存储 (S3/WebDAV/NAS)]
```

### 2.2 核心模块说明

#### A. 元数据管理 (Metadata Manager)
- **功能**: 维护目录树结构、文件属性（大小、权限、时间）。
- **优化**: 将远程文件列表缓存到本地数据库（如 SQLite 或 BoltDB），避免频繁请求远程 API，加速 `ls`, `stat` 等操作。

#### B. 缓存控制器 (Cache Manager) - *核心差异化功能*
- **分块缓存 (Chunking)**: 将大文件切分为固定大小的数据块（如 4MB-16MB）。
- **缓存上限管理 (Quota Management)**:
  - **软上限 (Soft Limit)**: 当缓存达到 80% 时，启动后台异步清理。
  - **硬上限 (Hard Limit)**: 当缓存达到 100% 时，写入/下载新数据前必须同步删除旧数据（LRU）。
  - **空间预留**: 确保本地磁盘始终保留一定比例的剩余空间。
- **分级策略**:
  - **Hot Data**: 经常访问的数据块保留在本地。
  - **Cold Data**: 仅保留元数据，实体数据在云端。
- **生命周期管理**:
  - `LRU (Least Recently Used)`: 自动清理最久未访问的块，释放本地空间。
  - `Write-Back`: 写入时先写入本地缓存即返回成功，后台异步上传到外部存储。

#### C. WebDAV 适配器 (WebDAV Adapter)
- **协议支持**: 支持标准的 WebDAV 协议（如 AList, Nextcloud, 坚果云等）。
- **连接池管理**: 维持长连接，减少 HTTPS 握手开销。
- **分片下载**: 利用 HTTP `Range` 头实现分块按需下载，完美契合 Chunking 机制。

#### D. API 控制接口 (Control API)
- 提供 HTTP RESTful 接口或 Socket 接口，供外部程序调用：
  - `POST /cache/prefetch`: 主动下载指定文件/目录到本地（预热）。
  - `DELETE /cache/evict`: 主动从本地删除缓存（释放空间，但不删除云端文件）。
  - `GET /stats`: 查看当前缓存占用、命中率。

---

## 3. 技术选型 (Technology Stack)

| 组件 | 推荐技术 | 理由 |
| :--- | :--- | :--- |
| **开发语言** | **Python 3** | 开发极其迅速，生态丰富，适合快速迭代逻辑。 |
| **文件系统接口** | **fusepy (FUSE 3)** | 经典的 Python FUSE 绑定，完美支持 POSIX 接口。 |
| **WebDAV 客户端** | **webdav4** 或 **requests** | 功能强大且稳定的 WebDAV 客户端库。 |
| **Web 框架** | **FastAPI** | 高性能异步框架，自带 OpenAPI 文档，适合编写控制 API。 |
| **元数据存储** | **SQLite** | Python 内置支持，无需额外部署。 |

---

## 4. 功能开发规划 (Development Roadmap) - **重构版 (Block-Based)**

### 阶段一：块存储核心 (Block Core) [进行中]
- **目标**: 提供一个由 WebDAV 支撑的超大虚拟磁盘文件 (`disk.img`)。
- **架构**:
  - **FUSE 层**: 仅暴露一个根目录和一个 `disk.img` 文件。
  - **Block Manager**: 将 `disk.img` 切分为 4MB 的 Block。
  - **Storage**: Block 压缩后存储在 WebDAV (`/blocks/blk_00001.zst`)。
  - **Cache**: 本地 LRU 缓存热点 Block。

### 阶段二：系统集成 (System Integration)
- **目标**: 将虚拟磁盘挂载为系统原生分区。
- **功能**:
  - 自动执行 `mount -o loop` 将镜像挂载到 `/mnt/data`。
  - 支持 `mkfs.ext4` 格式化虚拟磁盘。
  - 完美支持所有 POSIX 操作（因为本质是 ext4）。

### 阶段三：智能管理 (Smart Ops)
- **目标**: 优化性能与可靠性。
- **功能**:
  - **Bitmap Sync**: 记录哪些 Block 被分配，避免读取空 Block。
  - **Async Upload**: 写入操作仅更新本地 Cache，后台异步上传。
  - **Snapshot**: 利用 WebDAV 的特性实现磁盘快照。

### 阶段四：生产化与运维 (Production)
- **目标**: 稳定运行在 Debian 服务器。
- **功能**:
  - Systemd 服务文件 (`.service`)。
  - 监控指标导出 (Prometheus Metrics)。
  - 崩溃恢复 (Crash Recovery)：重启后自动扫描脏数据并上传。

---

## 5. 关键算法逻辑 (Key Logic)

### 5.1 读取流程 (Read Flow)
1. 用户程序发起 Read 请求 (offset, length)。
2. 系统计算该范围对应的 Chunk ID 列表。
3. **检查本地缓存**:
   - *Hit*: 直接从本地磁盘读取。
   - *Miss*: 阻塞请求，从外部存储下载对应 Chunk 到本地缓存目录，然后读取。
4. 返回数据给用户。
5. (可选) 触发预读机制，后台下载后续的几个 Chunk。

### 5.2 缓存控制 API 伪代码
```go
// 外部程序调用：要求缓存某个视频文件，以备稍后高速播放
func HandlePrefetch(w http.ResponseWriter, r *http.Request) {
    filePath := r.URL.Query().Get("path")
    // 异步任务：遍历文件的所有 Chunk 并下载
    go CacheEngine.Prefetch(filePath) 
    w.Write([]byte("Prefetch task started"))
}

// 外部程序调用：磁盘空间不足，要求释放特定目录
func HandleEvict(w http.ResponseWriter, r *http.Request) {
    dirPath := r.URL.Query().Get("path")
    // 仅删除本地缓存文件，保留元数据
    CacheEngine.Evict(dirPath)
    w.Write([]byte("Cache evicted"))
}
```

## 6. 预期风险与对策
1. **网络延迟**: 使用预读 (Prefetching) 策略，预测用户访问模式提前下载。
2. **数据一致性**: 采用最终一致性模型。文件上传完成前，其他客户端可能看不到最新数据。
3. **断电丢数据**: 写入时采用 WAL (Write Ahead Log) 或仅在完全上传后才确认写入（即 Write-Through 模式，更安全但慢），建议默认 Write-Back 但需用户知晓风险。

---

## 7. Web 管理界面设计 (Web Management UI)

为了方便运维和管理，系统将内置一个轻量级的 Web 控制台。

### 7.1 技术选型
- **后端**: 复用核心程序的 HTTP API 接口。
- **前端**: 单页应用 (SPA)，嵌入到二进制文件中，无需额外部署 Web 服务器。
  - 框架: Vue.js 3 (Lite) 或 Vanilla JS + TailwindCSS (CDN/Local)。
  - 部署: 编译时将 HTML/CSS/JS 资源打包进 Go/Rust 二进制文件 (使用 `embed` 特性)。

### 7.2 核心功能页面

#### A. 磁盘列表 (Disk List)
- **状态概览**:
  - 服务运行状态 (Running/Stopped)。
  - 当前挂载点路径。
  - 上行/下行 实时带宽。
- **存储监控**:
  - 本地缓存占用率 (Progress Bar: 20GB / 100GB)。
  - 云端存储预估用量。
  - 待上传队列长度 (Pending Uploads)。

#### B. 挂载配置 (Mount Settings)
- **存储源设置**:
  - 协议选择: S3 / WebDAV / Local (Test)。
  - 认证信息: Endpoint, AccessKey, SecretKey, Bucket。
- **本地策略**:
  - 挂载路径 (如 `/mnt/cloud_drive`)。
  - 缓存目录路径 (如 `/var/cache/cloud_drive`)。
  - 最大缓存容量限制。
  - 读写模式 (Read-only / Read-Write)。

#### C. 文件缓存管理 (Cache Explorer)
- **文件树视图**: 浏览虚拟硬盘中的文件结构。
- **状态标记**:
  - 🟢 **Cached**: 完全在本地。
  - 🟡 **Partial**: 部分块在本地。
  - ☁️ **Cloud**: 仅元数据在本地，内容在云端。
- **操作菜单**:
  - `Pin to Local`: 永久保存在本地，不受自动清理影响。
  - `Preload`: 立即下载。
  - `Evict`: 立即释放本地空间。


