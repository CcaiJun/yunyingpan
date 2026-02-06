#!/usr/bin/env python3
import os
import sqlite3
import time
import logging
import threading
import collections
import json
import zstandard as zstd
import lz4.frame
from webdav4.client import Client as WebDAVClient
from httpx import Timeout, Limits

logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, limit_kb_s):
        self.limit = limit_kb_s * 1024 if limit_kb_s > 0 else 0
        # 初始令牌设为 0，防止刚开始时出现巨大的突发流量
        self.tokens = 0
        # 令牌桶最大容量：限速值的一半，或者至少 1MB
        # 减小桶大小可以显著减少“传输一阵停一阵”的现象
        self.max_tokens = max(self.limit * 0.5, 1024 * 1024)
        self.last_update = time.time()
        self.lock = threading.Lock()

    def set_limit(self, limit_kb_s):
        with self.lock:
            self.limit = limit_kb_s * 1024 if limit_kb_s > 0 else 0
            self.max_tokens = max(self.limit * 0.5, 1024 * 1024)
            # 调整限速时重置令牌，避免突发
            self.tokens = 0
            self.last_update = time.time()

    def request(self, amount):
        if self.limit <= 0:
            return

        while amount > 0:
            wait_time = 0
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                # 补充令牌，但不超过 max_tokens
                self.tokens = min(self.max_tokens, self.tokens + elapsed * self.limit)
                self.last_update = now

                if self.tokens > 0:
                    # 尽可能消耗现有令牌
                    consume = min(amount, self.tokens)
                    self.tokens -= consume
                    amount -= consume
                
                if amount > 0:
                    # 计算还需要等待的时间，但限制单次等待时长以保持响应性
                    wait_time = min(amount / self.limit, 0.1)
            
            if wait_time > 0:
                time.sleep(wait_time)

class MetadataDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS blocks (
                    block_id INTEGER PRIMARY KEY,
                    status TEXT, -- 'empty', 'cached', 'dirty', 'uploading'
                    remote_exists INTEGER DEFAULT 0, -- 0: no, 1: yes
                    last_access REAL
                )
            ''')
            cursor = conn.execute("PRAGMA table_info(blocks)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'remote_exists' not in columns:
                conn.execute('ALTER TABLE blocks ADD COLUMN remote_exists INTEGER DEFAULT 0')
            conn.commit()

    def set_block_status(self, block_id, status, remote_exists=None):
        with sqlite3.connect(self.db_path) as conn:
            if remote_exists is not None:
                conn.execute('INSERT OR REPLACE INTO blocks (block_id, status, remote_exists, last_access) VALUES (?, ?, ?, ?)',
                            (block_id, status, remote_exists, time.time()))
            else:
                conn.execute('''
                    INSERT INTO blocks (block_id, status, last_access) VALUES (?, ?, ?)
                    ON CONFLICT(block_id) DO UPDATE SET status=excluded.status, last_access=excluded.last_access
                ''', (block_id, status, time.time()))
            conn.commit()

    def batch_set_remote_exists(self, block_ids):
        """批量更新远程存在标记，优化还原扫描性能"""
        if not block_ids:
            return
        with sqlite3.connect(self.db_path) as conn:
            now = time.time()
            # 使用 executemany 进行批量插入/更新，并在一个事务中提交
            conn.executemany('''
                INSERT INTO blocks (block_id, status, remote_exists, last_access) 
                VALUES (?, 'empty', 1, ?)
                ON CONFLICT(block_id) DO UPDATE SET remote_exists=1, last_access=excluded.last_access
            ''', [(bid, now) for bid in block_ids])
            conn.commit()

    def get_block_info(self, block_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT status, remote_exists FROM blocks WHERE block_id = ?', (block_id,))
            row = cursor.fetchone()
            if row:
                return row[0], row[1]
            return 'empty', 0

    def get_lru_blocks(self, limit):
        with sqlite3.connect(self.db_path) as conn:
            # 增加保护：不删除最近 60 秒内访问过的块，防止误删刚下载或正在使用的块
            cutoff_time = time.time() - 60
            cursor = conn.execute('SELECT block_id FROM blocks WHERE status = "cached" AND last_access < ? ORDER BY last_access ASC LIMIT ?', (cutoff_time, limit))
            return [row[0] for row in cursor.fetchall()]

    def get_dirty_blocks(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT block_id FROM blocks WHERE status = "dirty"')
            return [row[0] for row in cursor.fetchall()]

    def get_remote_exists_count(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM blocks WHERE remote_exists = 1')
            return cursor.fetchone()[0]

    def touch_block(self, block_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('UPDATE blocks SET last_access = ? WHERE block_id = ?', (time.time(), block_id))
            conn.commit()

    def batch_set_block_status(self, block_ids, status, remote_exists=None):
        """批量更新块状态，优化清理性能"""
        if not block_ids:
            return
        with sqlite3.connect(self.db_path) as conn:
            if remote_exists is not None:
                conn.executemany('INSERT OR REPLACE INTO blocks (block_id, status, remote_exists, last_access) VALUES (?, ?, ?, ?)',
                            [(bid, status, remote_exists, time.time()) for bid in block_ids])
            else:
                conn.executemany('''
                    INSERT INTO blocks (block_id, status, last_access) VALUES (?, ?, ?)
                    ON CONFLICT(block_id) DO UPDATE SET status=excluded.status, last_access=excluded.last_access
                ''', [(bid, status, time.time()) for bid in block_ids])
            conn.commit()

class BlockManager:
    def __init__(self, dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_size_gb, 
                 block_size_mb=4, img_name="virtual_disk.img", remote_path="blocks", concurrency=4,
                 compression="none", compression_level=3, upload_limit_kb=0, download_limit_kb=0):
        self.use_remote = bool(dav_url and dav_url.strip())
        if self.use_remote:
            limits = Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
            self.client = WebDAVClient(
                dav_url, 
                auth=(dav_user, dav_password), 
                timeout=Timeout(60.0, connect=20.0),
                limits=limits,
                follow_redirects=True
            )
        else:
            self.client = None

        self.cache_dir = os.path.abspath(cache_dir)
        self.disk_size = disk_size_gb * 1024 * 1024 * 1024
        self.max_cache_size = max_cache_size_gb * 1024 * 1024 * 1024
        self.block_size = block_size_mb * 1024 * 1024
        self.img_name = img_name if img_name.endswith('.img') else f"{img_name}.img"
        self.remote_path = remote_path.strip('/')
        self.concurrency = concurrency
        self.compression = compression
        self.compression_level = compression_level
        
        # 速率限制器压缩器初始化
        if self.compression == "zstd":
            self.cctx = zstd.ZstdCompressor(level=self.compression_level)
            self.dctx = zstd.ZstdDecompressor()
            self.cctx_lock = threading.Lock()
            self.dctx_lock = threading.Lock()
        
        # 速率限制器
        self.upload_limiter = RateLimiter(upload_limit_kb)
        self.download_limiter = RateLimiter(download_limit_kb)
        
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        self.db = MetadataDB(os.path.join(self.cache_dir, f'.{self.img_name}_metadata.db'))
        self.wal_path = os.path.join(self.cache_dir, f'.{self.img_name}_wal.log') # WAL 日志路径
        self.upload_queue = set()
        self.uploading_count = 0
        self.downloading_count = 0
        self.uploading_lock = threading.Lock()
        self.downloading_lock = threading.Lock()
        self.upload_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self._remote_dirs_checked = False
        self.scan_error = None # 记录扫描过程中的错误，用于上层判断安全性
        self.index_dirty = False # 标记索引是否需要更新
        self.last_index_save = time.time()
        
        # 恢复 WAL
        self._recover_from_wal()
        
        # 速度追踪
        self.total_downloaded_bytes = 0
        self.total_uploaded_bytes = 0
        self.download_speed = 0 # bytes/s
        self.upload_speed = 0   # bytes/s
        self._last_speed_check = time.time()
        self._last_downloaded = 0
        self._last_uploaded = 0
        
        # 滑动窗口平均速度
        self._speed_window_size = 5 # 记录最近 5 次（约 10 秒）的快照
        self._download_history = collections.deque(maxlen=self._speed_window_size)
        self._upload_history = collections.deque(maxlen=self._speed_window_size)
        
        dirty_blocks = self.db.get_dirty_blocks()
        if dirty_blocks:
            for bid in dirty_blocks:
                self.upload_queue.add(bid)
                
        for _ in range(self.concurrency):
            threading.Thread(target=self._upload_worker, daemon=True).start()
        
        threading.Thread(target=self._cache_worker, daemon=True).start()
        threading.Thread(target=self._speed_worker, daemon=True).start()
        threading.Thread(target=self._index_worker, daemon=True).start()
        
        # 如果是新连接的磁盘（数据库中没有远程块记录），启动一个后台线程扫描远程已有的块
        if self.use_remote:
            if self.db.get_remote_exists_count() > 0:
                self._remote_dirs_checked = True
                # 仍然可以在后台扫描一次，以防远程有更新，但不需要阻塞启动
                threading.Thread(target=self._scan_remote_blocks, daemon=True).start()
            else:
                threading.Thread(target=self._scan_remote_blocks, daemon=True).start()

    def _recover_from_wal(self):
        """从 WAL 日志恢复未完成的上传任务"""
        try:
            if not os.path.exists(self.wal_path):
                return
            
            logger.info(f"Recovering from WAL: {self.wal_path}")
            recovered_count = 0
            with open(self.wal_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try:
                        # WAL 格式: "PENDING:123" 或 "COMPLETED:123"
                        # 简单的逻辑：如果最后的状态不是 COMPLETED，则重新加入队列
                        # 但这里我们只记录 PENDING 的，如果重启了，所有在 WAL 里的都视为可能未完成
                        # 更精确的做法是：每次上传前 append PENDING，成功后 append COMPLETED
                        # 然后在此处分析。为了简化，我们只记录 "需要上传的块"，如果重启还在，就重新检查
                        if line.startswith("PENDING:"):
                            bid = int(line.split(":")[1])
                            # 检查是否真的需要上传（可能已经传了但没记 COMPLETED，或者本地已经没了）
                            # 简单起见，直接加入 dirty 队列，由 upload_worker 去判断
                            if self.db.is_dirty(bid): # 只有数据库也认为是 dirty 的才加
                                self.upload_queue.add(bid)
                                recovered_count += 1
                    except: pass
            
            if recovered_count > 0:
                logger.warning(f"Recovered {recovered_count} blocks from WAL")
                
            # 恢复完后，是否清空 WAL？
            # 这里的 WAL 是 append-only 的，如果不清理会无限增长
            # 应该在每次全量 flush 后清理，或者定期清理
            # 暂时策略：启动时读取完后，清空它（因为都已经加入内存 queue 了）
            # open(self.wal_path, 'w').close() 
            # 不，清空不安全。如果 crash 了内存 queue 也没了。
            # 正确做法：upload_worker 完成后会从 set 移除。
            # WAL 应该只在 add_to_queue 时写入。
            # 简化版 WAL：只在程序非正常退出时有用。正常退出应该为空？
            # 让我们实现一个简单的：每次 write 操作加入 dirty 时写入 WAL。
            # 启动时读取。
            # 什么时候清理？ -> _index_worker 保存索引成功后（意味着所有之前的 dirty 都处理或持久化了）？
            # 或者：每次成功上传一个块，都在 WAL 记一笔？太慢。
            # 
            # 改进策略：
            # 1. WAL 仅用于记录 "用户写入了数据，但尚未上传" 的意图。
            # 2. 实际上 sqlite 的 dirty_blocks 已经是持久化的 WAL 了！
            # 3. self.db.get_dirty_blocks() 在 __init__ 里已经调用了。
            # 
            # 也就是说，只要 sqlite 不坏，dirty 状态就不会丢。
            # 既然如此，额外的文本 WAL 意义何在？
            # 意义在于：sqlite 的写入可能滞后？不，sqlite 是事务的。
            # 
            # 除非：我们在 write() 函数里，是先写文件，后写 sqlite。
            # 如果写了文件，没写 sqlite 就断电 -> 孤儿文件，不影响一致性（因为读不到）。
            # 如果写了 sqlite (dirty=1)，没写文件 -> 再次上传时会报错（读不到文件），或者读到旧文件。
            #
            # 真正的 WAL 需求是：
            # "提示用户数据可能丢失"
            # 我们可以检查：是否存在 dirty=1 的块，但在本地 cache 目录里找不到对应的文件？
            # 这意味着元数据说有新数据，但数据本体丢了（缓存没刷盘）。
            
            dirty_blocks = self.db.get_dirty_blocks()
            missing_blocks = []
            for bid in dirty_blocks:
                block_path = os.path.join(self.cache_dir, str(bid))
                if not os.path.exists(block_path):
                    missing_blocks.append(bid)
            
            if missing_blocks:
                logger.error(f"CRITICAL: Found {len(missing_blocks)} blocks marked as dirty but missing on disk! Data loss detected.")
                # 可以生成一个警告文件供用户查看
                with open(os.path.join(self.cache_dir, "DATA_LOSS_WARNING.txt"), "w") as w:
                    w.write(f"Detected {len(missing_blocks)} missing data blocks. IDs: {missing_blocks}\n")
                    w.write("This usually happens due to power loss before data was flushed to disk.\n")
                
                # 尝试修复：重置这些块为 dirty=0 (放弃修改，回滚到云端版本) ?
                # 或者保留 dirty=1 让上传线程去处理（会失败）
                # 安全起见，我们不做自动回滚，只报警。
                
        except Exception as e:
            logger.error(f"WAL recovery failed: {e}")

    def _index_worker(self):
        """定期保存索引的后台线程"""
        logger.info(f"Index worker started for {self.img_name}")
        while True:
            try:
                # 每 30 秒检查一次是否需要保存索引
                time.sleep(30)
                if self.index_dirty:
                    # 只有当距离上次保存超过 30 秒，或者距离上次上传超过 30 秒时才保存
                    # 避免在连续上传时频繁保存索引文件
                    if time.time() - self.last_index_save >= 30:
                        logger.info(f"Periodic index save triggered for {self.img_name}")
                        self._save_remote_index()
            except Exception as e:
                logger.error(f"Index worker error for {self.img_name}: {e}")

    def _load_remote_index(self):
        """尝试从云端加载块索引文件 block_index.json (支持回滚机制)"""
        if not self.use_remote: return False
        
        candidates = [f"{self.remote_path}/block_index.json"]
        
        # 尝试读取 pointer 文件获取最新版本作为备选
        try:
            bio_ptr = io.BytesIO()
            self.client.download_fileobj(f"{self.remote_path}/latest_index_pointer.json", bio_ptr)
            bio_ptr.seek(0)
            ptr_data = json.load(bio_ptr)
            if "latest" in ptr_data:
                candidates.append(ptr_data["latest"])
        except: pass
        
        # 如果 pointer 失败，尝试列出目录找最新的几个 index
        try:
             files = self.client.ls(self.remote_path, detail=False)
             index_files = []
             for f in files:
                 fname = os.path.basename(f)
                 if fname.startswith("block_index_") and fname.endswith(".json"):
                     try:
                         ts = int(fname.replace("block_index_", "").replace(".json", ""))
                         index_files.append((ts, f))
                     except: pass
             index_files.sort(key=lambda x: x[0], reverse=True)
             for _, fpath in index_files[:3]: # 取最近3个
                 if fpath not in candidates:
                     candidates.append(fpath)
        except: pass
        
        for index_path in candidates:
            try:
                import io
                bio = io.BytesIO()
                # 尝试下载索引文件
                logger.info(f"Trying to load remote index from {index_path}...")
                self.client.download_fileobj(index_path, bio)
                bio.seek(0)
                data = json.load(bio)
                
                if isinstance(data, list):
                    # 校验数据格式
                    block_ids = [int(x) for x in data if isinstance(x, (int, str)) and str(x).isdigit()]
                    logger.info(f"Loaded remote index with {len(block_ids)} blocks for {self.img_name}")
                    if block_ids:
                        self.db.batch_set_remote_exists(block_ids)
                    return True
            except Exception as e:
                # 404 是正常情况（新盘或旧盘未生成索引）
                if "404" not in str(e) and "Not Found" not in str(e):
                    logger.warning(f"Failed to load remote index from {index_path}: {e}")
                continue # 尝试下一个
                
        return False

    def _save_remote_index(self):
        """将当前已知的远程块列表保存到 block_index.json (原子更新 + 多版本)"""
        if not self.use_remote: return
        
        try:
            # 获取所有 remote_exists=1 的块ID
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.execute('SELECT block_id FROM blocks WHERE remote_exists = 1')
                block_ids = [row[0] for row in cursor.fetchall()]
            
            import io
            data = json.dumps(block_ids)
            data_bytes = data.encode('utf-8')
            
            # 1. 原子更新：先上传到 .tmp，然后 MOVE
            # 这样可以防止上传过程中断导致 index 文件损坏
            tmp_path = f"{self.remote_path}/block_index.json.tmp"
            final_path = f"{self.remote_path}/block_index.json"
            
            bio = io.BytesIO(data_bytes)
            self.client.upload_fileobj(bio, tmp_path, overwrite=True)
            self.client.move(remote_path_from=tmp_path, remote_path_to=final_path, overwrite=True)
            
            # 2. 多版本备份 (Index Versioning)
            # 生成带时间戳的备份文件，如 block_index_1700000000.json
            timestamp = int(time.time())
            version_path = f"{self.remote_path}/block_index_{timestamp}.json"
            
            # 我们可以直接 COPY 刚生成的 final_path，或者重新上传一份
            # 由于 COPY 可能不被支持 (参考之前的测试)，我们选择直接上传一份新的
            # 虽然多耗费流量，但 index 文件通常不大（几MB），且能保证兼容性
            bio_ver = io.BytesIO(data_bytes)
            self.client.upload_fileobj(bio_ver, version_path, overwrite=True)
            
            # 更新 pointer 文件 (指向最新的版本)
            pointer_content = json.dumps({"latest": version_path, "timestamp": timestamp})
            self.client.upload_fileobj(io.BytesIO(pointer_content.encode('utf-8')), f"{self.remote_path}/latest_index_pointer.json", overwrite=True)
            
            # 3. 清理旧版本 (保留最近 5 个)
            # 这是一个后台任务，不需要每次都做，或者可以异步做
            # 为了简化，我们每次都检查一下
            try:
                files = self.client.ls(self.remote_path, detail=False)
                index_files = []
                for f in files:
                    fname = os.path.basename(f)
                    if fname.startswith("block_index_") and fname.endswith(".json") and fname != "block_index.json":
                        try:
                            ts = int(fname.replace("block_index_", "").replace(".json", ""))
                            index_files.append((ts, f))
                        except: pass
                
                # 按时间倒序排列
                index_files.sort(key=lambda x: x[0], reverse=True)
                
                # 保留最新的 5 个，删除其余的
                if len(index_files) > 5:
                    for ts, fpath in index_files[5:]:
                        logger.info(f"Cleaning up old index version: {fpath}")
                        try:
                            self.client.clean(fpath) # webdav4 client use clean? No, usually delete or remove. Let's check api.
                            # client.remove is the standard one? webdav4 client has no 'clean' method in snippets, but 'delete' or 'remove'?
                            # Let's use a try-catch block with known methods
                            if hasattr(self.client, 'remove'):
                                self.client.remove(fpath)
                            elif hasattr(self.client, 'delete'):
                                self.client.delete(fpath)
                        except Exception as clean_err:
                            logger.warning(f"Failed to clean old index {fpath}: {clean_err}")
            except Exception as e:
                logger.warning(f"Index cleanup failed: {e}")

            logger.info(f"Saved remote index (atomic+versioned) with {len(block_ids)} blocks for {self.img_name}")
            self.last_index_save = time.time()
            self.index_dirty = False
        except Exception as e:
            logger.error(f"Failed to save remote index for {self.img_name}: {e}")

    def _scan_remote_blocks(self):
        try:
            logger.info(f"Scanning remote blocks for {self.img_name} in {self.remote_path}...")
            
            # 0. 优先尝试加载索引文件 (Fast Path)
            if self._load_remote_index():
                logger.info(f"Index loaded successfully, skipping full scan for {self.img_name}")
                self._remote_dirs_checked = True
                
                # 预热逻辑：主动触发前 2 个块的下载
                # 即使是索引加载的，也预热一下
                threading.Thread(target=self.read, args=(self.block_size, 0), daemon=True).start()
                return

            # 1. 一次性获取远程文件列表 (WebDAV ls 通常是一个 PROPFIND 请求)
            max_retries = 3
            files = []
            for attempt in range(1, max_retries + 1):
                try:
                    files = self.client.ls(self.remote_path, detail=False)
                    break
                except Exception as ls_err:
                    logger.error(f"Failed to list remote directory {self.remote_path} (attempt {attempt}/{max_retries}): {ls_err}")
                    if attempt == max_retries:
                        # 只有最后一次尝试失败才尝试 mkdir
                        try:
                            self.client.mkdir(self.remote_path)
                        except: pass
                    else:
                        time.sleep(2) # 重试前稍等
            
            remote_block_ids = []
            for f in files:
                filename = os.path.basename(f)
                if filename.startswith("blk_") and filename.endswith(".dat"):
                    try:
                        block_id = int(filename[4:-4])
                        remote_block_ids.append(block_id)
                    except: continue
            
            if remote_block_ids:
                # 2. 批量更新数据库 (单个事务)
                self.db.batch_set_remote_exists(remote_block_ids)
                logger.info(f"Scan complete: found and indexed {len(remote_block_ids)} existing remote blocks for {self.img_name}")
                
                # 扫描完成后，立即生成并上传索引文件，以便下次快速启动
                threading.Thread(target=self._save_remote_index, daemon=True).start()
                
                # 预热逻辑：主动触发前 2 个块的下载（通常包含文件系统超级块和根 Inode）
                # 这会让挂载后的第一次 ls 变得飞快
                for i in range(min(2, len(remote_block_ids))):
                    bid = sorted(remote_block_ids)[i]
                    if bid < 5: # 只预热最开头的几个元数据块
                        threading.Thread(target=self.read, args=(self.block_size, bid * self.block_size), daemon=True).start()
            else:
                logger.info(f"Scan complete: no remote blocks found for {self.img_name}")
                # 即使没有找到块，也标记为脏，以便 index_worker 或 unmount 时保存一个初始的空索引文件
                self.index_dirty = True
                # 也可以立即保存一次，让用户在云端能看到
                threading.Thread(target=self._save_remote_index, daemon=True).start()
            
        except Exception as e:
            logger.error(f"Failed to scan remote blocks: {e}")
            self.scan_error = str(e)
        finally:
            # 标记扫描完成，无论是否发现文件，或者是否出错
            # 这样可以防止 server.py 启动时卡死在等待扫描上
            self._remote_dirs_checked = True

    def _speed_worker(self):
        while True:
            time.sleep(2)
            now = time.time()
            duration = now - self._last_speed_check
            if duration > 0:
                curr_down = (self.total_downloaded_bytes - self._last_downloaded) / duration
                curr_up = (self.total_uploaded_bytes - self._last_uploaded) / duration
                
                # 添加到历史记录
                self._download_history.append(curr_down)
                self._upload_history.append(curr_up)
                
                # 计算平均值以实现平滑显示
                self.download_speed = sum(self._download_history) / len(self._download_history)
                self.upload_speed = sum(self._upload_history) / len(self._upload_history)
                
                if self.download_speed > 0 or self.upload_speed > 0:
                    logger.info(f"BM Speed Update (Smoothed): DOWN={self.download_speed:.2f}, UP={self.upload_speed:.2f}")
                
                self._last_downloaded = self.total_downloaded_bytes
                self._last_uploaded = self.total_uploaded_bytes
                self._last_speed_check = now

    def _get_block_path(self, block_id):
        return os.path.join(self.cache_dir, f"{self.img_name}_blk_{block_id:08d}.dat")

    def _is_all_zeros(self, path):
        try:
            if not os.path.exists(path): return True
            if os.path.getsize(path) == 0: return True
            stat_info = os.stat(path)
            if stat_info.st_blocks == 0: return True
            with open(path, 'rb') as f:
                chunk = f.read(4096)
                if any(chunk): return False
                f.seek(0)
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk: break
                    if any(chunk): return False
            return True
        except: return False

    def has_remote_data(self):
        if not self.use_remote:
            return False
        # 移除 try-except，让异常抛出以便上层感知网络错误
        # 检查远程目录下是否存在任何 block 文件
        files = self.client.ls(self.remote_path, detail=False)
        for f in files:
            if "blk_" in f:
                return True
        return False

    def _upload_worker(self):
        logger.info(f"Upload worker started for {self.img_name}, use_remote={self.use_remote}")
        while True:
            try:
                if not self.use_remote:
                    time.sleep(5)
                    continue
                block_id = None
                with self.upload_lock:
                    if self.upload_queue:
                        # 使用 pop() 移除并返回一个元素
                        block_id = self.upload_queue.pop()
                        # logger.debug(f"Popped block {block_id} from queue, remaining: {len(self.upload_queue)}")
                
                if block_id is None:
                    time.sleep(0.1)
                    continue

                self.db.set_block_status(block_id, 'uploading')
                with self.uploading_lock: self.uploading_count += 1
                try:
                    block_path = self._get_block_path(block_id)
                    # logger.debug(f"Processing upload for block {block_id}")
                    status, remote_exists = self.db.get_block_info(block_id)
                    if os.path.exists(block_path):
                        remote_file_path = f"{self.remote_path}/blk_{block_id:08d}.dat"
                        if self._is_all_zeros(block_path):
                            if remote_exists:
                                try:
                                    self.client.remove(remote_file_path)
                                    self.db.set_block_status(block_id, 'cached', remote_exists=0)
                                except: pass
                            else:
                                self.db.set_block_status(block_id, 'cached', remote_exists=0)
                            # 处理完成，不需要放回队列
                        else:
                            if not self._remote_dirs_checked:
                                # 这里不再加 upload_lock，避免死锁，改用专门的目录检查锁或简单的双重检查
                                if not self._remote_dirs_checked:
                                    parts = self.remote_path.split('/')
                                    current = ""
                                    for part in parts:
                                        current = f"{current}/{part}".strip('/')
                                        try:
                                            self.client.mkdir(current)
                                        except: pass
                                    self._remote_dirs_checked = True
                            try:
                                max_retries = 3
                                for attempt in range(max_retries + 1):
                                    try:
                                        if attempt > 0:
                                            import random
                                            time.sleep(random.uniform(0.5, 2.0))
                                        
                                        # 使用回调函数实时更新上传流量
                                        last_transferred = 0
                                        def upload_callback(transferred):
                                            nonlocal last_transferred
                                            diff = transferred - last_transferred
                                            if diff > 0:
                                                self.upload_limiter.request(diff)
                                                with self.stats_lock:
                                                    self.total_uploaded_bytes += diff
                                            last_transferred = transferred
                                        
                                        # 处理压缩逻辑
                                        if self.compression == "none":
                                            self.client.upload_file(block_path, remote_file_path, overwrite=True, callback=upload_callback)
                                        else:
                                            with open(block_path, 'rb') as f:
                                                raw_data = f.read()
                                            
                                            if self.compression == "zstd":
                                                with self.cctx_lock:
                                                    processed_data = self.cctx.compress(raw_data)
                                            elif self.compression == "lz4":
                                                processed_data = lz4.frame.compress(raw_data, compression_level=self.compression_level)
                                            
                                            # 上传内存中的压缩数据
                                            import io
                                            data_stream = io.BytesIO(processed_data)
                                            self.client.upload_fileobj(data_stream, remote_file_path, overwrite=True, callback=upload_callback)
                                        
                                        self.db.set_block_status(block_id, 'cached', remote_exists=1)
                                        self.index_dirty = True
                                        # logger.info(f"Successfully uploaded block {block_id}")
                                        break
                                    except Exception as e:
                                        if attempt == max_retries: raise e
                            except Exception as upload_err:
                                logger.error(f"Upload failed for block {block_id}: {upload_err}")
                                # 只有真正失败才放回队列
                                with self.upload_lock: self.upload_queue.add(block_id)
                                # 休息一下避免死循环重试
                                time.sleep(2)
                    else:
                         # 如果文件不存在，可能是被删除了，记录个日志
                         logger.warning(f"Block {block_id} file not found during upload, skipping.")
                except Exception as e:
                    logger.error(f"Block {block_id} upload process failed: {e}")
                    with self.upload_lock: self.upload_queue.add(block_id)
                finally:
                    with self.uploading_lock: self.uploading_count -= 1
            except Exception as outer_e:
                logger.error(f"Upload worker crashed: {outer_e}")
                time.sleep(1)

    def _cache_worker(self):
        logger.info(f"Cache worker started for {self.img_name}, limit={self.max_cache_size/1024/1024:.1f}MB")
        while True:
            try:
                # logger.debug("Cache worker cycle check...") 
                current_real_size = 0
                # 优化：使用 scandir 替代 listdir + stat，提高性能
                with os.scandir(self.cache_dir) as it:
                    for entry in it:
                        if entry.name.startswith(f"{self.img_name}_blk_") and entry.is_file():
                            try:
                                stat_info = entry.stat()
                                f_real_size = stat_info.st_blocks * 512
                                # 移除高频的全 0 检查，避免产生巨大的读 IO
                                current_real_size += f_real_size
                            except: pass
                
                # 提前清理：当达到 90% 容量时就开始触发清理，从最久未访问的文件开始清理
                cleanup_threshold = self.max_cache_size * 0.9
                if current_real_size > cleanup_threshold:
                    over_size = current_real_size - cleanup_threshold
                    logger.info(f"Cache over 90% threshold ({current_real_size/1024/1024:.1f}MB > {cleanup_threshold/1024/1024:.1f}MB), need to free {over_size/1024/1024:.1f}MB")
                    
                    freed_size = 0
                    total_deleted_count = 0
                    max_delete_limit = 2000 # 单次最大清理文件数，防止阻塞太久
                    
                    while freed_size < over_size and total_deleted_count < max_delete_limit:
                        # 每次取 50 个块进行处理
                        lru_blocks = self.db.get_lru_blocks(50)
                        if not lru_blocks:
                            logger.warning("Cache over limit but no cached blocks found in DB (possible sync issue)")
                            break
                        
                        batch_deleted_ids = []
                        for bid in lru_blocks:
                            path = self._get_block_path(bid)
                            real_size = 0
                            if os.path.exists(path):
                                try:
                                    stat_info = os.stat(path)
                                    real_size = stat_info.st_blocks * 512
                                    os.remove(path)
                                except Exception as e:
                                    logger.error(f"Failed to remove cache block {bid}: {e}")
                            
                            # 无论文件是否存在（可能已经被手动删了），都标记为从缓存清除
                            freed_size += real_size
                            batch_deleted_ids.append(bid)
                            
                            if freed_size >= over_size:
                                break
                        
                        if batch_deleted_ids:
                            # 批量更新 DB，这会使 get_lru_blocks 在下一次迭代返回新的块
                            self.db.batch_set_block_status(batch_deleted_ids, 'empty')
                            total_deleted_count += len(batch_deleted_ids)
                        else:
                            break # 防止死循环

                    logger.info(f"Cache cleanup finished: freed {freed_size/1024/1024:.1f}MB, deleted {total_deleted_count} blocks")
                
                # 即使没有超过限制，也要休息一下，避免 100% CPU 占用
                time.sleep(10)
            except Exception as e:
                logger.error(f"Cache worker error: {e}")
                time.sleep(10)

    def read(self, length, offset):
        result = bytearray(length)
        bytes_read = 0
        while bytes_read < length:
            curr_offset = offset + bytes_read
            block_id = curr_offset // self.block_size
            block_offset = curr_offset % self.block_size
            chunk_len = min(length - bytes_read, self.block_size - block_offset)
            block_path = self._get_block_path(block_id)
            status, remote_exists = self.db.get_block_info(block_id)
            if not os.path.exists(block_path):
                # 优化：如果远程扫描已完成，且数据库显示远程不存在，直接返回全0，避免大量 404 请求
                if self._remote_dirs_checked and not remote_exists and status != 'uploading':
                    result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                    bytes_read += chunk_len
                    continue

                # 优化策略：不进行预先检查，直接尝试下载
                # 这样可以减少一次 PROPFIND 请求 (RTT)，如果文件不存在，下载会返回 404
                if self.use_remote:
                    max_retries = 3
                    download_success = False
                    for attempt in range(1, max_retries + 1):
                        try:
                            with self.downloading_lock: self.downloading_count += 1
                            
                            # 使用回调函数实时更新下载流量
                            last_transferred = 0
                            def download_callback(transferred):
                                nonlocal last_transferred
                                diff = transferred - last_transferred
                                if diff > 0:
                                    self.download_limiter.request(diff)
                                    with self.stats_lock:
                                        self.total_downloaded_bytes += diff
                                last_transferred = transferred
                            
                            if self.compression == "none":
                                # 回滚为 download_file 以确保最佳兼容性和性能
                                self.client.download_file(f"{self.remote_path}/blk_{block_id:08d}.dat", block_path, callback=download_callback)
                            else:
                                # 对于压缩模式，暂时保留原逻辑，或者也改成流式处理（稍微复杂点）
                                # 为了保持一致性，也改成流式
                                with self.client.http.stream("GET", f"{self.remote_path}/blk_{block_id:08d}.dat", follow_redirects=True) as response:
                                    if response.status_code == 404:
                                        raise Exception("404 Not Found")
                                    response.raise_for_status()
                                    
                                    compressed_data = b""
                                    for chunk in response.iter_bytes():
                                        compressed_data += chunk
                                        if download_callback:
                                            download_callback(len(compressed_data))
                                    
                                    if self.compression == "zstd":
                                        with self.dctx_lock:
                                            raw_data = self.dctx.decompress(compressed_data)
                                    elif self.compression == "lz4":
                                        raw_data = lz4.frame.decompress(compressed_data)
                                    else:
                                        raw_data = compressed_data
                                    
                                    with open(block_path, 'wb') as f:
                                        f.write(raw_data)

                            self.db.set_block_status(block_id, 'cached', remote_exists=1)
                            download_success = True
                            break
                        except Exception as e:
                            # 检查是否是 404 错误
                            error_str = str(e)
                            if "404" in error_str or "Not Found" in error_str:
                                # 文件不存在，不需要重试
                                break
                            
                            logger.error(f"Download block {block_id} failed (attempt {attempt}/{max_retries}): {e}")
                            if attempt < max_retries:
                                time.sleep(1)
                        finally:
                            with self.downloading_lock: self.downloading_count -= 1
                    
                    if not download_success:
                        result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                        bytes_read += chunk_len
                        # 标记为 empty，但不一定更新 remote_exists=0，因为可能是网络错误
                        # 只有确认是 404 才更新 remote_exists=0，这里简化处理，暂不更新数据库的 remote_exists，以免误判
                        # 或者如果明确是 404，可以更新。
                        continue
                else:
                    result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                    bytes_read += chunk_len
                    self.db.set_block_status(block_id, 'empty', remote_exists=0)
                    continue
            try:
                with open(block_path, 'rb') as f:
                    f.seek(block_offset)
                    data = f.read(chunk_len)
                    if not data: data = b'\x00' * chunk_len
                    result[bytes_read:bytes_read+len(data)] = data
                    bytes_read += len(data)
            except Exception as e:
                logger.error(f"Read block {block_id} file failed: {e}")
                result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                bytes_read += chunk_len
            
            # 更新访问时间
            self.db.touch_block(block_id)
        return bytes(result)

    def write(self, buf, offset):
        length = len(buf)
        bytes_written = 0
        while bytes_written < length:
            curr_offset = offset + bytes_written
            block_id = curr_offset // self.block_size
            block_offset = curr_offset % self.block_size
            chunk_len = min(length - bytes_written, self.block_size - block_offset)
            block_path = self._get_block_path(block_id)
            status, remote_exists = self.db.get_block_info(block_id)
            if not os.path.exists(block_path):
                # 只有在初次启动且没有进行过远程扫描时才调用 exists 检查
                remote_exists_check = remote_exists
                if not remote_exists and self.use_remote and not self._remote_dirs_checked:
                    try:
                        remote_exists_check = self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat")
                    except:
                        remote_exists_check = False
                
                if self.use_remote and remote_exists_check:
                    try:
                        with self.downloading_lock: self.downloading_count += 1
                        
                        # 使用回调函数实时更新下载流量
                        last_transferred = 0
                        def download_callback(transferred):
                            nonlocal last_transferred
                            diff = transferred - last_transferred
                            if diff > 0:
                                self.download_limiter.request(diff)
                                with self.stats_lock:
                                    self.total_downloaded_bytes += diff
                            last_transferred = transferred
                        
                        if self.compression == "none":
                            self.client.download_file(f"{self.remote_path}/blk_{block_id:08d}.dat", block_path, callback=download_callback)
                        else:
                            import io
                            data_stream = io.BytesIO()
                            self.client.download_fileobj(f"{self.remote_path}/blk_{block_id:08d}.dat", data_stream, callback=download_callback)
                            compressed_data = data_stream.getvalue()
                            
                            if self.compression == "zstd":
                                with self.dctx_lock:
                                    raw_data = self.dctx.decompress(compressed_data)
                            elif self.compression == "lz4":
                                raw_data = lz4.frame.decompress(compressed_data)
                            else:
                                raw_data = compressed_data
                            
                            with open(block_path, 'wb') as f:
                                f.write(raw_data)

                        self.db.set_block_status(block_id, 'cached', remote_exists=1)
                    except Exception as e:
                        logger.error(f"Download/Decompress block {block_id} failed during write: {e}")
                        # 如果下载失败且本地文件也不存在，我们需要创建一个空文件以防后续 open(r+b) 失败
                        if not os.path.exists(block_path):
                            with open(block_path, 'wb') as f:
                                f.truncate(self.block_size)
                    finally:
                        with self.downloading_lock: self.downloading_count -= 1
                else:
                    with open(block_path, 'wb') as f: 
                        f.truncate(self.block_size)
            try:
                with open(block_path, 'r+b') as f:
                    f.seek(block_offset)
                    f.write(buf[bytes_written:bytes_written+chunk_len])
            except Exception as e:
                logger.error(f"Write to block file {block_id} failed: {e}")
                raise e
            bytes_written += chunk_len
            self.db.set_block_status(block_id, 'dirty')
            with self.upload_lock: self.upload_queue.add(block_id)
        return length

    def sync(self):
        """确保所有元数据和缓存都已写入磁盘"""
        # SQLite 在 commit 时已经保证了写入磁盘（尤其是开启了 WAL）
        # 这里主要作为一个占位符，如果未来有更复杂的缓存逻辑可以在此实现
        pass
