#!/usr/bin/env python3
import os
import sqlite3
import time
import logging
import threading
import collections
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

    def get_block_info(self, block_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT status, remote_exists FROM blocks WHERE block_id = ?', (block_id,))
            row = cursor.fetchone()
            if row:
                return row[0], row[1]
            return 'empty', 0

    def get_lru_blocks(self, limit):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT block_id FROM blocks WHERE status = "cached" ORDER BY last_access ASC LIMIT ?', (limit,))
            return [row[0] for row in cursor.fetchall()]

    def get_dirty_blocks(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT block_id FROM blocks WHERE status = "dirty"')
            return [row[0] for row in cursor.fetchall()]

class BlockManager:
    def __init__(self, dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_size_gb, 
                 block_size_mb=4, img_name="virtual_disk.img", remote_path="blocks", concurrency=4,
                 compression="none", upload_limit_kb=0, download_limit_kb=0):
        self.use_remote = bool(dav_url and dav_url.strip())
        if self.use_remote:
            limits = Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
            self.client = WebDAVClient(
                dav_url, 
                auth=(dav_user, dav_password), 
                timeout=Timeout(60.0, connect=20.0),
                limits=limits
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
        
        # 速率限制器压缩器初始化
        if self.compression == "zstd":
            self.cctx = zstd.ZstdCompressor(level=3)
            self.dctx = zstd.ZstdDecompressor()
            self.cctx_lock = threading.Lock()
            self.dctx_lock = threading.Lock()
        
        # 速率限制器
        self.upload_limiter = RateLimiter(upload_limit_kb)
        self.download_limiter = RateLimiter(download_limit_kb)
        
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        self.db = MetadataDB(os.path.join(self.cache_dir, f'.{self.img_name}_metadata.db'))
        self.upload_queue = set()
        self.uploading_count = 0
        self.downloading_count = 0
        self.uploading_lock = threading.Lock()
        self.downloading_lock = threading.Lock()
        self.upload_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self._remote_dirs_checked = False
        
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

    def _upload_worker(self):
        while True:
            if not self.use_remote:
                time.sleep(5)
                continue
            block_id = None
            with self.upload_lock:
                if self.upload_queue:
                    block_id = self.upload_queue.pop()
            if block_id is not None:
                self.db.set_block_status(block_id, 'uploading')
                with self.uploading_lock: self.uploading_count += 1
                try:
                    block_path = self._get_block_path(block_id)
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
                            continue
                        if not self._remote_dirs_checked:
                            with self.upload_lock:
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
                                            processed_data = lz4.frame.compress(raw_data)
                                        
                                        # 上传内存中的压缩数据
                                        import io
                                        data_stream = io.BytesIO(processed_data)
                                        self.client.upload_fileobj(data_stream, remote_file_path, overwrite=True, callback=upload_callback)
                                    
                                    self.db.set_block_status(block_id, 'cached', remote_exists=1)
                                    break
                                except Exception as e:
                                    if attempt == max_retries: raise e
                        except Exception as upload_err:
                            logger.error(f"Upload failed for block {block_id}: {upload_err}")
                            raise upload_err
                except Exception as e:
                    logger.error(f"Block {block_id} upload failed: {e}")
                    with self.upload_lock: self.upload_queue.add(block_id)
                finally:
                    with self.uploading_lock: self.uploading_count -= 1
            time.sleep(0.1)

    def _cache_worker(self):
        while True:
            try:
                current_real_size = 0
                block_files = [f for f in os.listdir(self.cache_dir) if f.startswith(f"{self.img_name}_blk_")]
                for f in block_files:
                    try:
                        f_path = os.path.join(self.cache_dir, f)
                        stat_info = os.stat(f_path)
                        f_real_size = stat_info.st_blocks * 512
                        if f_real_size > 0 and self._is_all_zeros(f_path):
                            with open(f_path, 'wb') as f_obj: f_obj.truncate(self.block_size)
                            f_real_size = 0
                        current_real_size += f_real_size
                    except: pass
                if current_real_size > self.max_cache_size:
                    lru_blocks = self.db.get_lru_blocks(20)
                    for bid in lru_blocks:
                        path = self._get_block_path(bid)
                        if os.path.exists(path):
                            os.remove(path)
                            self.db.set_block_status(bid, 'empty')
            except Exception as e:
                logger.error(f"Cache worker error: {e}")
            time.sleep(60)

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
                remote_exists_check = remote_exists or self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat")
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
                        logger.error(f"Download block {block_id} failed: {e}")
                        result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                        bytes_read += chunk_len
                        continue
                    finally:
                        with self.downloading_lock: self.downloading_count -= 1
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
            self.db.set_block_status(block_id, status)
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
                remote_exists_check = remote_exists or self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat")
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
                        f.flush()
                        try: os.fsync(f.fileno())
                        except: pass
            try:
                with open(block_path, 'r+b') as f:
                    f.seek(block_offset)
                    f.write(buf[bytes_written:bytes_written+chunk_len])
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except:
                        pass
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
