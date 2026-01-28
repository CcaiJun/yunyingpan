#!/usr/bin/env python3
import os
import sys
import errno
import logging
import threading
from fuse import FUSE, FuseOSError, Operations
from webdav4.client import Client as WebDAVClient

import sqlite3
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetadataDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('PRAGMA journal_mode=WAL')  # 开启 WAL 模式增强可靠性
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
            # 检查是否需要更新旧表
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
                # 保持原有的 remote_exists 值
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

    def get_config(self, key, default=None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT value FROM config WHERE key = ?', (key,))
            row = cursor.fetchone()
            return row[0] if row else default

    def set_config(self, key, value):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)', (key, value))
            conn.commit()

    def get_lru_blocks(self, limit):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT block_id FROM blocks WHERE status = "cached" ORDER BY last_access ASC LIMIT ?', (limit,))
            return [row[0] for row in cursor.fetchall()]

    def get_dirty_blocks(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT block_id FROM blocks WHERE status = "dirty"')
            return [row[0] for row in cursor.fetchall()]

class VDrive(Operations):
    def __init__(self, dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_size_gb, block_size_mb=4, img_name="virtual_disk.img", remote_path="blocks", concurrency=4):
        from httpx import Timeout, Limits
        # 核心优化：检查 WebDAV URL 是否为空
        self.use_remote = bool(dav_url and dav_url.strip())
        if self.use_remote:
            # 核心优化：为 WebDAVClient 增加连接池限制，以支持真正的并发上传
            # 默认 httpx 限制了总连接数，可能导致多线程竞争同一个连接
            limits = Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
            self.client = WebDAVClient(
                dav_url, 
                auth=(dav_user, dav_password), 
                timeout=Timeout(60.0, connect=20.0), # 增加超时时间，大块上传更稳定
                limits=limits
            )
        else:
            self.client = None
            logger.warning(f"WebDAV URL is empty, disk {img_name} will work in LOCAL ONLY mode")

        self.cache_dir = os.path.abspath(cache_dir)
        self.disk_size = disk_size_gb * 1024 * 1024 * 1024
        self.max_cache_size = max_cache_size_gb * 1024 * 1024 * 1024
        self.block_size = block_size_mb * 1024 * 1024
        self.img_name = img_name if img_name.endswith('.img') else f"{img_name}.img"
        self.remote_path = remote_path.strip('/')
        self.concurrency = concurrency
        
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        self.db = MetadataDB(os.path.join(self.cache_dir, f'.{self.img_name}_metadata.db'))
        self.upload_queue = set()
        self.uploading_count = 0  # 记录当前正在上传的任务数
        self.uploading_lock = threading.Lock() # 专门用于 uploading_count 的锁
        
        # 启动时恢复未完成的上传任务
        dirty_blocks = self.db.get_dirty_blocks()
        if dirty_blocks:
            logger.info(f"Recovering {len(dirty_blocks)} dirty blocks to upload queue")
            for bid in dirty_blocks:
                self.upload_queue.add(bid)
                
        self.upload_lock = threading.Lock()
        self._remote_dirs_checked = False
        
        # 启动后台线程
        for _ in range(self.concurrency):
            threading.Thread(target=self._upload_worker, daemon=True).start()
        
        threading.Thread(target=self._cache_worker, daemon=True).start()
        
        logger.info(f"Block-based VDrive initialized. Name: {self.img_name}, Size: {disk_size_gb}GB, Cache: {max_cache_size_gb}GB, Block Size: {block_size_mb}MB, Concurrency: {concurrency}")

    def _get_block_path(self, block_id):
        return os.path.join(self.cache_dir, f"{self.img_name}_blk_{block_id:08d}.dat")

    def _is_all_zeros(self, path):
        """检查文件是否全为零，使用更高效的 seek_data/seek_hole 方式（如果系统支持）"""
        try:
            if not os.path.exists(path):
                return True
            
            # 首先检查文件大小
            size = os.path.getsize(path)
            if size == 0:
                return True
            
            # 检查实际物理占用，如果是 0，肯定是空洞文件（全零）
            stat_info = os.stat(path)
            if stat_info.st_blocks == 0:
                return True

            with open(path, 'rb') as f:
                # 预读前 4KB，大部分元数据块如果非零，前 4KB 就会有数据
                chunk = f.read(4096)
                if any(chunk):
                    return False
                
                # 如果前 4KB 是零，再进行全量检查，但跳过已知的空洞部分
                f.seek(0)
                while True:
                    chunk = f.read(1024 * 1024) # 每次读 1MB
                    if not chunk:
                        break
                    if any(chunk): # 如果发现非零字节
                        return False
            return True
        except Exception as e:
            logger.error(f"Error checking zeros for {path}: {e}")
            return False

    def _upload_worker(self):
        while True:
            # 如果未开启远程，则不需要上传
            if not self.use_remote:
                time.sleep(5)
                continue
                
            block_id = None
            with self.upload_lock:
                if self.upload_queue:
                    # 优化：优先上传编号较小的块（通常是元数据或文件头部）
                    block_ids = sorted(list(self.upload_queue))
                    block_id = block_ids[0]
                    self.upload_queue.remove(block_id)
            
            if block_id is not None:
                # 标记正在上传，避免其他线程重复处理
                self.db.set_block_status(block_id, 'uploading')
                with self.uploading_lock:
                    self.uploading_count += 1
                try:
                    block_path = self._get_block_path(block_id)
                    status, remote_exists = self.db.get_block_info(block_id)
                    
                    if os.path.exists(block_path):
                        remote_file_path = f"{self.remote_path}/blk_{block_id:08d}.dat"
                        
                        # 流量优化：检查是否全为零
                        if self._is_all_zeros(block_path):
                            if remote_exists:
                                logger.info(f"Block {block_id} is zero but exists on remote. Deleting to save space.")
                                try:
                                    self.client.remove(remote_file_path)
                                    self.db.set_block_status(block_id, 'cached', remote_exists=0)
                                except: pass
                            else:
                                logger.info(f"Block {block_id} is zero and not on remote. Skipping.")
                                self.db.set_block_status(block_id, 'cached', remote_exists=0)
                            continue

                        # 核心优化：只在目录未检查时尝试一次创建，避免并发上传时大量 405/423 冲突
                        if not self._remote_dirs_checked:
                            with self.upload_lock: # 使用锁保护目录创建过程
                                if not self._remote_dirs_checked:
                                    parts = self.remote_path.split('/')
                                    current = ""
                                    for part in parts:
                                        current = f"{current}/{part}".strip('/')
                                        try: self.client.mkdir(current)
                                        except: pass
                                    self._remote_dirs_checked = True
                        
                        # 原子上传优化：减少 WebDAV 请求往返
                        try:
                            max_retries = 3
                            for attempt in range(max_retries + 1):
                                try:
                                    # 如果是重试，先随机睡眠一小会儿，避开并发冲突
                                    if attempt > 0:
                                        import random
                                        time.sleep(random.uniform(0.5, 2.0))
                                        
                                    logger.info(f"Uploading block {block_id} (attempt {attempt+1})...")
                                    self.client.upload_file(block_path, remote_file_path, overwrite=True)
                                    self.db.set_block_status(block_id, 'cached', remote_exists=1)
                                    logger.info(f"Block {block_id} uploaded successfully")
                                    break
                                except Exception as e:
                                    if attempt == max_retries:
                                        raise e
                                    # 423 Locked 通常是因为其他线程正在对同一个父目录进行操作
                                    logger.warning(f"Upload retry {attempt+1} for block {block_id}: {e}")
                        except Exception as upload_err:
                            logger.error(f"Upload failed for block {block_id}: {upload_err}")
                            raise upload_err
                except Exception as e:
                    logger.error(f"Block {block_id} upload failed: {e}")
                    # 只有在非 4xx 错误（通常是网络问题）时才重新加入队列
                    # 注意：webdav4 的异常处理可能需要更细致
                    with self.upload_lock:
                        self.upload_queue.add(block_id)
                finally:
                    with self.uploading_lock:
                        self.uploading_count -= 1
            time.sleep(0.1) # 减小睡眠时间，提高响应速度

    def _cache_worker(self):
        while True:
            try:
                # 使用稀疏统计获取真实大小
                current_real_size = 0
                block_files = [f for f in os.listdir(self.cache_dir) if f.startswith(f"{self.img_name}_blk_")]
                for f in block_files:
                    try:
                        f_path = os.path.join(self.cache_dir, f)
                        stat_info = os.stat(f_path)
                        f_real_size = stat_info.st_blocks * 512
                        
                        # 核心优化：如果一个块文件是全零的且物理占用大于0，或者它是空的，尝试释放它
                        if f_real_size > 0 and self._is_all_zeros(f_path):
                            logger.info(f"Releasing zero block file: {f}")
                            # 使用 truncate 重新变回真正的空洞文件
                            with open(f_path, 'wb') as f_obj:
                                f_obj.truncate(self.block_size)
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
                            logger.info(f"Evicted block {bid} from cache")
            except Exception as e:
                logger.error(f"Cache worker error: {e}")
            time.sleep(60)

    def getattr(self, path, fh=None):
        logger.debug(f"getattr: {path}")
        if path == '/':
            return {'st_mode': 0o40755, 'st_nlink': 2}
        if path == '/' + self.img_name:
            return {
                'st_mode': 0o100644,
                'st_nlink': 1,
                'st_size': self.disk_size,
                'st_uid': os.getuid(),
                'st_gid': os.getgid(),
                'st_atime': time.time(),
                'st_mtime': time.time(),
                'st_ctime': time.time(),
            }
        raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        logger.debug(f"readdir: {path}")
        yield '.'
        yield '..'
        yield self.img_name

    def open(self, path, flags):
        logger.debug(f"open: {path}")
        if path == '/' + self.img_name:
            # 返回一个虚拟的句柄，不要返回 0，因为 0 是 stdin
            return 1000
        raise FuseOSError(errno.ENOENT)

    def read(self, path, length, offset, fh):
        # logger.debug(f"read: {path}, length: {length}, offset: {offset}")
        if path != '/' + self.img_name:
            raise FuseOSError(errno.EIO)

        # logger.info(f"Read request: {length} bytes at offset {offset}")
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
                # 如果本地没有，尝试从远程下载
                if self.use_remote and (remote_exists or self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat")):
                    logger.info(f"Downloading block {block_id}...")
                    try:
                        self.client.download_file(f"{self.remote_path}/blk_{block_id:08d}.dat", block_path)
                        self.db.set_block_status(block_id, 'cached', remote_exists=1)
                    except Exception as e:
                        logger.error(f"Download block {block_id} failed: {e}")
                        result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                        bytes_read += chunk_len
                        continue
                else:
                    # 远程也没有，说明是全 0 块
                    result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                    bytes_read += chunk_len
                    self.db.set_block_status(block_id, 'empty', remote_exists=0)
                    continue

            try:
                with open(block_path, 'rb') as f:
                    f.seek(block_offset)
                    data = f.read(chunk_len)
                    if not data:
                        data = b'\x00' * chunk_len
                    result[bytes_read:bytes_read+len(data)] = data
                    bytes_read += len(data)
            except Exception as e:
                logger.error(f"Read block {block_id} file failed: {e}")
                result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                bytes_read += chunk_len
            
            self.db.set_block_status(block_id, status)

        return bytes(result)

    def write(self, path, buf, offset, fh):
        if path != '/' + self.img_name:
            raise FuseOSError(errno.EIO)

        length = len(buf)
        # logger.info(f"Write request: {length} bytes at offset {offset}")
        bytes_written = 0
        
        while bytes_written < length:
            curr_offset = offset + bytes_written
            block_id = curr_offset // self.block_size
            block_offset = curr_offset % self.block_size
            chunk_len = min(length - bytes_written, self.block_size - block_offset)
            
            block_path = self._get_block_path(block_id)
            status, remote_exists = self.db.get_block_info(block_id)
            
            # 确保块文件存在
            if not os.path.exists(block_path):
                # 尝试下载
                if self.use_remote and (remote_exists or self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat")):
                    try:
                        self.client.download_file(f"{self.remote_path}/blk_{block_id:08d}.dat", block_path)
                        self.db.set_block_status(block_id, 'cached', remote_exists=1)
                    except Exception as e:
                        logger.error(f"Download for write failed: {e}")
                        # 下载失败，创建一个空洞文件（稀疏文件）
                        with open(block_path, 'wb') as f:
                            f.truncate(self.block_size)
                else:
                    # 远程也没有，创建一个空洞文件（稀疏文件）以节省本地空间
                    with open(block_path, 'wb') as f:
                        f.truncate(self.block_size)
            
            try:
                # 性能优化：在写入前先检查数据是否全为零
                # 如果是全零块且本地文件尚不存在或已是空洞，则无需实际写入磁盘，保持空洞即可
                is_buf_zeros = all(b == 0 for b in buf[bytes_written:bytes_written+chunk_len])
                
                if is_buf_zeros and not os.path.exists(block_path):
                    # 数据全零且块文件不存在，直接按空洞处理，不创建实际物理块
                    pass
                else:
                    # 只有在非零数据或文件已存在时才执行写入
                    with open(block_path, 'r+b') as f:
                        f.seek(block_offset)
                        f.write(buf[bytes_written:bytes_written+chunk_len])
                        f.flush()
                        # os.fsync(f.fileno()) # 注释掉强制刷盘，大幅提升大容量磁盘格式化和写入性能
            except Exception as e:
                logger.error(f"Write to block file {block_id} failed: {e}")
                raise FuseOSError(errno.EIO)
            
            bytes_written += chunk_len
            self.db.set_block_status(block_id, 'dirty')
            with self.upload_lock:
                self.upload_queue.add(block_id)

        return length

    def release(self, path, fh):
        return 0

    def flush(self, path, fh):
        """当文件关闭或调用 fsync 时触发"""
        # 对于块镜像文件，确保所有脏块都进入上传队列
        return 0

    def fsync(self, path, datasync, fh):
        """强制同步到磁盘"""
        if path == '/' + self.img_name:
            # 优化：不再遍历所有块文件进行 fsync，因为 write 已经包含了 fsync
            # 且在大容量硬盘下，遍历数百个文件极其缓慢
            pass
        return 0

    def mkdir(self, path, mode):
        logger.info(f"mkdir: {path}")
        try:
            self.client.mkdir(path.lstrip('/'))
            return 0
        except Exception as e:
            logger.error(f"mkdir error {path}: {e}")
            raise FuseOSError(errno.EIO)

    def unlink(self, path):
        logger.info(f"unlink: {path}")
        cache_path = self._get_cache_path(path)
        if os.path.exists(cache_path):
            os.remove(cache_path)
        try:
            self.client.remove(path.lstrip('/'))
            return 0
        except Exception as e:
            logger.error(f"unlink error {path}: {e}")
            raise FuseOSError(errno.EIO)

    def rmdir(self, path):
        logger.info(f"rmdir: {path}")
        try:
            self.client.remove(path.lstrip('/'))
            return 0
        except Exception as e:
            logger.error(f"rmdir error {path}: {e}")
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        cache_path = self._get_cache_path(path)
        self._ensure_parent_dir(cache_path)
        fd = os.open(cache_path, os.O_WRONLY | os.O_CREAT, mode)
        with self.upload_lock:
            self.upload_queue.add(path)
        return fd

    def release(self, path, fh):
        try:
            if fh != 1000:
                os.close(fh)
        except Exception as e:
            logger.error(f"Error closing fh for {path}: {e}")
        return 0


def main(mountpoint, dav_url, dav_user, dav_password, cache_dir):
    FUSE(VDrive(dav_url, dav_user, dav_password, cache_dir, 100), mountpoint, nothreads=True, foreground=True, nonempty=True)

if __name__ == '__main__':
    # Usage: python3 v_drive.py <mountpoint> <dav_url> <dav_user> <dav_password> <local_cache_dir>
    if len(sys.argv) < 6:
        print('Usage: v_drive.py <mountpoint> <dav_url> <dav_user> <dav_password> <local_cache_dir>')
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
