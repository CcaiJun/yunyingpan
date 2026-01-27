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

class VDrive(Operations):
    def __init__(self, dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_size_gb, block_size_mb=4, img_name="virtual_disk.img", remote_path="blocks", concurrency=4):
        self.client = WebDAVClient(dav_url, auth=(dav_user, dav_password))
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
        self.upload_lock = threading.Lock()
        
        # 启动后台线程
        for _ in range(self.concurrency):
            threading.Thread(target=self._upload_worker, daemon=True).start()
        
        threading.Thread(target=self._cache_worker, daemon=True).start()
        
        logger.info(f"Block-based VDrive initialized. Name: {self.img_name}, Size: {disk_size_gb}GB, Cache: {max_cache_size_gb}GB, Block Size: {block_size_mb}MB, Concurrency: {concurrency}")

    def _get_block_path(self, block_id):
        return os.path.join(self.cache_dir, f"{self.img_name}_blk_{block_id:08d}.dat")

    def _is_all_zeros(self, path):
        """检查文件是否全为零，使用高效的读块方式"""
        try:
            if not os.path.exists(path):
                return True
            with open(path, 'rb') as f:
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
            block_id = None
            with self.upload_lock:
                if self.upload_queue:
                    block_ids = sorted(list(self.upload_queue))
                    block_id = block_ids[0]
                    self.upload_queue.remove(block_id)
            
            if block_id is not None:
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

                        # 确保远程目录存在
                        parts = self.remote_path.split('/')
                        current = ""
                        for part in parts:
                            current = f"{current}/{part}".strip('/')
                            try: self.client.mkdir(current)
                            except: pass
                        
                        self.client.upload_file(block_path, remote_file_path, overwrite=True)
                        self.db.set_block_status(block_id, 'cached', remote_exists=1)
                        logger.info(f"Block {block_id} uploaded to WebDAV")
                except Exception as e:
                    logger.error(f"Block {block_id} upload failed: {e}")
                    with self.upload_lock:
                        self.upload_queue.add(block_id)
            time.sleep(0.5)

    def _cache_worker(self):
        while True:
            try:
                # 简单的缓存清理逻辑
                current_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) 
                                 for f in os.listdir(self.cache_dir) if f.startswith(f"{self.img_name}_blk_"))
                if current_size > self.max_cache_size:
                    lru_blocks = self.db.get_lru_blocks(10)
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
        yield '.'
        yield '..'
        yield self.img_name

    def open(self, path, flags):
        if path == '/' + self.img_name:
            return 0
        raise FuseOSError(errno.ENOENT)

    def read(self, path, length, offset, fh):
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
                if remote_exists or self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat"):
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
                if remote_exists or self.client.exists(f"{self.remote_path}/blk_{block_id:08d}.dat"):
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
                with open(block_path, 'r+b') as f:
                    f.seek(block_offset)
                    f.write(buf[bytes_written:bytes_written+chunk_len])
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
