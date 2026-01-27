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
                    last_access REAL
                )
            ''')
            conn.commit()

    def get_config(self, key, default=None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT value FROM config WHERE key = ?', (key,))
            row = cursor.fetchone()
            return row[0] if row else default

    def set_config(self, key, value):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)', (key, value))
            conn.commit()

    def set_block_status(self, block_id, status):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('INSERT OR REPLACE INTO blocks (block_id, status, last_access) VALUES (?, ?, ?)',
                        (block_id, status, time.time()))
            conn.commit()

    def get_block_status(self, block_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT status FROM blocks WHERE block_id = ?', (block_id,))
            row = cursor.fetchone()
            return row[0] if row else 'empty'

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

    def _upload_worker(self):
        while True:
            block_id = None
            with self.upload_lock:
                if self.upload_queue:
                    block_id = self.upload_queue.pop()
            
            if block_id is not None:
                try:
                    block_path = self._get_block_path(block_id)
                    if os.path.exists(block_path):
                        remote_file_path = f"{self.remote_path}/blk_{block_id:08d}.dat"
                        # 确保远程目录存在
                        parts = self.remote_path.split('/')
                        current = ""
                        for part in parts:
                            current = f"{current}/{part}".strip('/')
                            try: self.client.mkdir(current)
                            except: pass
                        
                        self.client.upload_file(block_path, remote_file_path, overwrite=True)
                        self.db.set_block_status(block_id, 'cached')
                        logger.info(f"Block {block_id} uploaded to WebDAV: {remote_file_path}")
                except Exception as e:
                    logger.error(f"Block {block_id} upload failed: {e}")
                    with self.upload_lock:
                        self.upload_queue.add(block_id)
            time.sleep(1)

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

        logger.info(f"Read request: {length} bytes at offset {offset}")
        result = bytearray(length)
        bytes_read = 0
        
        while bytes_read < length:
            curr_offset = offset + bytes_read
            block_id = curr_offset // self.block_size
            block_offset = curr_offset % self.block_size
            chunk_len = min(length - bytes_read, self.block_size - block_offset)
            
            block_path = self._get_block_path(block_id)
            status = self.db.get_block_status(block_id)
            
            if status == 'empty':
                # 尝试从 WebDAV 下载
                remote_file_path = f"{self.remote_path}/blk_{block_id:08d}.dat"
                try:
                    # 首先检查本地是否有文件（可能数据库没记录但文件在）
                    if not os.path.exists(block_path):
                        if self.client.exists(remote_file_path):
                            logger.info(f"Downloading block {block_id} from WebDAV...")
                            self.client.download_file(remote_file_path, block_path)
                            self.db.set_block_status(block_id, 'cached')
                        else:
                            # 远程也没有，说明是全 0 块
                            result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                            bytes_read += chunk_len
                            continue
                    else:
                        self.db.set_block_status(block_id, 'cached')
                except Exception as e:
                    logger.error(f"Check/Download block {block_id} failed: {e}")
                    result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                    bytes_read += chunk_len
                    continue

            try:
                if not os.path.exists(block_path):
                    # 兜底：如果数据库说有但文件没了
                    result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                    bytes_read += chunk_len
                else:
                    with open(block_path, 'rb') as f:
                        f.seek(block_offset)
                        data = f.read(chunk_len)
                        if not data:
                            # 如果读不到数据，补 0
                            data = b'\x00' * chunk_len
                        result[bytes_read:bytes_read+len(data)] = data
                        bytes_read += len(data)
            except Exception as e:
                logger.error(f"Read block {block_id} file failed: {e}")
                # 不要轻易抛出 EIO，尝试用 0 填充保证系统不崩溃
                result[bytes_read:bytes_read+chunk_len] = b'\x00' * chunk_len
                bytes_read += chunk_len
            
            self.db.set_block_status(block_id, status)

        return bytes(result)

    def write(self, path, buf, offset, fh):
        if path != '/' + self.img_name:
            raise FuseOSError(errno.EIO)

        length = len(buf)
        logger.info(f"Write request: {length} bytes at offset {offset}")
        bytes_written = 0
        
        while bytes_written < length:
            curr_offset = offset + bytes_written
            block_id = curr_offset // self.block_size
            block_offset = curr_offset % self.block_size
            chunk_len = min(length - bytes_written, self.block_size - block_offset)
            
            block_path = self._get_block_path(block_id)
            
            # 确保块文件存在
            if not os.path.exists(block_path):
                # 尝试下载
                remote_file_path = f"{self.remote_path}/blk_{block_id:08d}.dat"
                try:
                    if self.client.exists(remote_file_path):
                        self.client.download_file(remote_file_path, block_path)
                    else:
                        # 创建空块文件并预分配空间
                        with open(block_path, 'wb') as f:
                            f.write(b'\x00' * self.block_size)
                except Exception as e:
                    logger.error(f"Prepare write block {block_id} failed: {e}")
                    with open(block_path, 'wb') as f:
                        f.write(b'\x00' * self.block_size)

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
