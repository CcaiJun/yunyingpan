#!/usr/bin/env python3
import os
import sys
import errno
import logging
import threading
from fuse import FUSE, FuseOSError, Operations
from block_manager import BlockManager
import logging
import errno
import time
import os
import sys

logger = logging.getLogger(__name__)

class VDrive(Operations):
    def __init__(self, dav_url, dav_user, dav_password, cache_dir, disk_size_gb, max_cache_size_gb, 
                 block_size_mb=4, img_name="virtual_disk.img", remote_path="blocks", concurrency=4,
                 compression="none", upload_limit_kb=0, download_limit_kb=0):
        self.img_name = img_name if img_name.endswith('.img') else f"{img_name}.img"
        self.bm = BlockManager(dav_url, dav_user, dav_password, cache_dir, disk_size_gb, 
                              max_cache_size_gb, block_size_mb, img_name, remote_path, concurrency,
                              compression, upload_limit_kb, download_limit_kb)
        self.disk_size = self.bm.disk_size
        
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
            return 1000
        raise FuseOSError(errno.ENOENT)

    def read(self, path, length, offset, fh):
        if path != '/' + self.img_name:
            raise FuseOSError(errno.EIO)
        return self.bm.read(length, offset)

    def write(self, path, buf, offset, fh):
        if path != '/' + self.img_name:
            raise FuseOSError(errno.EIO)
        try:
            return self.bm.write(buf, offset)
        except:
            raise FuseOSError(errno.EIO)

    def release(self, path, fh):
        return 0

    def flush(self, path, fh):
        if path == '/' + self.img_name:
            self.bm.sync()
        return 0

    def fsync(self, path, datasync, fh):
        if path == '/' + self.img_name:
            self.bm.sync()
        return 0


def main(mountpoint, dav_url, dav_user, dav_password, cache_dir):
    FUSE(VDrive(dav_url, dav_user, dav_password, cache_dir, 100), mountpoint, nothreads=True, foreground=True, nonempty=True)

if __name__ == '__main__':
    # Usage: python3 v_drive.py <mountpoint> <dav_url> <dav_user> <dav_password> <local_cache_dir>
    if len(sys.argv) < 6:
        print('Usage: v_drive.py <mountpoint> <dav_url> <dav_user> <dav_password> <local_cache_dir>')
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
