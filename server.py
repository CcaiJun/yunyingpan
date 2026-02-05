from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
import os
import threading
import logging
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import psutil
import sqlite3
import time
import subprocess

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import sys
import os
logger.info(f"Python Executable: {sys.executable}")
logger.info(f"Python Path: {sys.path}")

try:
    from v_drive import VDrive, FUSE
except ImportError as e:
    logger.error(f"Failed to import v_drive: {e}")
    # Continue to see if we can still start the server

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import json
from typing import Dict, List, Optional

# 配置持久化路径
CONFIG_FILE = "disks_config.json"
SYSTEM_CONFIG_FILE = "system_config.json"

class SystemConfig(BaseModel):
    computer_name: str = "default_pc"

class MountConfig(BaseModel):
    dav_url: str
    dav_user: str
    dav_password: str
    mount_path: str
    cache_dir: str
    disk_size_gb: int
    max_cache_gb: int
    disk_name: str = "virtual_disk"
    block_size_mb: int = 4
    remote_path: str = "blocks"
    concurrency: int = 5
    inode_ratio: int = 16384  # 默认 16KB (ext4 默认值)
    compression: str = "none"   # none, zstd, lz4
    compression_level: int = 3
    driver_mode: str = "fuse" # fuse or nbd
    upload_limit_kb: int = 0
    download_limit_kb: int = 0

class DiskInstance:
    def __init__(self, config: MountConfig):
        # 清理配置中的空格
        config.dav_url = config.dav_url.strip()
        config.dav_user = config.dav_user.strip()
        config.dav_password = config.dav_password.strip()
        
        self.config = config
        self.vdrive = None
        self.nbd_server = None
        self.thread = None
        self.status = "stopped" # stopped, starting, running, error
        self.error_msg = ""
        self.final_mountpoint = f"/mnt/v_disks/{config.disk_name}"
        self.cache_usage_bytes = 0
        self.loop_status = "unmounted"
        self.upload_queue_size = 0
        self.download_queue_size = 0
        self.upload_speed = 0
        self.download_speed = 0
        self.nbd_device = ""
        self.total_blocks = 0
        self.uploaded_blocks = 0
        self.stored_size_bytes = 0
        self.used_inodes = 0
        self.total_inodes = 0
        self.startup_progress = 0
        self.instance_version = 0 # 增加版本号，用于区分不同的启动生命周期
        self.init_progress = None # 初始化（扩容）进度：0-100，None表示未进行
        self.init_status = "" # 初始化状态描述

def load_configs() -> List[dict]:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_configs(configs: List[dict]):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(configs, f, indent=4)

def load_system_config() -> SystemConfig:
    if os.path.exists(SYSTEM_CONFIG_FILE):
        try:
            with open(SYSTEM_CONFIG_FILE, 'r') as f:
                data = json.load(f)
                return SystemConfig(**data)
        except:
            return SystemConfig()
    return SystemConfig()

def save_system_config(config: SystemConfig):
    with open(SYSTEM_CONFIG_FILE, 'w') as f:
        json.dump(config.dict(), f, indent=4)

# 全局变量
system_config = load_system_config()

# 全局锁，防止 NBD 设备分配竞争
global_nbd_lock = threading.Lock()

def do_mount(inst: DiskInstance):
    if inst.status == "starting":
        return
    
    inst.instance_version += 1
    current_version = inst.instance_version
    inst.status = "starting"
    inst.error_msg = ""
    inst.startup_progress = 0
    
    try:
        cfg = inst.config
        import subprocess
        import time
        
        # 分配端口 (避开 10809)
        port = 10810 + list(disks.keys()).index(cfg.disk_name)
        
        logger.info(f"Pre-cleaning mount paths for {cfg.disk_name} (Port {port})")
        inst.startup_progress = 5
        
        # 0. 预清理
        try:
            logger.info(f"Cleanup start (Version {current_version})")
            subprocess.run(['umount', '-l', inst.final_mountpoint], check=False, capture_output=True)
            if cfg.driver_mode == "fuse":
                subprocess.run(['fusermount3', '-u', cfg.mount_path], check=False, capture_output=True)
            else:
                # 尝试断开当前已知的 NBD 设备
                if inst.nbd_device:
                    logger.info(f"Disconnecting stale NBD device {inst.nbd_device} (Version {current_version})")
                    subprocess.run(['nbd-client', '-d', inst.nbd_device], check=False, capture_output=True)
                
                # 兜底清理：尝试断开所有可能的 NBD 连接，确保端口释放
                for i in range(16):
                    subprocess.run(['nbd-client', '-d', f'/dev/nbd{i}'], check=False, capture_output=True)
                
                if inst.nbd_server:
                    try:
                        logger.info(f"Stopping old NBD server (ID {inst.nbd_server.server_id})")
                        inst.nbd_server.stop()
                    except:
                        pass
                
                # 等待旧线程彻底退出，释放端口和资源
                if inst.thread and inst.thread.is_alive():
                    logger.info(f"Waiting for old thread to exit (Version {current_version})")
                    inst.thread.join(timeout=5)
                    if inst.thread.is_alive():
                        logger.warning(f"Old thread did not exit in time (Version {current_version})")
                
                # 确保端口已释放
                import socket
                port_free = False
                for _ in range(5):
                    try:
                        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        test_sock.bind(('127.0.0.1', port))
                        test_sock.close()
                        port_free = True
                        break
                    except:
                        time.sleep(1)
                
                if not port_free:
                    logger.warning(f"Port {port} still seems busy after 5s (Version {current_version})")
                else:
                    logger.info(f"Port {port} is now free (Version {current_version})")
            subprocess.run(['umount', '-l', cfg.mount_path], check=False, capture_output=True)
            os.makedirs(cfg.mount_path, exist_ok=True)
            os.makedirs(inst.final_mountpoint, exist_ok=True)
            logger.info(f"Cleanup finished (Version {current_version})")
        except Exception as e:
            logger.warning(f"Cleanup failed (Version {current_version}): {e}")
        
        inst.startup_progress = 15

        if cfg.driver_mode == "fuse":
            # --- FUSE 模式 ---
            inst.startup_progress = 20
            from v_drive import VDrive, FUSE
            inst.vdrive = VDrive(
                dav_url=cfg.dav_url, dav_user=cfg.dav_user, dav_password=cfg.dav_password, 
                cache_dir=cfg.cache_dir, disk_size_gb=cfg.disk_size_gb,
                max_cache_size_gb=cfg.max_cache_gb, block_size_mb=cfg.block_size_mb,
                img_name=cfg.disk_name, remote_path=cfg.remote_path, concurrency=cfg.concurrency,
                compression=cfg.compression,
                compression_level=cfg.compression_level,
                upload_limit_kb=cfg.upload_limit_kb, download_limit_kb=cfg.download_limit_kb
            )

            def start_fuse():
                vdrive_ptr = inst.vdrive
                thread_version = current_version
                logger.info(f"FUSE thread started (Version {thread_version})")
                try:
                    FUSE(inst.vdrive, cfg.mount_path, foreground=True, nonempty=True, allow_other=True, direct_io=True)
                except Exception as fuse_err:
                    logger.error(f"FUSE error (Version {thread_version}): {fuse_err}")
                finally:
                    # 使用版本号校验，确保只有最新启动的线程能修改状态
                    if inst.instance_version == thread_version:
                        if inst.status not in ["stopped", "starting"]:
                            logger.error(f"FUSE process exited unexpectedly (Version {thread_version}, Status {inst.status})")
                            inst.status = "error"
                            inst.error_msg = "FUSE 进程异常退出"
                        else:
                            logger.info(f"FUSE thread finished naturally (Version {thread_version}, Status {inst.status})")

            inst.thread = threading.Thread(target=start_fuse, daemon=True)
            inst.thread.start()

            img_name = cfg.disk_name if cfg.disk_name.endswith('.img') else f"{cfg.disk_name}.img"
            target_path = os.path.join(cfg.mount_path, img_name)
            
            # 等待 FUSE 镜像文件出现
            inst.startup_progress = 30
            max_wait = 300
            while max_wait > 0 and not os.path.exists(target_path):
                if not inst.thread.is_alive():
                    if inst.instance_version == current_version:
                        raise Exception("FUSE 进程启动失败")
                    else:
                        return
                time.sleep(1)
                max_wait -= 1
                inst.startup_progress = min(60, inst.startup_progress + 0.5)
            if not os.path.exists(target_path):
                if inst.instance_version == current_version:
                    raise Exception("FUSE 镜像生成超时")
                else:
                    return
            
            inst.startup_progress = 60
            mount_cmd = ['mount', '-o', 'loop', target_path, inst.final_mountpoint]

        else:
            # --- NBD 模式 ---
            inst.startup_progress = 20
            from nbd_server import NBDServer
            from block_manager import BlockManager
            
            # 分配端口 (避开 10809)
            port = 10810 + list(disks.keys()).index(cfg.disk_name)
            
            bm = BlockManager(
                dav_url=cfg.dav_url, dav_user=cfg.dav_user, dav_password=cfg.dav_password, 
                cache_dir=cfg.cache_dir, disk_size_gb=cfg.disk_size_gb,
                max_cache_size_gb=cfg.max_cache_gb, block_size_mb=cfg.block_size_mb,
                img_name=cfg.disk_name, remote_path=cfg.remote_path, concurrency=cfg.concurrency,
                compression=cfg.compression,
                compression_level=cfg.compression_level,
                upload_limit_kb=cfg.upload_limit_kb, download_limit_kb=cfg.download_limit_kb
            )
            inst.vdrive = bm # 兼容状态检查中的上传队列统计
            
            inst.nbd_server = NBDServer(bm, host='127.0.0.1', port=port)
            
            def start_nbd():
                server_ptr = inst.nbd_server
                thread_version = current_version
                s_id = server_ptr.server_id
                logger.info(f"NBD server thread started (Version {thread_version}, ID {s_id})")
                try:
                    server_ptr.start()
                    logger.info(f"NBD server thread finished normally (Version {thread_version}, ID {s_id})")
                except Exception as e:
                    logger.error(f"NBD server error (Version {thread_version}, ID {s_id}): {e}")
                    # 如果启动失败，且版本匹配，记录错误信息供 do_mount 使用
                    if inst.instance_version == thread_version:
                        inst.error_msg = f"NBD 服务端启动失败: {e}"
                finally:
                    # 使用版本号校验，确保只有最新启动的线程能修改状态
                    # 只有在非主动停止且当前版本匹配时才设置错误状态
                    if inst.instance_version == thread_version:
                        # 如果 server_ptr.running 已经是 False，说明它根本没启动成功或者已经被 stop 了
                        if inst.status == "running" and not server_ptr.running:
                            logger.error(f"NBD server exited unexpectedly (Version {thread_version}, ID {s_id}, Status {inst.status})")
                            inst.status = "error"
                            inst.error_msg = "NBD 服务端异常退出"
                        else:
                            logger.info(f"NBD server thread cleanup (Version {thread_version}, ID {s_id}, Status {inst.status})")

            inst.thread = threading.Thread(target=start_nbd, daemon=True)
            inst.thread.start()
            
            # 寻找空闲 NBD 设备并连接
            inst.startup_progress = 30
            
            # 等待 server 启动并进入监听状态
            max_wait = 10
            while max_wait > 0 and not inst.nbd_server.running:
                if not inst.thread.is_alive():
                    # 只有在版本匹配时才抛出异常
                    if inst.instance_version == current_version:
                        raise Exception(f"NBD 服务端启动失败: {inst.error_msg}")
                    else:
                        return # 旧版本线程，直接退出
                time.sleep(0.5)
                max_wait -= 0.5
            
            if not inst.nbd_server.running:
                if inst.instance_version == current_version:
                    raise Exception("NBD 服务端启动超时，请检查端口是否被占用")
                else:
                    return # 旧版本线程，直接退出

            with global_nbd_lock:
                nbd_dev = None
                for i in range(16):
                    dev = f"/dev/nbd{i}"
                    # 检查是否已经挂载或正在被 nbd-client 使用
                    check_res = subprocess.run(['nbd-client', '-check', dev], capture_output=True)
                    if check_res.returncode != 0:
                        # 额外检查 mount 列表
                        mount_check = subprocess.run(['mount'], capture_output=True, text=True)
                        if dev not in mount_check.stdout:
                            nbd_dev = dev
                            break
                    inst.startup_progress = min(40, inst.startup_progress + 0.5)
                
                if not nbd_dev: raise Exception("没有可用的 NBD 设备")
                
                inst.nbd_device = nbd_dev
                inst.startup_progress = 45
                
                # 连接 NBD 客户端 (在锁内连接以防竞争)
                logger.info(f"Connecting nbd-client to {nbd_dev} on port {port} (Version {current_version})")
                nbd_client_cmd = ["nbd-client", "127.0.0.1", str(port), nbd_dev, "-N", "default", "-g", "-persist"]
                try:
                    # 使用 -persist 模式
                    result = subprocess.run(nbd_client_cmd, capture_output=True, text=True, timeout=15)
                    if result.returncode != 0:
                        logger.error(f"nbd-client failed with return code {result.returncode}: {result.stderr}")
                        raise Exception(f"nbd-client 连接失败: {result.stderr}")
                    logger.info(f"nbd-client connected successfully (Version {current_version})")
                except subprocess.TimeoutExpired:
                    logger.error(f"nbd-client connection timed out (Version {current_version})")
                    raise Exception("NBD 客户端连接超时")
                except Exception as e:
                    logger.error(f"nbd-client connection failed (Version {current_version}): {e}")
                    raise Exception(f"NBD 客户端连接失败: {e}")
                inst.startup_progress = 55
            
            inst.startup_progress = 60
            target_path = nbd_dev
            mount_cmd = ['mount', nbd_dev, inst.final_mountpoint]

        # 检查并格式化
        inst.startup_progress = 65
        
        # 等待远程扫描完成，避免 blkid 触发大量 exists 请求导致超时
        try:
            bm = None
            if inst.config.driver_mode == "fuse":
                bm = inst.vdrive.bm
            else:
                bm = inst.vdrive
            
            if bm and bm.use_remote:
                # 如果数据库中已经有记录，说明之前扫描过，不需要等待本次扫描完成也可以进行 blkid
                if bm.db.get_remote_exists_count() > 0:
                    logger.info(f"Disk {cfg.disk_name} already has indexed blocks, skipping wait for remote scan.")
                else:
                    logger.info(f"Waiting for remote scan to complete for {cfg.disk_name}...")
                    max_scan_wait = 60 # 延长等待时间以应对大量文件的还原
                    while max_scan_wait > 0 and not bm._remote_dirs_checked:
                        time.sleep(1)
                        max_scan_wait -= 1
                    if not bm._remote_dirs_checked:
                        logger.warning(f"Remote scan for {cfg.disk_name} is taking too long (60s+)...")
        except: pass

        needs_format = True
        
        # 增加逻辑：如果远程已经有数据块，绝对不要格式化，否则会破坏原有数据
        has_remote = False
        remote_check_error = None
        try:
            bm = None
            if inst.config.driver_mode == "fuse":
                bm = inst.vdrive.bm
            else:
                bm = inst.vdrive
            
            # 优化：优先检查本地数据库记录，避免重复调用 ls
            if bm:
                remote_count = bm.db.get_remote_exists_count()
                if remote_count > 0:
                    has_remote = True
                    logger.info(f"Disk {cfg.disk_name} has {remote_count} indexed remote blocks, skipping auto-format.")
                elif bm.has_remote_data(): # 兜底检查一次
                    has_remote = True
                    logger.info(f"Disk {cfg.disk_name} has remote data (via ls), skipping auto-format.")
        except Exception as e:
            logger.warning(f"Failed to check remote data: {e}")
            remote_check_error = str(e)
            
        # 安全守门员：在决定格式化之前，必须确认这不是误判
        if needs_format and not has_remote and bm and bm.use_remote:
            # 1. 如果扫描曾经出错
            if bm.scan_error:
                raise Exception(f"云端扫描曾发生错误 ({bm.scan_error})，无法确认磁盘是否为空。为防止数据丢失，已终止格式化。请检查网络。")
            
            # 2. 如果刚才的主动检查出错
            if remote_check_error:
                raise Exception(f"连接云端确认状态失败 ({remote_check_error})，为防止数据丢失，已终止格式化。请检查网络。")
                
            # 3. 如果扫描根本没完成（超时）
            if not bm._remote_dirs_checked:
                 raise Exception("云端扫描超时未完成，无法确认磁盘状态。为防止数据丢失，已终止操作。")
                 
            logger.info(f"Disk {cfg.disk_name} confirmed empty (Remote scan OK, DB empty, LS empty), proceeding with format.")

        inst.startup_progress = 75
        try:
            res = subprocess.run(['blkid', target_path], capture_output=True, text=True, timeout=120)
            if "TYPE=" in res.stdout: 
                needs_format = False
                logger.info(f"Disk {cfg.disk_name} already has a filesystem: {res.stdout.strip()}")
        except subprocess.TimeoutExpired:
            logger.error(f"blkid timed out for {target_path}")
            raise Exception("读取文件系统超时，磁盘可能已卡死，请尝试重启应用或重置磁盘")
        except: pass

        inst.startup_progress = 85
        if needs_format and not has_remote:
            logger.info(f"Formatting {target_path}...")
            is_fast_format = False
            try:
                # 显式使用 -E nodiscard 避免 TRIM 操作导致 NBD 超时
                # 快速格式化策略：如果磁盘大于 10GB，先格式化为 1GB，挂载后再扩容
                # 这样可以避免 mkfs 初始化海量 inode 表时的漫长等待
                mkfs_cmd = ['mkfs.ext4', '-F', '-i', str(cfg.inode_ratio), '-b', '4096', 
                              '-O', '^metadata_csum', '-E', 'lazy_itable_init=1,lazy_journal_init=1,nodiscard', target_path]
                
                if cfg.disk_size_gb >= 10:
                    logger.info(f"Using fast format strategy (initial 1GB) for {cfg.disk_name}")
                    mkfs_cmd.append('1G')
                    is_fast_format = True
                
                subprocess.run(mkfs_cmd, check=True, timeout=600)
            except subprocess.TimeoutExpired:
                logger.error(f"mkfs.ext4 timed out for {target_path}")
                raise Exception("格式化磁盘超时，可能存在 I/O 阻塞")
        elif needs_format and has_remote:
            logger.error(f"Disk {cfg.disk_name} needs format but has remote data! Skipping format to prevent data loss. Please check if parameters (block size, compression) match the original disk.")
            raise Exception("无法识别文件系统，且检测到云端已有数据。请检查磁盘参数（分块大小、压缩算法等）是否与创建时一致。")

        inst.startup_progress = 95
        # 最终挂载
        logger.info(f"Final mount (Version {current_version}): {' '.join(mount_cmd)}")
        try:
            # 检查是否已经挂载，避免 "already mounted" 错误
            check_mount = subprocess.run(['mount'], capture_output=True, text=True)
            if inst.final_mountpoint in check_mount.stdout:
                logger.info(f"Path {inst.final_mountpoint} already mounted, skipping mount command.")
            else:
                try:
                    # 增加超时时间到 300s (5分钟)，防止因网络慢或元数据下载多而超时
                    subprocess.run(mount_cmd, check=True, timeout=300)
                except subprocess.CalledProcessError as e:
                    if e.returncode == 32:
                        logger.warning("Mount failed with structure error, attempting fsck repair...")
                        # 尝试自动修复
                        subprocess.run(['fsck.ext4', '-y', nbd_dev], check=False)
                        # 修复后重试挂载
                        subprocess.run(mount_cmd, check=True, timeout=300)
                    else:
                        raise e
        except subprocess.TimeoutExpired:
            logger.error(f"Mount command timed out (Version {current_version}): {' '.join(mount_cmd)}")
            inst.status = "error" # 显式设置状态
            inst.error_msg = "挂载磁盘超时，可能是因为正在从云端下载大量元数据，请耐心等待或重试"
            raise Exception("挂载磁盘超时，可能是因为正在从云端下载大量元数据，请耐心等待或重试")
        except subprocess.CalledProcessError as e:
            # 如果报错信息包含 "already mounted"，视为成功
            if "already mounted" in str(e.stderr or "") or "already mounted" in str(e.output or ""):
                logger.info(f"Mount reported 'already mounted', treating as success (Version {current_version})")
            else:
                logger.error(f"Mount command failed (Version {current_version}): {e}")
                inst.status = "error" # 显式设置状态
                inst.error_msg = f"挂载失败: {e}"
                raise Exception(f"挂载失败: {e}")
        
        inst.startup_progress = 100
        inst.status = "running"
        logger.info(f"Disk {cfg.disk_name} started in {cfg.driver_mode} mode")
        
        # 启动后台扩容任务（如果是快速格式化）
        if locals().get('is_fast_format', False):
            def resize_task():
                import time
                inst.init_progress = 0
                inst.init_status = "Wait"
                time.sleep(5) # 等待系统稳定
                try:
                    logger.info(f"Starting online resize for {cfg.disk_name}...")
                    inst.init_status = "Locating"
                    # 查找挂载点对应的设备
                    find_cmd = ['findmnt', '-n', '-o', 'SOURCE', '-T', inst.final_mountpoint]
                    res = subprocess.run(find_cmd, capture_output=True, text=True)
                    device = res.stdout.strip()
                    
                    if device:
                        logger.info(f"Resizing device {device} for {cfg.disk_name}")
                        inst.init_status = "Resizing"
                        
                        # 启动 resize2fs
                        proc = subprocess.Popen(['resize2fs', device], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        
                        # 监控循环
                        target_size_bytes = cfg.disk_size_gb * 1024 * 1024 * 1024
                        while proc.poll() is None:
                            try:
                                st = os.statvfs(inst.final_mountpoint)
                                current_size_bytes = st.f_blocks * st.f_frsize
                                # 初始是 1GB，目标是 target
                                # 进度计算： (curr - 1G) / (target - 1G)
                                # 简化计算，直接用占比
                                progress = min(99, int((current_size_bytes / target_size_bytes) * 100))
                                inst.init_progress = progress
                            except: pass
                            time.sleep(1)
                            
                        # 检查结果
                        if proc.returncode == 0:
                            logger.info(f"Online resize completed for {cfg.disk_name}")
                            inst.init_progress = 100
                            inst.init_status = "Done"
                            time.sleep(2) # 展示一会儿完成状态
                            inst.init_progress = None # 隐藏进度条
                        else:
                            stderr = proc.stderr.read().decode()
                            logger.error(f"Resize failed: {stderr}")
                            inst.init_status = "Error"
                            # 保持错误状态供用户查看
                    else:
                        logger.warning(f"Could not find device for {inst.final_mountpoint}, skipping resize")
                        inst.init_progress = None
                except Exception as e:
                    logger.error(f"Resize failed for {cfg.disk_name}: {e}")
                    inst.init_status = "Error"
            
            threading.Thread(target=resize_task, daemon=True).start()

    except Exception as e:
        import traceback
        # 如果是新版本正在启动，不要用旧版本的错误覆盖它
        if inst.instance_version == current_version:
            inst.status = "error"
            inst.error_msg = str(e)
        logger.error(f"Mount failed (Version {current_version}): {e}\n{traceback.format_exc()}")

def unmount_disk(inst: DiskInstance):
    inst.status = "stopped"
    import subprocess
    try:
        # 1. 卸载最终挂载点
        subprocess.run(['umount', '-l', inst.final_mountpoint], check=False)
        
        if inst.config.driver_mode == "fuse":
            # 2. 卸载 FUSE
            subprocess.run(['fusermount3', '-u', inst.config.mount_path], check=False)
        else:
            # 2. 断开 NBD
            if inst.nbd_device:
                subprocess.run(['nbd-client', '-d', inst.nbd_device], check=False)
            if inst.nbd_server:
                inst.nbd_server.stop()
        
        subprocess.run(['umount', '-l', inst.config.mount_path], check=False)
    except Exception as e:
        logger.error(f"Unmount error: {e}")

# 全局管理
disks: Dict[str, DiskInstance] = {}

@app.on_event("startup")
async def startup_event():
    import time
    logger.info("FastAPI Backend Starting Up...")
    logger.info(f"Registered routes: {[route.path for route in app.routes]}")
    
    # 初始化加载
    for cfg_dict in load_configs():
        try:
            cfg = MountConfig(**cfg_dict)
            instance = DiskInstance(cfg)
            
            # 自动检测是否已经在运行
            if os.path.ismount(instance.final_mountpoint):
                logger.warning(f"Detected stale mount for disk {cfg.disk_name} (Service restarted), will re-mount.")
                try:
                    import subprocess
                    subprocess.run(['umount', '-l', instance.final_mountpoint], check=False)
                except: pass
            
            disks[cfg.disk_name] = instance
        except:
            pass
    
    # 后台状态更新逻辑
    def status_updater():
        last_cache_check = 0
        while True:
            try:
                # 1. 获取当前系统的所有挂载点
                import subprocess
                res = subprocess.run(['mount'], capture_output=True, text=True, timeout=2)
                mounts_output = res.stdout
                
                now = time.time()
                check_cache = (now - last_cache_check) > 30 # 每30秒检查一次缓存大小，而不是每5秒
                
                for name, instance in disks.items():
                    # 4. 更新上传进度和状态
                    try:
                        bm = None
                        if instance.config.driver_mode == "fuse":
                            if instance.vdrive:
                                bm = instance.vdrive.bm
                        else:
                            bm = instance.vdrive
                        
                        # 无论是否运行，先尝试基于配置计算总块数
                        instance.total_blocks = int((instance.config.disk_size_gb * 1024 * 1024 * 1024) // (instance.config.block_size_mb * 1024 * 1024))
                        
                        if bm:
                            with bm.upload_lock:
                                queue_size = len(bm.upload_queue)
                            with bm.uploading_lock:
                                uploading_size = bm.uploading_count
                            with bm.downloading_lock:
                                downloading_size = bm.downloading_count
                            instance.upload_queue_size = queue_size + uploading_size
                            instance.download_queue_size = downloading_size
                            instance.upload_speed = bm.upload_speed
                            instance.download_speed = bm.download_speed
                            instance.uploaded_blocks = int(bm.db.get_remote_exists_count())
                            instance.stored_size_bytes = instance.uploaded_blocks * (instance.config.block_size_mb * 1024 * 1024)
                            
                            if instance.upload_speed > 0 or instance.download_speed > 0:
                                logger.info(f"Disk {name} stats: UP={instance.upload_speed:.2f}, DOWN={instance.download_speed:.2f}, Blocks={instance.uploaded_blocks}/{instance.total_blocks}")
                        else:
                            # 如果 bm 不存在，尝试直接读取数据库文件获取进度
                            try:
                                img_name = instance.config.disk_name
                                if not img_name.endswith('.img'):
                                    img_name = f"{img_name}.img"
                                
                                db_path = os.path.join(instance.config.cache_dir, f'.{img_name}_metadata.db')
                                if os.path.exists(db_path):
                                    # 使用 sqlite3 直接读取，注意超时设置防止锁竞争
                                    with sqlite3.connect(db_path, timeout=1) as conn:
                                        cursor = conn.execute('SELECT COUNT(*) FROM blocks WHERE remote_exists = 1')
                                        instance.uploaded_blocks = int(cursor.fetchone()[0])
                                else:
                                    instance.uploaded_blocks = 0
                                
                                instance.stored_size_bytes = instance.uploaded_blocks * (instance.config.block_size_mb * 1024 * 1024)
                            except Exception:
                                instance.uploaded_blocks = 0
                                
                            instance.upload_queue_size = 0
                            instance.download_queue_size = 0
                            instance.upload_speed = 0
                            instance.download_speed = 0
                    except Exception as e:
                        logger.error(f"Error updating stats for {name}: {e}")

                    # 2. 计算缓存大小 (优化：使用 scandir 并降低频率)
                    if check_cache:
                        # 更新 Inode 使用情况 (仅在已挂载时)
                        if instance.loop_status == "mounted" and os.path.exists(instance.final_mountpoint):
                            try:
                                import os as native_os
                                st = native_os.statvfs(instance.final_mountpoint)
                                instance.total_inodes = st.f_files
                                instance.used_inodes = st.f_files - st.f_ffree
                            except Exception as st_err:
                                logger.debug(f"Failed to get statvfs for {name}: {st_err}")
                        else:
                            instance.total_inodes = 0
                            instance.used_inodes = 0

                        total_size = 0
                        try:
                            if os.path.exists(instance.config.cache_dir):
                                base_name = instance.config.disk_name
                                if not base_name.endswith('.img'):
                                    base_name = f"{base_name}.img"
                                
                                # 使用 scandir 提高效率
                                with os.scandir(instance.config.cache_dir) as it:
                                    for entry in it:
                                        # 匹配分块文件和元数据文件
                                        if entry.is_file() and (entry.name.startswith(base_name + "_blk_") or entry.name.startswith("." + base_name + "_metadata.db")):
                                            try:
                                                stat_info = entry.stat()
                                                total_size += stat_info.st_blocks * 512
                                            except: pass
                        except Exception as e:
                            logger.error(f"Cache stats error for {name}: {e}")
                        instance.cache_usage_bytes = total_size

                    # 3. 检测挂载状态
                    is_loop_mounted = f" {instance.final_mountpoint} " in mounts_output or mounts_output.endswith(f" {instance.final_mountpoint}")
                    is_fuse_mounted_in_list = f" {instance.config.mount_path} " in mounts_output or mounts_output.endswith(f" {instance.config.mount_path}")
                    
                    is_fuse_connected = False
                    if is_fuse_mounted_in_list:
                        # 使用 subprocess 调用 stat 并设置超时，防止 FUSE 挂起导致主状态线程死锁
                        try:
                            subprocess.run(['stat', '-t', instance.config.mount_path], 
                                         capture_output=True, timeout=1, check=True)
                            is_fuse_connected = True
                        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                            is_fuse_connected = False
                        except Exception:
                            is_fuse_connected = False
                    
                    if instance.config.driver_mode == "fuse":
                        if is_loop_mounted and is_fuse_connected:
                            instance.loop_status = "mounted"
                            # 仅当不在启动中时才更新为 running
                            if instance.status != "starting":
                                instance.status = "running"
                                instance.error_msg = ""
                        elif is_loop_mounted:
                            instance.loop_status = "mounted"
                            # 仅当不在启动中时才报连接断开错误
                            if instance.status != "starting":
                                instance.status = "error"
                                instance.error_msg = "FUSE 层连接断开 (Transport endpoint not connected)"
                        else:
                            instance.loop_status = "unmounted"
                            # 仅当当前状态为 running 时才自动切换到 stopped，排除 starting 状态
                            if instance.status == "running":
                                instance.status = "stopped"
                    else:
                        # NBD 模式
                        if is_loop_mounted:
                            instance.loop_status = "mounted"
                            # 仅当不在启动中时才更新为 running
                            if instance.status != "starting":
                                instance.status = "running"
                                instance.error_msg = ""
                        else:
                            instance.loop_status = "unmounted"
                            # 仅当当前状态为 running 时才自动切换到 stopped，排除 starting 状态
                            if instance.status == "running":
                                instance.status = "stopped"
                
                if check_cache:
                    last_cache_check = now
                    
            except Exception as e:
                logger.error(f"Status updater error: {e}")
            
            time.sleep(2)

    threading.Thread(target=status_updater, daemon=True).start()

    # 自动重连逻辑
    def auto_reconnect():
        while True:
            for name, instance in disks.items():
                if instance.status == "error" and "Transport endpoint not connected" in instance.error_msg:
                    logger.info(f"Attempting to auto-reconnect disk {name}...")
                    threading.Thread(target=do_mount, args=(instance,), daemon=True).start()
            time.sleep(30)
    
    threading.Thread(target=auto_reconnect, daemon=True).start()

    # 自动挂载所有配置好的磁盘
    def auto_mount_all():
        time.sleep(3) # 等待网络稳定
        logger.info("Auto-mounting all configured disks...")
        for name, instance in disks.items():
            if instance.status != "running":
                logger.info(f"Auto-mounting disk {name}...")
                threading.Thread(target=do_mount, args=(instance,), daemon=True).start()
                time.sleep(1) # 稍微错开启动时间
    
    threading.Thread(target=auto_mount_all, daemon=True).start()

# 静态文件路由
@app.get("/", response_class=HTMLResponse)
async def read_index():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return f.read()
    return "index.html not found"

@app.get("/favicon.ico")
async def favicon():
    if os.path.exists("favicon.ico"):
        return FileResponse("favicon.ico")
    return Response(status_code=204) # No content

@app.get("/disks")
async def list_disks():
    start_time = time.time()
    result = []
    for name, instance in disks.items():
        result.append({
            "disk_name": name,
            "status": instance.status,
            "loop_status": instance.loop_status,
            "config": instance.config,
            "cache_usage_bytes": instance.cache_usage_bytes,
            "upload_queue_size": instance.upload_queue_size,
            "download_queue_size": instance.download_queue_size,
            "upload_speed": instance.upload_speed,
            "download_speed": instance.download_speed,
            "total_blocks": instance.total_blocks,
            "uploaded_blocks": instance.uploaded_blocks,
            "stored_size_bytes": instance.stored_size_bytes,
            "used_inodes": instance.used_inodes,
            "total_inodes": instance.total_inodes,
            "error_msg": instance.error_msg,
            "final_mountpoint": instance.final_mountpoint,
            "startup_progress": instance.startup_progress,
            "init_progress": instance.init_progress,
            "init_status": instance.init_status
        })
    
    duration = time.time() - start_time
    if duration > 0.5:
        logger.warning(f"list_disks took {duration:.2f}s")
        
    return result

@app.get("/api/system/config")
async def get_system_config():
    return system_config

@app.post("/api/system/config")
async def update_system_config(config: SystemConfig):
    global system_config
    system_config = config
    save_system_config(system_config)
    return {"status": "ok"}

@app.post("/api/webdav/list_computers")
async def list_webdav_computers(req: Request):
    data = await req.json()
    dav_url = data.get("dav_url")
    dav_user = data.get("dav_user")
    dav_password = data.get("dav_password")
    
    if not all([dav_url, dav_user, dav_password]):
        raise HTTPException(status_code=400, detail="Missing credentials")
    
    from webdav4.client import Client as WebDAVClient
    try:
        client = WebDAVClient(dav_url, auth=(dav_user, dav_password), timeout=60.0, follow_redirects=True)
        
        # 确保 v_disks 目录存在
        if not client.exists("v_disks"):
            try:
                client.mkdir("v_disks")
            except:
                pass
            return []
            
        # 列出 v_disks 下的所有目录
        items = client.ls("v_disks", detail=True)
        # 过滤出目录，并提取名称（只取最后一部分，防止某些 WebDAV 返回全路径）
        computers = []
        for item in items:
            if item['type'] == 'directory':
                full_name = item['name'].strip('/')
                base_name = full_name.split('/')[-1]
                if base_name:
                    computers.append(base_name)
        
        logger.info(f"WebDAV computers found: {computers}")
        return computers
    except Exception as e:
        logger.error(f"WebDAV list failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/webdav/list_disks")
async def list_webdav_disks(req: Request):
    data = await req.json()
    dav_url = data.get("dav_url")
    dav_user = data.get("dav_user")
    dav_password = data.get("dav_password")
    computer_name = data.get("computer_name")
    
    if not all([dav_url, dav_user, dav_password, computer_name]):
        raise HTTPException(status_code=400, detail="Missing parameters")
    
    from webdav4.client import Client as WebDAVClient
    try:
        client = WebDAVClient(dav_url, auth=(dav_user, dav_password), timeout=60.0, follow_redirects=True)
        path = f"v_disks/{computer_name}"
        
        if not client.exists(path):
            return []
            
        items = client.ls(path, detail=True)
        disks_info = []
        for item in items:
            if item['type'] == 'directory':
                full_name = item['name'].strip('/')
                disk_name = full_name.split('/')[-1]
                if disk_name:
                    has_config = client.exists(f"{path}/{disk_name}/config.json")
                    disks_info.append({"name": disk_name, "has_config": has_config})
        
        logger.info(f"WebDAV disks found for {computer_name}: {disks_info}")
        return disks_info
    except Exception as e:
        logger.error(f"WebDAV list disks failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/webdav/get_disk_config")
async def get_webdav_disk_config(req: Request):
    data = await req.json()
    dav_url = data.get("dav_url")
    dav_user = data.get("dav_user")
    dav_password = data.get("dav_password")
    computer_name = data.get("computer_name")
    disk_name = data.get("disk_name")
    
    if not all([dav_url, dav_user, dav_password, computer_name, disk_name]):
        raise HTTPException(status_code=400, detail="Missing parameters")
    
    from webdav4.client import Client as WebDAVClient
    import io
    try:
        client = WebDAVClient(dav_url, auth=(dav_user, dav_password), timeout=60.0, follow_redirects=True)
        # 尝试获取 config.json
        # 路径规则：v_disks/计算机名/虚拟硬盘名/config.json
        config_path = f"v_disks/{computer_name}/{disk_name}/config.json"
        
        if not client.exists(config_path):
            return {"status": "not_found", "message": "Cloud config not found"}
            
        # 下载并解析
        bio = io.BytesIO()
        client.download_fileobj(config_path, bio)
        bio.seek(0)
        config_json = json.load(bio)
        return {"status": "ok", "config": config_json}
    except Exception as e:
        logger.error(f"WebDAV get config failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def sync_config_to_webdav_task(config: MountConfig):
    """后台同步配置到 WebDAV 的任务"""
    try:
        from webdav4.client import Client as WebDAVClient
        import io
        logger.info(f"Background: Starting cloud config sync for disk: {config.disk_name}")
        client = WebDAVClient(config.dav_url, auth=(config.dav_user, config.dav_password), timeout=60.0, follow_redirects=True)
        
        remote_path_clean = config.remote_path.strip('/')
        remote_config_path = f"{remote_path_clean}/config.json"
        
        # 确保远程目录存在
        parts = remote_path_clean.split('/')
        current = ""
        for part in parts:
            if not part: continue
            current = f"{current}/{part}" if current else part
            if not client.exists(current):
                client.mkdir(current)

        # 将配置转换为 JSON 字符串并上传
        config_data = config.model_dump()
        config_json = json.dumps(config_data, indent=4, ensure_ascii=False)
        client.upload_fileobj(io.BytesIO(config_json.encode('utf-8')), remote_config_path, overwrite=True)
        logger.info(f"Background: Successfully synced config to cloud: {remote_config_path}")
    except Exception as e:
        logger.error(f"Background: Failed to sync config to cloud: {e}")

@app.post("/mount")
async def mount_drive(config: MountConfig):
    # 路径规则强制执行/默认化
    # 1. WebDAV 保存路径：v_disks/计算机名/虚拟硬盘名
    if not config.remote_path or config.remote_path in ["blocks", "v_disks/blocks", f"v_disks/{config.disk_name}"]:
        config.remote_path = f"v_disks/{system_config.computer_name}/{config.disk_name}"
        logger.info(f"Automatically set remote_path to {config.remote_path}")

    # 2. 挂载路径 (FUSE 镜像层)：/mnt/fuse_mirror/虚拟硬盘名
    # 如果用户没填，或者使用的是旧的默认路径，则自动修正
    if not config.mount_path or any(old in config.mount_path for old in ["/tmp/v_drive_raw", "/mnt/v_drive_raw"]):
        config.mount_path = f"/mnt/fuse_mirror/{config.disk_name}"
        logger.info(f"Automatically set mount_path to {config.mount_path}")

    # 3. 本地缓存目录：/var/cache/虚拟硬盘名
    if not config.cache_dir or any(old in config.cache_dir for old in ["/tmp/v_drive_cache", "cache"]):
        config.cache_dir = f"/var/cache/{config.disk_name}"
        logger.info(f"Automatically set cache_dir to {config.cache_dir}")

    # 检查是否是更新现有磁盘
    existing_instance = disks.get(config.disk_name)
    if existing_instance:
        # 校验关键属性是否被修改
        old = existing_instance.config
        locked_changes = []
        if old.disk_name != config.disk_name: locked_changes.append("磁盘名称")
        if old.remote_path != config.remote_path: locked_changes.append("远程路径")
        if old.block_size_mb != config.block_size_mb: locked_changes.append("分块大小")
        if old.disk_size_gb != config.disk_size_gb: locked_changes.append("磁盘容量")
        if old.inode_ratio != config.inode_ratio: locked_changes.append("Inode 比例")
        if old.compression != config.compression: locked_changes.append("压缩算法")
        
        if locked_changes:
            raise HTTPException(status_code=400, detail=f"禁止修改关键属性: {', '.join(locked_changes)}。修改这些属性会导致现有数据损坏。")
        
        # 更新可变属性
        # 更新速率限制
        if existing_instance.vdrive:
            bm = existing_instance.vdrive if existing_instance.config.driver_mode == "nbd" else existing_instance.vdrive.bm
            bm.upload_limiter.set_limit(config.upload_limit_kb)
            bm.download_limiter.set_limit(config.download_limit_kb)
            
            # 动态更新缓存大小限制
            if config.max_cache_gb != old.max_cache_gb:
                new_size = config.max_cache_gb * 1024 * 1024 * 1024
                bm.max_cache_size = new_size
                logger.info(f"Dynamic update: Cache limit for {config.disk_name} changed to {config.max_cache_gb}GB")

        existing_instance.config = config
        instance = existing_instance
    else:
        # 检查路径冲突
        for name, inst in disks.items():
            if inst.config.mount_path == config.mount_path:
                raise HTTPException(status_code=400, detail=f"FUSE挂载路径冲突: {config.mount_path} 已被磁盘 {name} 使用")
        instance = DiskInstance(config)
        disks[config.disk_name] = instance

    # 保存配置
    configs = load_configs()
    updated = False
    for i, cfg in enumerate(configs):
        if cfg.get('disk_name') == config.disk_name:
            configs[i] = config.model_dump()
            updated = True
            break
    if not updated:
        configs.append(config.model_dump())
    save_configs(configs)

    # 异步同步配置到云端 WebDAV (config.json)
    # 不再阻塞主线程，改为后台执行
    threading.Thread(target=sync_config_to_webdav_task, args=(config,), daemon=True).start()
    
    sync_status = "pending"
    sync_msg = f"配置已保存，正在后台同步至云端: {config.remote_path.strip('/')}/config.json"

    if instance.status == "running":
        return {
            "message": f"磁盘 {config.disk_name} 配置已更新（部分改动需重启生效）",
            "sync_status": sync_status,
            "sync_msg": sync_msg
        }

    # 异步执行挂载逻辑
    threading.Thread(target=do_mount, args=(instance,), daemon=True).start()
    
    return {
        "message": f"磁盘 {config.disk_name} 挂载任务已启动",
        "sync_status": sync_status,
        "sync_msg": sync_msg
    }

@app.post("/unmount/{disk_name}")
def unmount_disk_endpoint(disk_name: str):
    instance = disks.get(disk_name)
    if not instance:
        raise HTTPException(status_code=404, detail="磁盘不存在")
    
    try:
        unmount_disk(instance)
        instance.loop_status = "unmounted"
        return {"message": f"磁盘 {disk_name} 已卸载"}
    except Exception as e:
        logger.error(f"Unmount disk {disk_name} error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/disks/{disk_name}")
def delete_disk(disk_name: str, delete_remote: bool = False):
    logger.info(f"Deleting disk: {disk_name}, delete_remote: {delete_remote}")
    instance = disks.get(disk_name)
    
    # 查找配置以获取远程路径和缓存目录信息（即使实例没运行）
    configs = load_configs()
    target_config = next((c for c in configs if c.get('disk_name') == disk_name), None)
    
    # 从配置文件中清理
    new_configs = [c for c in configs if c.get('disk_name') != disk_name]
    save_configs(new_configs)
    
    # 如果指定了删除远程文件
    if delete_remote and target_config:
        try:
            from webdav4.client import Client as WebDAVClient
            client = WebDAVClient(target_config['dav_url'], auth=(target_config['dav_user'], target_config['dav_password']), timeout=60.0, follow_redirects=True)
            remote_path = target_config['remote_path'].strip('/')
            if client.exists(remote_path):
                logger.info(f"Deleting remote directory: {remote_path}")
                client.remove(remote_path)
        except Exception as e:
            logger.error(f"Failed to delete remote files for {disk_name}: {e}")

    # 清理本地缓存文件
    if target_config:
        try:
            cache_dir = target_config['cache_dir']
            disk_name = target_config['disk_name']
            # 兼容带有 .img 后缀和不带后缀的情况
            prefixes = [
                f"{disk_name}_blk_", 
                f".{disk_name}_metadata.db",
                f"{disk_name}.img_blk_", 
                f".{disk_name}.img_metadata.db"
            ]
            if os.path.exists(cache_dir):
                for f in os.listdir(cache_dir):
                    if any(f.startswith(p) for p in prefixes):
                        try:
                            os.remove(os.path.join(cache_dir, f))
                        except: pass
                # 如果缓存目录为空（例如专为该磁盘创建的目录），则尝试删除目录
                try:
                    if not os.listdir(cache_dir):
                        os.rmdir(cache_dir)
                except: pass
        except Exception as e:
            logger.error(f"Failed to delete local cache for {disk_name}: {e}")

    if not instance:
        return {"message": f"磁盘 {disk_name} 配置及本地缓存已清理" + ("，远程文件已尝试删除" if delete_remote else "")}
    
    try:
        # 如果正在运行，先尝试卸载
        if instance.status in ["running", "starting", "error"]:
            try:
                import subprocess
                if os.path.ismount(instance.final_mountpoint):
                    subprocess.run(['umount', '-l', instance.final_mountpoint], check=False)
                if os.path.ismount(instance.config.mount_path):
                    subprocess.run(['fusermount3', '-u', instance.config.mount_path], check=False)
                    subprocess.run(['umount', '-l', instance.config.mount_path], check=False)
            except:
                pass
        
        if disk_name in disks:
            del disks[disk_name]
        
        logger.info(f"Disk {disk_name} deleted successfully")
        return {"message": f"磁盘 {disk_name} 已成功删除" + ("（含远程文件）" if delete_remote else "")}
    except Exception as e:
        logger.error(f"Error deleting disk {disk_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cache/prefetch")
def prefetch_cache(path: str):
    global vdrive_instance
    if not vdrive_instance:
        raise HTTPException(status_code=400, detail="Drive not mounted")
    
    success = vdrive_instance.prefetch(path)
    return {"success": success}

@app.post("/cache/evict")
def evict_cache(path: str):
    global vdrive_instance
    if not vdrive_instance:
        raise HTTPException(status_code=400, detail="Drive not mounted")
    
    success = vdrive_instance.evict(path)
    return {"success": success}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
