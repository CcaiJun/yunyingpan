from fastapi import FastAPI, HTTPException, Request
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
    
    inst.status = "starting"
    inst.error_msg = ""
    
    try:
        cfg = inst.config
        import subprocess
        import time
        logger.info(f"Pre-cleaning mount paths for {cfg.disk_name}")
        
        # 0. 预清理
        try:
            subprocess.run(['umount', '-l', inst.final_mountpoint], check=False, capture_output=True)
            if cfg.driver_mode == "fuse":
                subprocess.run(['fusermount3', '-u', cfg.mount_path], check=False, capture_output=True)
            else:
                if inst.nbd_device:
                    subprocess.run(['nbd-client', '-d', inst.nbd_device], check=False, capture_output=True)
            subprocess.run(['umount', '-l', cfg.mount_path], check=False, capture_output=True)
            os.makedirs(cfg.mount_path, exist_ok=True)
            os.makedirs(inst.final_mountpoint, exist_ok=True)
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")

        if cfg.driver_mode == "fuse":
            # --- FUSE 模式 ---
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
                try:
                    FUSE(inst.vdrive, cfg.mount_path, foreground=True, nonempty=True, allow_other=True, direct_io=True)
                except Exception as fuse_err:
                    logger.error(f"FUSE error: {fuse_err}")
                finally:
                    if inst.status != "stopped":
                        inst.status = "error"
                        inst.error_msg = "FUSE 进程异常退出"

            inst.thread = threading.Thread(target=start_fuse, daemon=True)
            inst.thread.start()

            img_name = cfg.disk_name if cfg.disk_name.endswith('.img') else f"{cfg.disk_name}.img"
            target_path = os.path.join(cfg.mount_path, img_name)
            
            # 等待 FUSE 镜像文件出现
            max_wait = 60
            while max_wait > 0 and not os.path.exists(target_path):
                if not inst.thread.is_alive(): raise Exception("FUSE 进程启动失败")
                time.sleep(1)
                max_wait -= 1
            if not os.path.exists(target_path): raise Exception("FUSE 镜像生成超时")
            
            mount_cmd = ['mount', '-o', 'loop', target_path, inst.final_mountpoint]

        else:
            # --- NBD 模式 ---
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
                try:
                    inst.nbd_server.start()
                except Exception as e:
                    logger.error(f"NBD server error: {e}")
                finally:
                    if inst.status != "stopped":
                        inst.status = "error"
                        inst.error_msg = "NBD 服务端异常退出"

            inst.thread = threading.Thread(target=start_nbd, daemon=True)
            inst.thread.start()
            
            # 寻找空闲 NBD 设备并连接
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
                
                if not nbd_dev: raise Exception("没有可用的 NBD 设备")
                
                inst.nbd_device = nbd_dev
                time.sleep(1) # 等待 server 启动
                
                # 连接 NBD 客户端 (在锁内连接以防竞争)
                subprocess.run(['nbd-client', '127.0.0.1', str(port), nbd_dev, '-N', 'default', '-g'], check=True)
            
            target_path = nbd_dev
            mount_cmd = ['mount', nbd_dev, inst.final_mountpoint]

        # 检查并格式化
        needs_format = True
        try:
            res = subprocess.run(['blkid', target_path], capture_output=True, text=True)
            if "TYPE=" in res.stdout: needs_format = False
        except: pass

        if needs_format:
            logger.info(f"Formatting {target_path}...")
            subprocess.run(['mkfs.ext4', '-F', '-i', str(cfg.inode_ratio), '-b', '4096', 
                          '-O', '^metadata_csum', '-E', 'lazy_itable_init=1,lazy_journal_init=1', target_path], check=True)

        # 最终挂载
        logger.info(f"Final mount: {' '.join(mount_cmd)}")
        subprocess.run(mount_cmd, check=True)
        
        inst.status = "running"
        logger.info(f"Disk {cfg.disk_name} started in {cfg.driver_mode} mode")

    except Exception as e:
        inst.status = "error"
        inst.error_msg = str(e)
        logger.error(f"Mount failed: {e}")

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

# 初始化加载
for cfg_dict in load_configs():
    try:
        cfg = MountConfig(**cfg_dict)
        instance = DiskInstance(cfg)
        
        # 自动检测是否已经在运行
        if os.path.ismount(instance.final_mountpoint):
            # 检查 FUSE 挂载点是否正常
            try:
                os.stat(instance.config.mount_path)
                instance.status = "running"
                logger.info(f"Detected existing healthy mount for disk {cfg.disk_name}, setting status to running")
            except:
                logger.warning(f"Detected broken FUSE mount for disk {cfg.disk_name}, setting status to error")
                instance.status = "error"
                instance.error_msg = "FUSE 层连接断开 (Transport endpoint not connected)"
        elif os.path.ismount(instance.config.mount_path):
            # FUSE 还在但 Loop 不在，可能是异常退出
            logger.warning(f"Detected partial mount for disk {cfg.disk_name} (FUSE ok, Loop missing)")
            instance.status = "error"
            instance.error_msg = "检测到残留挂载，请先卸载或重启"
        
        disks[cfg.disk_name] = instance
    except:
        pass

@app.on_event("startup")
async def startup_event():
    import time
    logger.info("FastAPI Backend Starting Up...")
    logger.info(f"Registered routes: {[route.path for route in app.routes]}")
    
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
                            instance.status = "running"
                            instance.error_msg = ""
                        elif is_loop_mounted:
                            instance.loop_status = "mounted"
                            instance.status = "error"
                            instance.error_msg = "FUSE 层连接断开 (Transport endpoint not connected)"
                        else:
                            instance.loop_status = "unmounted"
                            if instance.status == "running":
                                instance.status = "stopped"
                    else:
                        # NBD 模式
                        if is_loop_mounted:
                            instance.loop_status = "mounted"
                            instance.status = "running"
                            instance.error_msg = ""
                        else:
                            instance.loop_status = "unmounted"
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
    return FileResponse("favicon.ico")

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
            "final_mountpoint": instance.final_mountpoint
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
        client = WebDAVClient(dav_url, auth=(dav_user, dav_password))
        
        # 确保 v_disks 目录存在
        if not client.exists("v_disks"):
            try:
                client.mkdir("v_disks")
            except:
                pass
            return []
            
        # 列出 v_disks 下的所有目录
        items = client.ls("v_disks", detail=True)
        # 过滤出目录，并提取名称
        computers = [item['name'].strip('/') for item in items if item['type'] == 'directory']
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
        client = WebDAVClient(dav_url, auth=(dav_user, dav_password))
        path = f"v_disks/{computer_name}"
        
        if not client.exists(path):
            return []
            
        items = client.ls(path, detail=True)
        disks_info = []
        for item in items:
            if item['type'] == 'directory':
                name = item['name'].strip('/')
                has_config = client.exists(f"{path}/{name}/config.json")
                disks_info.append({"name": name, "has_config": has_config})
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
        client = WebDAVClient(dav_url, auth=(dav_user, dav_password))
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
        client = WebDAVClient(config.dav_url, auth=(config.dav_user, config.dav_password))
        
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
def unmount_disk(disk_name: str):
    instance = disks.get(disk_name)
    if not instance:
        raise HTTPException(status_code=404, detail="磁盘不存在")
    
    try:
        import subprocess
        # 1. 卸载 Loop 挂载点 (e.g., /mnt/v_disks/256)
        if os.path.ismount(instance.final_mountpoint):
            subprocess.run(['umount', '-l', instance.final_mountpoint], check=False)
        
        # 2. 卸载 FUSE 挂载点 (e.g., /mnt/pan256)
        if os.path.ismount(instance.config.mount_path):
            subprocess.run(['fusermount3', '-u', instance.config.mount_path], check=False)
            subprocess.run(['umount', '-l', instance.config.mount_path], check=False)
        
        instance.status = "stopped"
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
            client = WebDAVClient(target_config['dav_url'], auth=(target_config['dav_user'], target_config['dav_password']))
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
