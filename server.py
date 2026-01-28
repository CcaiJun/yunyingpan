from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
import os
import threading
import logging
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from v_drive import VDrive, FUSE
import psutil

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    inode_ratio: int = 4194304  # 默认 4MB (对应 largefile4)
    driver_mode: str = "fuse" # fuse or nbd

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
                img_name=cfg.disk_name, remote_path=cfg.remote_path, concurrency=cfg.concurrency
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
                img_name=cfg.disk_name, remote_path=cfg.remote_path, concurrency=cfg.concurrency
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
            
            # 寻找空闲 NBD 设备
            nbd_dev = None
            for i in range(16):
                dev = f"/dev/nbd{i}"
                if subprocess.run(['nbd-client', '-check', dev], capture_output=True).returncode != 0:
                    nbd_dev = dev
                    break
            if not nbd_dev: raise Exception("没有可用的 NBD 设备")
            
            inst.nbd_device = nbd_dev
            time.sleep(1) # 等待 server 启动
            
            # 连接 NBD 客户端 (指定 export name 以兼容部分客户端，-g 尝试禁用 NBD_OPT_GO)
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
                    # 4. 更新上传队列大小
                    bm = None
                    if instance.config.driver_mode == "fuse":
                        if instance.vdrive:
                            bm = instance.vdrive.bm
                    else:
                        bm = instance.vdrive # NBD 模式下 instance.vdrive 直接是 BlockManager
                    
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
                        if instance.upload_speed > 0 or instance.download_speed > 0:
                            logger.info(f"Disk {name} speed: UP={instance.upload_speed}, DOWN={instance.download_speed}")
                    else:
                        instance.upload_queue_size = 0
                        instance.download_queue_size = 0
                        instance.upload_speed = 0
                        instance.download_speed = 0

                    # 2. 计算缓存大小 (优化：降低频率)
                    if check_cache:
                        total_size = 0
                        try:
                            if os.path.exists(instance.config.cache_dir):
                                # 严格匹配：v_drive.py 中使用的是 self.img_name + "_blk_"
                                # 其中 self.img_name = disk_name + ".img" (如果原名不带.img)
                                base_name = instance.config.disk_name
                                if not base_name.endswith('.img'):
                                    base_name = f"{base_name}.img"
                                
                                # 构建完整前缀，例如 "disk1.img_blk_"
                                # 这样可以避免匹配到 "disk11.img_blk_"
                                prefix = f"{base_name}_blk_"
                                
                                for f in os.listdir(instance.config.cache_dir):
                                    if f.startswith(prefix) and f.endswith(".dat"):
                                        try:
                                            f_path = os.path.join(instance.config.cache_dir, f)
                                            # 使用 st_blocks * 512 获取文件在磁盘上实际占用的空间 (考虑稀疏文件)
                                            stat_info = os.stat(f_path)
                                            total_size += stat_info.st_blocks * 512
                                        except: pass
                        except Exception as e:
                            logger.error(f"Cache stats error for {name}: {e}")
                        instance.cache_usage_bytes = total_size
                    
                    # 3. 检测挂载状态
                    # 优化正则匹配，减少开销
                    is_loop_mounted = f" {instance.final_mountpoint} " in mounts_output or mounts_output.endswith(f" {instance.final_mountpoint}")
                    is_fuse_mounted_in_list = f" {instance.config.mount_path} " in mounts_output or mounts_output.endswith(f" {instance.config.mount_path}")
                    
                    is_fuse_connected = False
                    if is_fuse_mounted_in_list:
                        try:
                            # 仅尝试获取元数据，不读内容，避免 FUSE 挂起导致后端挂起
                            # 使用低层级的 os.stat 并设置超时（如果可能）
                            os.stat(instance.config.mount_path)
                            is_fuse_connected = True
                        except:
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
def list_disks():
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
            "error_msg": instance.error_msg,
            "final_mountpoint": instance.final_mountpoint
        })
    return result

@app.post("/mount")
async def mount_drive(config: MountConfig):
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
        
        if locked_changes:
            raise HTTPException(status_code=400, detail=f"禁止修改关键属性: {', '.join(locked_changes)}。修改这些属性会导致现有数据损坏。")
        
        # 更新可变属性
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

    if instance.status == "running":
        return {"message": f"磁盘 {config.disk_name} 配置已更新（部分改动需重启生效）"}

    # 异步执行挂载逻辑
    threading.Thread(target=do_mount, args=(instance,), daemon=True).start()
    
    return {"message": f"磁盘 {config.disk_name} 挂载任务已启动"}

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
