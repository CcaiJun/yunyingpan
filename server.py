from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import threading
import logging
from fastapi.middleware.cors import CORSMiddleware
from v_drive import VDrive, FUSE
import psutil

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

class DiskInstance:
    def __init__(self, config: MountConfig):
        self.config = config
        self.vdrive = None
        self.thread = None
        self.status = "stopped" # stopped, starting, running, error
        self.error_msg = ""
        self.final_mountpoint = f"/mnt/v_disks/{config.disk_name}"

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

# 全局管理
disks: Dict[str, DiskInstance] = {}

# 初始化加载
for cfg_dict in load_configs():
    try:
        cfg = MountConfig(**cfg_dict)
        disks[cfg.disk_name] = DiskInstance(cfg)
    except:
        pass

@app.get("/disks")
def list_disks():
    result = []
    for name, instance in disks.items():
        total_size = 0
        if os.path.exists(instance.config.cache_dir):
            for root, dirs, files in os.walk(instance.config.cache_dir):
                for f in files:
                    if f.startswith('.') or f == '.metadata.db': continue
                    try: total_size += os.path.getsize(os.path.join(root, f))
                    except: pass
        
        loop_status = "unmounted"
        try:
            import subprocess
            res = subprocess.run(['mount'], capture_output=True, text=True)
            if instance.final_mountpoint in res.stdout:
                loop_status = "mounted"
        except: pass

        result.append({
            "disk_name": name,
            "status": instance.status,
            "loop_status": loop_status,
            "config": instance.config,
            "cache_usage_bytes": total_size,
            "upload_queue_size": len(instance.vdrive.upload_queue) if instance.vdrive else 0,
            "error_msg": instance.error_msg,
            "final_mountpoint": instance.final_mountpoint
        })
    return result

@app.post("/mount")
async def mount_drive(config: MountConfig):
    # 冲突检测
    if config.disk_name in disks and disks[config.disk_name].status == "running":
        # 如果名称相同且正在运行，尝试更新或报错
        pass # 后续处理更新逻辑
    
    # 检查路径冲突
    for name, inst in disks.items():
        if name != config.disk_name:
            if inst.config.mount_path == config.mount_path:
                raise HTTPException(status_code=400, detail=f"FUSE挂载路径冲突: {config.mount_path} 已被磁盘 {name} 使用")
    
    # 创建或更新实例
    instance = disks.get(config.disk_name)
    if not instance:
        instance = DiskInstance(config)
        disks[config.disk_name] = instance
    else:
        instance.config = config # 更新配置
        
    # 保存配置到文件
    all_configs = [inst.config.dict() for inst in disks.values()]
    save_configs(all_configs)

    # 异步执行挂载逻辑
    instance.status = "starting"
    instance.error_msg = ""
    
    def do_mount(inst: DiskInstance):
        try:
            cfg = inst.config
            # 0. 预清理：如果挂载路径已存在且是挂载点，尝试先卸载
            import subprocess
            try:
                subprocess.run(['fusermount3', '-u', cfg.mount_path], check=False, capture_output=True)
                subprocess.run(['umount', '-l', cfg.mount_path], check=False, capture_output=True)
            except:
                pass

            # 1. 启动 FUSE 层
            inst.vdrive = VDrive(
                dav_url=cfg.dav_url, 
                dav_user=cfg.dav_user, 
                dav_password=cfg.dav_password, 
                cache_dir=cfg.cache_dir, 
                disk_size_gb=cfg.disk_size_gb,
                max_cache_size_gb=cfg.max_cache_gb,
                block_size_mb=cfg.block_size_mb,
                img_name=cfg.disk_name,
                remote_path=cfg.remote_path,
                concurrency=cfg.concurrency
            )
            
            os.makedirs(cfg.mount_path, exist_ok=True)
            
            def start_fuse():
                FUSE(inst.vdrive, cfg.mount_path, foreground=True, nonempty=True, allow_other=True, direct_io=True)
                inst.status = "stopped"
            
            inst.thread = threading.Thread(target=start_fuse, daemon=True)
            inst.thread.start()
            
            # 2. 等待镜像并挂载 Loop
            img_name = cfg.disk_name if cfg.disk_name.endswith('.img') else f"{cfg.disk_name}.img"
            img_path = os.path.join(cfg.mount_path, img_name)
            
            import time
            max_wait = 20
            while not os.path.exists(img_path) and max_wait > 0:
                time.sleep(1)
                max_wait -= 1
            
            if not os.path.exists(img_path):
                raise Exception("FUSE镜像未能按时生成")

            os.makedirs(inst.final_mountpoint, exist_ok=True)
            import subprocess
            subprocess.run(['umount', '-l', inst.final_mountpoint], check=False)
            
            # 检查格式化
            needs_format = True
            try:
                res = subprocess.run(['blkid', img_path], capture_output=True, text=True)
                if "TYPE=" in res.stdout: needs_format = False
            except: pass
            
            if needs_format:
                subprocess.run(['mkfs.ext4', '-F', img_path], check=True)
            
            subprocess.run(['mount', '-o', 'loop', img_path, inst.final_mountpoint], check=True)
            inst.status = "running"
            logger.info(f"Disk {cfg.disk_name} mounted successfully at {inst.final_mountpoint}")
            
        except Exception as e:
            inst.status = "error"
            inst.error_msg = str(e)
            logger.error(f"Mount disk {inst.config.disk_name} failed: {e}")

    threading.Thread(target=do_mount, args=(instance,), daemon=True).start()
    
    return {"message": f"磁盘 {config.disk_name} 挂载任务已启动"}

@app.post("/unmount/{disk_name}")
def unmount_disk(disk_name: str):
    instance = disks.get(disk_name)
    if not instance:
        raise HTTPException(status_code=404, detail="磁盘不存在")
    
    try:
        import subprocess
        # 1. 卸载 Loop
        subprocess.run(['umount', '-l', instance.final_mountpoint], check=False)
        # 2. 卸载 FUSE
        subprocess.run(['fusermount3', '-u', instance.config.mount_path], check=False)
        subprocess.run(['umount', '-l', instance.config.mount_path], check=False)
        
        instance.status = "stopped"
        return {"message": f"磁盘 {disk_name} 已卸载"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/disks/{disk_name}")
def delete_disk(disk_name: str):
    logger.info(f"Deleting disk: {disk_name}")
    instance = disks.get(disk_name)
    if not instance:
        # 即使不存在也尝试从配置文件中删除（以防万一同步出错）
        configs = load_configs()
        new_configs = [c for c in configs if c.get('disk_name') != disk_name]
        if len(configs) != len(new_configs):
            save_configs(new_configs)
            return {"message": f"磁盘 {disk_name} 配置已从文件中清理"}
        raise HTTPException(status_code=404, detail="磁盘不存在")
    
    try:
        if instance.status == "running" or instance.status == "starting":
            try:
                unmount_disk(disk_name)
            except:
                pass
        
        if disk_name in disks:
            del disks[disk_name]
        
        # 强制更新配置文件
        all_configs = [inst.config.dict() for inst in disks.values()]
        save_configs(all_configs)
        logger.info(f"Disk {disk_name} deleted successfully")
        return {"message": f"磁盘 {disk_name} 已成功删除"}
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
