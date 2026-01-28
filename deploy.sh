#!/bin/bash

# Yingpan (虚拟 WebDAV 磁盘) 部署脚本
# 此脚本用于自动化安装和配置 Yingpan 后端。

# 1. 检查 root 权限和运行位置
if [ "$EUID" -ne 0 ]; then
  echo "请以 root 权限运行（使用 sudo）。"
  exit 1
fi

if [ ! -f "server.py" ]; then
  echo "错误：当前目录下未找到 server.py。"
  echo "请在应用根目录下运行此脚本。"
  exit 1
fi

echo "--- 开始部署 Yingpan ---"

# 2. 检查端口占用
if lsof -Pi :8001 -sTCP:LISTEN -t >/dev/null ; then
    echo "警告：端口 8001 已被占用。如果 Yingpan 已经在运行，脚本将继续尝试更新。"
fi

# 3. 更新并安装系统依赖
echo "正在安装系统依赖..."
apt-get update
apt-get install -y python3 python3-pip python3-venv fuse3 e2fsprogs jq lsof psmisc nbd-client

# 4. 加载内核模块
echo "正在加载内核模块 (FUSE & NBD)..."
modprobe fuse || echo "警告：无法加载 fuse 模块。"
if ! lsmod | grep -q "^nbd"; then
    modprobe nbd nbds_max=128 || echo "警告：无法加载 nbd 模块。"
fi

# 确保开机自动加载模块
echo "正在配置开机自动加载模块..."
echo "fuse" > /etc/modules-load.d/yingpan.conf
echo "nbd" >> /etc/modules-load.d/yingpan.conf
echo "options nbd nbds_max=128" > /etc/modprobe.d/nbd.conf

# 5. 配置 FUSE
if [ -f /etc/fuse.conf ]; then
    if ! grep -q "^user_allow_other" /etc/fuse.conf; then
        echo "正在 /etc/fuse.conf 中启用 user_allow_other..."
        sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf
    fi
fi

# 6. 创建必要目录
echo "正在创建系统目录..."
mkdir -p /mnt/v_disks
mkdir -p /var/cache/v_drive
chmod 777 /var/cache/v_drive

# 7. 设置 Python 虚拟环境
echo "正在设置 Python 虚拟环境..."
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi
source venv/bin/activate

# 8. 安装 Python 依赖
echo "正在安装 Python 依赖..."
pip install --upgrade pip
pip install -r requirements.txt

# 9. 创建 systemd 服务文件
echo "正在创建 systemd 服务..."
WORKING_DIR=$(pwd)
SERVICE_FILE="/etc/systemd/system/yingpan.service"

cat <<EOF > $SERVICE_FILE
[Unit]
Description=Yingpan - Virtual WebDAV Disk Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$WORKING_DIR
ExecStart=$WORKING_DIR/venv/bin/python3 server.py
Restart=always
RestartSec=5
LimitNOFILE=65536
StandardOutput=append:$WORKING_DIR/server.log
StandardError=append:$WORKING_DIR/server.log

[Install]
WantedBy=multi-user.target
EOF

# 10. 重新加载 systemd 并启动服务
echo "正在启动 Yingpan 服务..."
systemctl daemon-reload
systemctl enable yingpan
systemctl restart yingpan

echo "--- 部署完成 ---"
echo "服务状态："
systemctl status yingpan --no-pager

echo ""
echo "管理控制台地址: http://localhost:8001"
echo "日志文件路径: $WORKING_DIR/server.log"
echo "使用 'systemctl stop yingpan' 停止服务。"
echo "使用 'systemctl start yingpan' 启动服务。"
echo "使用 'systemctl restart yingpan' 重启服务。"
