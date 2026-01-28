#!/bin/bash

# Yingpan 备份脚本
# 此脚本用于打包应用代码和磁盘配置，以便在重装系统后恢复。

BACKUP_NAME="yingpan_backup_$(date +%Y%m%d_%H%M%S).tar.gz"
echo "--- 开始备份 Yingpan ---"

# 检查配置文件是否存在
if [ ! -f "disks_config.json" ]; then
    echo "警告：未找到 disks_config.json。将只备份源代码。"
fi

# 打包必要文件
# 排除 venv, __pycache__, server.log, test_file_*
tar -czf "$BACKUP_NAME" \
    --exclude="venv" \
    --exclude="__pycache__" \
    --exclude="server.log" \
    --exclude="test_file_*" \
    --exclude="*.tar.gz" \
    .

echo "--- 备份完成 ---"
echo "备份文件已保存为: $BACKUP_NAME"
echo "请将此文件下载并保存到安全的地方（如 U 盘或云盘）。"
echo ""
echo "恢复步骤："
echo "1. 在新系统中解压此文件: tar -xzf $BACKUP_NAME"
echo "2. 进入解压后的目录"
echo "3. 运行部署脚本: sudo ./deploy.sh"
echo "4. 系统会自动安装依赖并根据 disks_config.json 自动挂载磁盘。"
