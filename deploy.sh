#!/bin/bash

# Yingpan (Virtual WebDAV Disk) Deployment Script
# This script automates the setup of the Yingpan backend.

# 1. Check for root privileges and location
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root (use sudo)."
  exit 1
fi

if [ ! -f "server.py" ]; then
  echo "Error: server.py not found in current directory."
  echo "Please run this script from the application folder."
  exit 1
fi

# Check if FUSE kernel module is loaded
if ! lsmod | grep -q fuse; then
  echo "Loading fuse kernel module..."
  modprobe fuse || echo "Warning: Could not load fuse module. FUSE may not work."
fi

echo "--- Starting Yingpan Deployment ---"

# 2. Update and install system dependencies
echo "Installing system dependencies..."
apt-get update
apt-get install -y python3 python3-pip python3-venv fuse3 e2fsprogs jq lsof psmisc

# 3. Create necessary directories
echo "Creating system directories..."
mkdir -p /mnt/v_disks
mkdir -p /var/cache/v_drive
chmod 777 /var/cache/v_drive

# 4. Set up Python virtual environment
echo "Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi
source venv/bin/activate

# 5. Install Python dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# 6. Create systemd service file
echo "Creating systemd service..."
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
StandardOutput=append:$WORKING_DIR/server.log
StandardError=append:$WORKING_DIR/server.log

[Install]
WantedBy=multi-user.target
EOF

# 7. Reload systemd and start service
echo "Starting Yingpan service..."
systemctl daemon-reload
systemctl enable yingpan
systemctl restart yingpan

echo "--- Deployment Complete ---"
echo "Service status:"
systemctl status yingpan --no-pager

echo ""
echo "Dashboard is available at: http://localhost:8001"
echo "Log file: $WORKING_DIR/server.log"
echo "Use 'systemctl stop yingpan' to stop the service."
echo "Use 'systemctl start yingpan' to start the service."
echo "Use 'systemctl restart yingpan' to restart the service."
