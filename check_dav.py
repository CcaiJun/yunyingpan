import json
from webdav4.client import Client

with open('disks_config.json', 'r') as f:
    disks = json.load(f)

for disk in disks:
    print(f"Checking disk: {disk['disk_name']}")
    client = Client(disk['dav_url'], auth=(disk['dav_user'], disk['dav_password']))
    remote_path = disk['remote_path'].strip('/')
    try:
        files = client.ls(remote_path, detail=False)
        print(f"  Files in {remote_path}: {files}")
    except Exception as e:
        print(f"  Error listing {remote_path}: {e}")
