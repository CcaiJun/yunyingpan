import json
from webdav4.client import Client

with open('disks_config.json', 'r') as f:
    disks = json.load(f)

for disk in disks:
    if disk['disk_name'] == '测试同步3':
        client = Client(disk['dav_url'], auth=(disk['dav_user'], disk['dav_password']))
        index_path = f"{disk['remote_path'].strip('/')}/block_index.json"
        try:
            import io
            bio = io.BytesIO()
            client.download_fileobj(index_path, bio)
            print(f"Index content for {disk['disk_name']}: {bio.getvalue().decode()}")
        except Exception as e:
            print(f"Error reading index: {e}")
