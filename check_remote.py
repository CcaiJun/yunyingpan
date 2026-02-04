
from webdav4.client import Client
import os

client = Client("https://webdav.123pan.cn/webdav", auth=("18096322625", "9tcq5hiu"))
files = client.ls("v_disks/la2c4g/shiping", detail=True)
for f in files[:10]:
    print(f"{f['name']}: {f['size']} bytes")
