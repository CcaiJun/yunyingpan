
import logging
from webdav4.client import Client
import io

logging.basicConfig(level=logging.INFO)

client = Client("https://webdav.123pan.cn/webdav", auth=("18096322625", "9tcq5hiu"))

print("Testing download_fileobj...")
# 找一个存在的文件
files = client.ls("v_disks/la2c4g/shiping", detail=False)
if not files:
    print("No files found")
    exit()

target = files[0]
print(f"Downloading {target}")

buff = io.BytesIO()
# 这会触发 PROPFIND 吗？
client.download_fileobj(target, buff)
print(f"Downloaded {len(buff.getvalue())} bytes")
