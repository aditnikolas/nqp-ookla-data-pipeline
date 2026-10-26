import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pathlib import Path

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

import threading
class DownloadProgress:
    def __init__(self, total_bytes, name):
        self.total = total_bytes
        self.name = name
        self.seen = 0
        self.next_pct = 10
        self.lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self.lock:
            self.seen += bytes_amount
            pct = self.seen / self.total * 100
            if pct >= self.next_pct:
                logger.info("%s: %3.0f%% (%.1f / %.1f MB)",
                            self.name, pct, self.seen / 1e6, self.total / 1e6)
                self.next_pct += 10

BUCKET = "ookla-open-data"
REGION = "us-west-2"
s3 = boto3.client('s3', region_name=REGION, config=Config(signature_version=UNSIGNED))
paginator = s3.get_paginator("list_objects_v2")
quarter_list = []
for page in paginator.paginate(Bucket=BUCKET, Prefix="parquet/performance/type=mobile/"): 
    for obj in page.get("Contents", []): 
        if obj['Key'].endswith('.parquet'): 
            parts = obj['Key'].split('/') 
            term = {}
            term['Key'] = obj['Key']
            term['Size'] = obj['Size']
            for part in parts: 
                if '=' in part:
                    key, value = part.split("=", 1)
                    if value.isdigit():
                        value = int(value)
                    term[key] = value
            quarter_list.append(term)

quarter = {(d['year'], d['quarter']) for d in quarter_list}
latest_quarter = sorted(quarter)[-4:]
files_to_download = []
for d in quarter_list:
    if (d["year"], d["quarter"]) in latest_quarter:
        files_to_download.append(d)

files_to_download.sort(key=lambda d: (d["year"], d["quarter"]))
# for f in files_to_download:
    # print(f"Key: {f['Key']}, Year: {f['year']}, Quarter: {f['quarter']}")

ookla_file = files_to_download[-1]
filename = ookla_file['Key'].split("/")[-1]
folder_path = Path("data") / "bronze" / "ookla" / f"type={ookla_file['type']}" / f"year={ookla_file['year']}" / f"quarter={ookla_file['quarter']}"
folder_path.mkdir(parents=True, exist_ok=True)
final_path = Path(folder_path / filename)

try:
    logger.info(f"Downloading file : {filename} ...")
    s3.download_file(BUCKET, ookla_file['Key'], str(final_path),
    Callback=DownloadProgress(ookla_file["Size"], filename))
except Exception:
    logger.exception("Download Failed: %s", ookla_file['Key'])
    raise

local_size = final_path.stat().st_size
if local_size != ookla_file["Size"]:
    raise ValueError(f"Ukuran beda! lokal={local_size}, s3={ookla_file['Size']}")
logger.info("Ukuran cocok: %.1f MB", local_size / 1e6)



