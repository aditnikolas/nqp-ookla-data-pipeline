import boto3
import logging
import argparse
import threading
from botocore import UNSIGNED
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from pathlib import Path


BUCKET = "ookla-open-data"
TRANSFER_CONFIG = TransferConfig(max_concurrency=20, multipart_chunksize=4 * 1024 * 1024)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Ingest Ookla Open Data ke bronze")
    parser.add_argument("--n-quarter", type=int, default=4)
    parser.add_argument("--type", choices=["mobile", "fixed"], default="mobile")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)

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

def make_s3_client(region="us-west-2", **config_kwargs):
    config = Config(signature_version=UNSIGNED, **config_kwargs)
    return boto3.client('s3', region_name=region, config=config)

def parse_partition(obj): 
    term = {}
    parts = obj['Key'].split('/') 
    term['Key'] = obj['Key']
    term['Size'] = obj['Size']
    for part in parts: 
        if '=' in part:
            key, value = part.split("=", 1)
            if value.isdigit():
                value = int(value)
            term[key] = value
    return term

def list_parquet_files(s3_client, bucket, data_type="mobile"):
    paginator = s3_client.get_paginator("list_objects_v2")
    quarter_list = []
    prefix = f"parquet/performance/type={data_type}"
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj['Key'].endswith('.parquet'):
                term = parse_partition(obj)
                quarter_list.append(term)
    if not quarter_list:
        raise ValueError(f"No file .parquet in s3://{bucket}/{prefix}")
    return quarter_list

def pick_latest_quarters(quarter_list, n=4):
    all_quarter = {(d['year'], d['quarter']) for d in quarter_list}
    latest_quarter = sorted(all_quarter)[-n:]
    files_to_download = []
    for d in quarter_list:
        if (d["year"], d["quarter"]) in latest_quarter:
            files_to_download.append(d)
    files_to_download.sort(key=lambda d: (d["year"], d["quarter"]))
    return files_to_download

def build_local_path(file, base_dir):
    filename = file['Key'].split("/")[-1]
    folder_path = base_dir / f"type={file['type']}" / f"year={file['year']}" / f"quarter={file['quarter']}"
    final_path = folder_path / filename
    return final_path

def download_one(s3_client, file, final_path, bucket):
    filename = final_path.name
    if is_already_downloaded(file, final_path):
        return "Skipped"

    if final_path.exists():
        logger.warning("File different (local=%s, s3=%s), Re-download: %s",
                        final_path.stat().st_size, file["Size"], final_path)
        logger.info("Deleting existing file %s", final_path)
        final_path.unlink(missing_ok=True)

    final_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        logger.info("Downloading %s ...", filename)
        s3_client.download_file(bucket, file['Key'], str(final_path), Config=TRANSFER_CONFIG, Callback=DownloadProgress(file["Size"], filename))
        logger.info("Download finish %s ...", filename)
    except Exception:
        logger.exception("Download Failed: %s", file['Key'])
        raise

    logger.info("Validating %s ...", filename)
    local_size = final_path.stat().st_size
    if local_size != file["Size"]:
        final_path.unlink(missing_ok=True)
        raise ValueError(f"Different size! local={local_size}, s3={file['Size']}")
    logger.info("File size validated! %.1f MB", local_size / 1e6)
    return "Downloaded"

def is_already_downloaded(file, final_path):
    if final_path.exists():
        if final_path.stat().st_size == file['Size']:
            return True
    return False
    
def main(argv=None):
    args = parse_args(argv)
    logger.info("Setelan: %s", vars(args))

    s3 = make_s3_client("us-west-2", read_timeout=15, 
                        retries={
                                "max_attempts": 10, 
                                "mode": "standard"
                        })
    quarter_list = list_parquet_files(s3, BUCKET, args.type)
    dict_quarter = pick_latest_quarters(quarter_list, args.n_quarter)
    base_dir = Path("data") / "bronze" / "ookla"

    for file in dict_quarter:
        final_path = build_local_path(file, base_dir)

        # ===== dry-run di sini =====
        if args.dry_run:
            action = "SKIP" if is_already_downloaded(file, final_path) else "DOWNLOAD"
            logger.info("[dry-run] %s -> %s", action, final_path)
            continue          # loncat ke file berikutnya, download_one gak kepanggil
        # ===========================

        status = download_one(s3, file, final_path, BUCKET)
        logger.info("%s: %s", status.upper(), final_path.name)


if __name__ == "__main__":
    main()
