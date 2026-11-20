import time
import logging
from pathlib import Path
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

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
KEY = "parquet/performance/type=mobile/year=2026/quarter=3/2026-07-01_performance_mobile_tiles.parquet"
SIZE = 189_644_612
TMP = Path("/tmp/bench.parquet")

def run(label, client_config, transfer_config):
    s3 = boto3.client("s3", region_name="us-west-2", config=client_config)
    TMP.unlink(missing_ok=True)
    start = time.perf_counter()
    s3.download_file(BUCKET, KEY, str(TMP),
                     Config=transfer_config,
                     Callback=DownloadProgress(SIZE, label))
    elapsed = time.perf_counter() - start
    logger.info("[%s] selesai %.1f detik | %.2f MB/s", label, elapsed, SIZE / 1e6 / elapsed)
    TMP.unlink(missing_ok=True)


if __name__ == "__main__":
    label = KEY.split("/")[-1]
    # client_config = Config(signature_version=UNSIGNED)
    client_config = Config(signature_version=UNSIGNED, read_timeout=15, 
                           retries={
                               "max_attempts": 10, 
                               "mode": "standard"
                            })
    # transfer_config = TransferConfig()
    transfer_config = TransferConfig(max_concurrency=20, 
                                     multipart_chunksize=4 * 1024 * 1024)
    
    run(label, client_config, transfer_config)

