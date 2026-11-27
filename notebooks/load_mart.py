from dotenv import load_dotenv
from pathlib import Path

import logging
import os
import psycopg
import duckdb as dcb
import argparse

load_dotenv()
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# POSTGRES_DB = os.getenv("POSTGRES_DB")

# if POSTGRES_DB == None or POSTGRES_PASSWORD == None or POSTGRES_USER==None:
#     raise ValueError("There is missing DB Name, Password or Username information on .env files")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

GOLD_ROOT = Path("data/gold/network-performance-province")
MART_TABLE = "mart_network_quality_province"
COLUMNS = ['prov_id', 'type', 'year', 'quarter', 'provinsi',
           'n_tiles', 'tests', 'devices',  
           'dl_mbps', 'ul_mbps', 'latency_ms']

    
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Create Gold Data")
    parser.add_argument("--n-quarter", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-type", choices=["mobile", "fixed"], default="mobile")
    return parser.parse_args(argv)

def get_db_config() -> dict:
    configuration = {
        "user" : os.getenv("POSTGRES_USER"), 
        "password" : os.getenv("POSTGRES_PASSWORD"),
        "dbname" : os.getenv("POSTGRES_DB"),
        "host" : "localhost",
        "port" : "5432"
    }
    for key, value in configuration.items():
        if value is None:
            raise ValueError("There is None Value in : ", key)
    return configuration

def list_gold_quarters(n: int, type: str) -> list[dict]:
    folder = {f.parent for f in GOLD_ROOT.rglob("*.parquet")}
    quarter_list = []
    for f in folder:
        term = {}
        slash_strip = f.parts
        for part in slash_strip:
            if "=" in part:
                key, value = part.split("=", 1)
                if value.isdigit():
                    value = int(value)
                term[key] = value

        if term.get("type") == type:
            quarter_list.append(term)

    all_quarter = {(d['year'], d['quarter']) for d in quarter_list}
    latest_quarter = sorted(all_quarter)[-n:]
    selected = []
    for d in quarter_list:
        if (d["year"], d["quarter"]) in latest_quarter:
            selected.append(d)
    selected.sort(key=lambda d: (d["year"], d["quarter"]))
    return selected

def read_gold_quarter(d: dict) -> list[tuple]:

    cols = ", ".join(COLUMNS)
    pattern = f"{GOLD_ROOT}/*/*/*/*.parquet" 

    rows = dcb.execute(
        f"""
        SELECT {cols}
        FROM read_parquet('{pattern}', hive_partitioning = true)
        WHERE type = ? AND year = ? AND quarter = ?
        """,
        [d["type"], d["year"], d["quarter"]]
    ).fetchall()

    if not rows:
        raise ValueError(f"Empty Gold Data {d}: check path : {pattern}")
    logger.info("Gold %s %s Q%s: %d baris", d["type"], d["year"], d["quarter"], len(rows))
    return rows

# ===== 4. load ke mart (inti!) =====
def load_quarter(conn, d: dict, rows: list[tuple]) -> int:
    sstring = []
    for i in range(len(COLUMNS)):
        sstring.append('%s')

    placer = ", ".join(sstring)
    cols = ", ".join(COLUMNS)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM {MART_TABLE}
                WHERE type=%s AND year=%s AND quarter=%s;
            """, (d['type'], d['year'], d['quarter']))
            cur.executemany(f"""
                INSERT INTO {MART_TABLE} ({cols})
                VALUES ({placer})
                """, rows)
    logger.info("Load %s %s Q%s: %d baris", d["type"], d["year"], d["quarter"], len(rows))
    return len(rows)

# ===== 5. cek hasil load =====
def verify_load(conn, d: dict, rows: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*) as total_row, sum(tests) as total_test
            FROM {MART_TABLE}
            WHERE type=%s AND year=%s AND quarter=%s;
        """, (d['type'], d['year'], d['quarter']))
        pg_rows, pg_tests = cur.fetchone()          # tuple 2 nilai → bongkar jadi 2 variabel

    # ===== 2. Angka ACUAN dari rows (Gold) =====
    idx_tests = COLUMNS.index('tests')              # posisi kolom tests di tuple
    exp_rows = len(rows)                          # jumlah baris Gold
    exp_tests = sum(r[idx_tests] for r in rows)       # total tests Gold

    # ===== 3. Bandingkan =====
    if pg_rows != exp_rows:
        raise ValueError(
            f"Verify GAGAL {d['type']} {d['year']} Q{d['quarter']}: "
            f"baris postgres={pg_rows}, gold={exp_rows}"
        )
    if pg_tests != exp_tests:
        raise ValueError(
            f"Verify GAGAL {d['type']} {d['year']} Q{d['quarter']}: tests postgres= {pg_tests}, gold={exp_tests}"      # lengkapi sendiri
        )

    logger.info(
        "Verify OK %s %s Q%s: %d baris, tests %d",
        d["type"], d["year"], d["quarter"], pg_rows, pg_tests,
    )


def main(argv=None):
    args = parse_args(argv)
    cfg = get_db_config()

    quarter_dict = list_gold_quarters(args.n_quarter, args.run_type)
        # print(len(rows), rows[0])

    with psycopg.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"]
    ) as conn:
        for d in quarter_dict:
            rows = read_gold_quarter(d)
            load_quarter(conn, d, rows)
            verify_load(conn, d, rows)


if __name__ == "__main__":
    main()