import duckdb as dcb
import logging
import argparse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


GEO = "reference/provinsi_indonesia.json"
SRC = "data/bronze/ookla/type=mobile/year=2026/quarter=3/*.parquet"
out_path = Path("data/silver/ookla_idn/year=2026/quarter=3/data.parquet")
# out_path.parent.mkdir(parents=True, exist_ok=True)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Create Silver Data")
    parser.add_argument("--n-quarter", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--type", choices=["mobile", "fixed"], default="mobile")
    return parser.parse_args(argv)
    

def build_silver(con, src, geo, out_path):
    con.sql(f"""
        COPY (
            WITH tiles AS (
                SELECT quadkey, tile_x, tile_y,
                    avg_d_kbps, avg_u_kbps, avg_lat_ms, tests, devices,
                    ST_Point(tile_x, tile_y) AS pt
                FROM read_parquet('{src}')
                WHERE tile_x BETWEEN 95 AND 141 AND tile_y BETWEEN -11 AND 6
            )
            SELECT t.quadkey, t.tile_x, t.tile_y,
                t.avg_d_kbps, t.avg_u_kbps, t.avg_lat_ms, t.tests, t.devices,
                p.prov_id, p.provinsi
            FROM tiles t
            INNER JOIN prov p ON ST_Within(t.pt, p.geom)
        ) TO '{out_path}' (FORMAT PARQUET)
    """)
    logger.info("Finish Silver Data: %s", out_path)

def bronze_folder_list(base):
    folder = {f.parent for f in base.rglob("*.parquet")}
    return [get_folder_key(f) for f in sorted(folder)]

def get_folder_key(folder):
    term = {}
    slash_strip = folder.parts
    for part in slash_strip:
        if "=" in part:
            key, value = part.split("=", 1)
            if value.isdigit():
                value = int(value)
            term[key] = value
    return term

def quarter_sort(quarter_list, n:int):
    all_quarter = {(d['year'], d['quarter']) for d in quarter_list}
    latest_quarter = sorted(all_quarter)[-n:]
    selected = []
    for d in quarter_list:
        if (d["year"], d["quarter"]) in latest_quarter:
            selected.append(d)
    selected.sort(key=lambda d: (d["year"], d["quarter"]))
    return selected

def main(argv=None):
    BASEDIR = Path("data/bronze/ookla")
    args = parse_args(argv)    
    logger.info("Default settings: %s", vars(args))
    
    con = dcb.connect()
    con.sql("LOAD spatial;")

    # 1x read map + makevalid geometry and store in memory
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE prov AS
        SELECT id AS prov_id, PROVINSI AS provinsi, ST_MakeValid(geom) AS geom
        FROM ST_Read('{GEO}')
    """)

    for d in quarter_sort(bronze_folder_list(BASEDIR), args.n_quarter):
        silver_path = Path(f"data/silver/ookla_idn/type={d['type']}/year={d['year']}/quarter={d['quarter']}")
        silver_path.mkdir(parents=True, exist_ok=True)
        out_path = f"{silver_path}/data.parquet"
        src_path = f"data/bronze/ookla/type={d['type']}/year={d['year']}/quarter={d['quarter']}/*.parquet"
        build_silver(con, src_path, GEO, out_path)
        
        print(con.sql(f"SELECT COUNT(*), SUM(tests) AS total_test FROM read_parquet('{out_path}') LIMIT 5"))

if __name__ == "__main__":
    main()
