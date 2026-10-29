import duckdb as dcb
import pyarrow.parquet as pq
from pathlib import Path

SRC = Path("data/bronze/ookla/type=mobile/*/*/*.parquet")
# pf = pq.ParquetFile(SRC)

print(dcb.sql(f"""
    select year, quarter,
        sum(tests) as id_test,
        sum(tests) filter (
            where (tile_x between 103.6 and 104.1) and (tile_y between 1.15 and 1.48)) as sgp_test,
        round(100.0 * sgp_test / id_test, 1) as pct_sgp
    from read_parquet('{SRC}', hive_partitioning=true)
    where (tile_x between 92 and 141) and (tile_y between -11 and 6)
    group by year, quarter
    """))

rg = pf.metadata.row_group(0)          # row group pertama
for i in range(rg.num_columns):
    col = rg.column(i)
    print(col.path_in_schema)
    print(col.statistics)

print(dcb.sql(f"select * from '{path}' limit 5"))