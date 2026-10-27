import duckdb as dcb
import pyarrow.parquet as pq
from pathlib import Path

path = Path("data/bronze/ookla/type=mobile/year=2026/quarter=3/2026-07-01_performance_mobile_tiles.parquet")
pf = pq.ParquetFile(path)

print(pf.metadata)
print(pf.schema_arrow)

rg = pf.metadata.row_group(0)          # row group pertama
for i in range(rg.num_columns):
    col = rg.column(i)
    print(col.path_in_schema)
    print(col.statistics)

print(dcb.sql(f"select * from '{path}' limit 5"))