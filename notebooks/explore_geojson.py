import duckdb as dcb

con = dcb.connect()
con.sql("INSTALL spatial;")
con.sql("LOAD spatial;")

GEO = "/home/mervas/projects/network-quality-performance/reference/provinsi_indonesia.json"
# print(con.sql(f"""
#     /* select ST_IsValid(geom) as Validity,
#         count(*) as jumlah
#     from ST_Read('{GEO}')
#     group by ST_IsValid(geom) 
#     */
#     /*
#     select
#         KODE_PROV,
#         count(*) as jumlah
#     from ST_Read('{GEO}')
#     group by KODE_PROV
#     having count(*) > 1
#     */
#     /*
#     select
#         count(*) as total,
#         count(distinct KODE_PROV) as jumlah
#     from ST_Read('{GEO}') 
#     */
#     /*
#     SELECT ST_IsValid(ST_MakeValid(geom)) AS valid_setelah_fix, COUNT(*)
#     FROM ST_Read('{GEO}')
#     GROUP BY 1 
#     */
#     /*
#     select
#         count(distinct id) as kode_unik_id
#     from ST_Read('{GEO}')
#     */
#     """))

# print(con.sql(f"""
#     WITH prov AS (
#         SELECT PROVINSI AS provinsi, ST_MakeValid(geom) AS geom
#         FROM ST_Read('{GEO}')
#     ),
#     tiles AS (
#         SELECT tile_x, tile_y, tests, ST_Point(tile_x, tile_y) AS pt
#         FROM read_parquet('data/bronze/ookla/type=mobile/year=2026/quarter=3/*.parquet')
#         WHERE tile_x BETWEEN 95 AND 141 AND tile_y BETWEEN -11 AND 6
#     )
#     SELECT FLOOR(t.tile_x) AS lon,
#            FLOOR(t.tile_y) AS lat,
#            COUNT(*)        AS n_tiles,
#            SUM(t.tests)    AS tests
#     FROM tiles t
#     LEFT JOIN prov p ON ST_Within(t.pt, p.geom)
#     WHERE p.provinsi IS NULL          -- ⬅️ cuma ambil tile yang GAK dapet provinsi
#     GROUP BY ALL
#     ORDER BY n_tiles DESC
#     LIMIT 15
# """))

# print(con.sql(f"""
# WITH prov AS (
#     SELECT PROVINSI AS provinsi, ST_MakeValid(geom) AS geom
#     FROM ST_Read('{GEO}')
# ),
# tiles AS (
#     SELECT tile_x, tile_y, ST_Point(tile_x, tile_y) AS pt
#     FROM read_parquet('data/bronze/ookla/type=mobile/year=2026/quarter=3/*.parquet')
#     WHERE tile_x BETWEEN 110 AND 111 AND tile_y BETWEEN -8 AND -7
# ),
# yatim AS (
#     SELECT t.*
#     FROM tiles t
#     LEFT JOIN prov p ON ST_Within(t.pt, p.geom)
#     WHERE p.provinsi IS NULL
#     LIMIT 10
# )
# SELECT y.tile_x, y.tile_y,
#        (SELECT string_agg(provinsi, ', ') FROM prov WHERE ST_Within(y.pt, prov.geom)) AS cek_manual
# FROM yatim y
# """))

# print(con.sql(f"""
#     select *
#     from read_parquet('/home/mervas/projects/network-quality-performance/data/bronze/ookla/type=mobile/year=2026/quarter=3/2026-07-01_performance_mobile_tiles.parquet')
# """))