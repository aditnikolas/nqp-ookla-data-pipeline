import argparse
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path("data/silver/ookla_idn")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

def spark_initiation():
    spark = (SparkSession.builder
         .appName("nqp-gold")
         .master("local[*]")       # jalan di VM sendiri, pakai semua core CPU
         .getOrCreate())
    return spark
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Create Gold Data")
    parser.add_argument("--n-quarter", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-type", choices=["mobile", "fixed"], default="mobile")
    return parser.parse_args(argv)

def lookup_folder(n, type='mobile'):
    folder = {f.parent for f in BASE_DIR.rglob("*.parquet")}
    folder_list = []
    for f in folder:
        folder_list.append(get_folder_key(f))

    selected_list = year_quarter_sort(folder_list, n)
    return selected_list

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

def year_quarter_sort(quarter_list, n:int):
    all_quarter = {(d['year'], d['quarter']) for d in quarter_list}
    latest_quarter = sorted(all_quarter)[-n:]
    selected = []
    for d in quarter_list:
        if (d["year"], d["quarter"]) in latest_quarter:
            selected.append(d)
    selected.sort(key=lambda d: (d["year"], d["quarter"]))
    return selected

def validate_gold(clean_data, silver_data, d):
    logger.info("Validating Year: %s, Quartal: %s", d['year'], d['quarter'])
    silver_prov = silver_data.select(F.col("provinsi")).distinct().count()
    silver_tile = silver_data.count()
    silver_test = silver_data.agg(F.sum(F.col("tests"))).first()[0]
    
    gold_prov = clean_data.count()
    gold_tile = clean_data.select(F.sum(F.col("n_tiles"))).first()[0]
    gold_test = clean_data.agg(F.sum(F.col("tests")).alias("gold_test")).first()["gold_test"]

    null_checker = clean_data.where(
        (F.col("provinsi").isNull()) | (F.col("prov_id").isNull()) | (F.col("year").isNull()) | (F.col("quarter").isNull())
    )
    du_null_checker = clean_data.filter(
        (F.col("dl_mbps").isNull()) | (F.col("dl_mbps") <= 0) |
        (F.col("ul_mbps").isNull()) | (F.col("ul_mbps") <= 0) |
        (F.col("latency_ms").isNull()) | (F.col("latency_ms") <= 0)
    )

    soft_checker = clean_data.filter(
        (F.col("dl_mbps") > 1000) | (F.col("latency_ms") > 500)
    )
    
    # Hard Check
    if null_checker.count() > 0 :
        null_checker.show()
        raise ValueError(f"Total NULL data on year {d['year']} quartal {d['quarter']}")
    if du_null_checker.count() > 0 :
        du_null_checker.show()
        raise ValueError(f"Total NULL data on year {d['year']} quartal {d['quarter']}")        

    if silver_prov != gold_prov:
        raise ValueError(f"Total of Province didn't match on quartal {d['quarter']} clean data: {gold_prov}, silver data: {silver_prov}")
    elif silver_tile != gold_tile:
        raise ValueError(f"Total of tile didn't match on quartal {d['quarter']} clean data: {gold_tile}, silver data: {silver_tile}")
    elif silver_test != gold_test:
        raise ValueError(f"Total of test didn't match on quartal {d['quarter']} clean data: {gold_test}, silver data: {silver_test}")
    else:
        logger.info("All data from silver inline with gold data with detail province: %s, tile: %s, test: %s", gold_prov, gold_tile, gold_test)
        logger.info("Silver data for Year: %s, Quartal: %s are VALIDATED! ready to move Gold Layer", d['year'], d['quarter'])

    # Soft Check
    if soft_checker.count() > 0 :
        logger.warning("Anomaly data with value DL > 1000 Mbps or Latency > 500 ms, total province affected:")
        soft_checker.show()

    return True

def export_to_gold(clean_data, gold_dir):
    clean_data.drop("type","year","quarter").coalesce(1).write.mode("overwrite").parquet(str(gold_dir))
    return True

def main(argv=None):
    spark = spark_initiation()
    args = parse_args(argv)
    partition_list = lookup_folder(args.n_quarter, args.run_type)

    df = spark.read.parquet(str(BASE_DIR))
    # df.printSchema()
    for d in partition_list:
        # df_filtered = df.where(f"year={d['year']}, quarter={d['quarter']}")
        df_filtered = df.where((F.col("year")==f"{d['year']}") & (F.col("quarter")==f"{d['quarter']}"))
        clean_data = df_filtered.groupBy("provinsi", "year", "quarter", "prov_id", "type").agg(
            F.count("*").alias("n_tiles"),
            F.sum(F.col("tests")).alias("tests"),
            F.round((F.sum(F.col("avg_d_kbps") * F.col("tests")) / F.sum(F.col("tests")) / 1000),2).alias("dl_mbps"),
            F.round((F.sum(F.col("avg_u_kbps") * F.col("tests")) / F.sum(F.col("tests")) / 1000),2).alias("ul_mbps"),
            F.round((F.sum(F.col("avg_lat_ms") * F.col("tests")) / F.sum(F.when(F.col("avg_lat_ms").isNotNull(), F.col("tests")))),1).alias("latency_ms")
        )

        if validate_gold(clean_data, df_filtered, d):
            gold_dir = Path(f"data/gold/network-performance-province/type={d['type']}/year={d['year']}/quarter={d['quarter']}")
            gold_dir.mkdir(parents=True, exist_ok=True)

            if export_to_gold(clean_data, gold_dir):
                logger.info("Done create gold layer in : %s", gold_dir)
                gold_show = spark.read.parquet(str(gold_dir))
                logger.info("Jumlah Provinsi ada : %s", gold_show.count())


        # max_min = clean_data.groupBy("provinsi").agg(
        #     F.max(F.col("dl_mbps")).alias("max_mbps")
        # ).orderBy(F.desc("max_mbps"))
        # print(max_min.show())
        # sqltest = df_filtered.select(F.col("provinsi")).distinct().count()
        # print(sqltest)

    spark.stop()


if __name__ == "__main__":
    main()