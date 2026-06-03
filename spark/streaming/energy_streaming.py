from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, stddev, when, lit, current_timestamp
)
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import execute_batch

# --- Ayarlar ---
DB_CONFIG = {
    "host": "timescaledb",
    "port": 5432,
    "database": "energydb",
    "user": "data_writer",
    "password": "sifre123",
    "options": "-c search_path=main_data"
}

# Türkiye elektrik tarifeleri (TL/kWh)
NIGHT_TARIFF = 1.80   # 22:00 - 06:00
DAY_TARIFF   = 3.20   # 06:00 - 22:00

# Anomali eşiği (veri keşfinden)
USAGE_MEAN = 35.2
USAGE_STD  = 27.8
Z_THRESHOLD = 3.0

spark = SparkSession.builder \
    .appName("SteelEnergyStreaming") \
    .config("spark.driver.extraClassPath", "/usr/local/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/usr/local/spark/jars/*") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.session.timeZone", "Europe/Istanbul") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("time",                   StringType()),
    StructField("usage_kwh",              DoubleType()),
    StructField("lagging_reactive_power", DoubleType()),
    StructField("leading_reactive_power", DoubleType()),
    StructField("co2_tco2",               DoubleType()),
    StructField("lagging_pf",             DoubleType()),
    StructField("leading_pf",             DoubleType()),
    StructField("nsm",                    IntegerType()),
    StructField("week_status",            StringType()),
    StructField("day_of_week",            StringType()),
    StructField("load_type",              StringType()),
    # Yeni özellikler
    StructField("hour",                   IntegerType()),
    StructField("day",                    IntegerType()),
    StructField("month",                  IntegerType()),
    StructField("quarter",                IntegerType()),
    StructField("is_weekend",             BooleanType()),
    StructField("shift",                  StringType()),
    StructField("tariff_period",          StringType()),
    StructField("day_period",             StringType()),
    StructField("usage_ma_1h",            DoubleType()),
    StructField("usage_ma_4h",            DoubleType()),
    StructField("usage_diff",             DoubleType()),
    StructField("usage_pct_change",       DoubleType()),
    StructField("pf_efficiency",          DoubleType()),
    StructField("reactive_power_balance", DoubleType()),
    StructField("low_pf_flag",            BooleanType()),
    StructField("z_score",                DoubleType()),
    StructField("z_anomaly",              BooleanType()),
    StructField("cost_tl",                DoubleType()),
    StructField("cost_saving",            DoubleType()),
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "energy_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()


df = df_raw \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("time", col("time").cast("timestamp")) \
    .filter(col("time").isNotNull())


df = df.withColumn(
    "z_score",
    (col("usage_kwh") - lit(USAGE_MEAN)) / lit(USAGE_STD)
)

df = df.withColumn("hour", col("time").cast("int") % 86400 / 3600)

df = df.withColumn(
    "tariff_type",
    when((col("hour") >= 22) | (col("hour") < 6), "night").otherwise("day")
)

df = df.withColumn(
    "cost_tl",
    when(col("tariff_type") == "night",
         col("usage_kwh") * lit(NIGHT_TARIFF)
    ).otherwise(col("usage_kwh") * lit(DAY_TARIFF))
)


def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    rows = batch_df.collect()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Anomalileri yaz
    anomaly_rows = [
        (r["time"], r["usage_kwh"], r["z_score"], r["load_type"])
        for r in rows
        if r["time"] is not None
        and r["z_anomaly"] is True
    ]

    if anomaly_rows:
        execute_batch(cur, """
            INSERT INTO main_data.anomalies
            (time, usage_kwh, z_score, load_type)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, anomaly_rows)

    # Maliyet analizini yaz — tariff_type Spark'ta hesaplanıyor
    cost_rows = [
        (r["time"], r["usage_kwh"], r["tariff_type"], r["cost_tl"], r["load_type"])
        for r in rows
        if r["time"] is not None and r["cost_tl"] is not None
    ]

    if cost_rows:
        execute_batch(cur, """
            INSERT INTO main_data.cost_analysis
            (time, usage_kwh, tariff_type, cost_tl, load_type)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, cost_rows)

    conn.commit()
    cur.close()
    conn.close()

    print(f"  [Batch {batch_id}] {len(rows)} satır | "
          f"{len(anomaly_rows)} anomali | "
          f"{len(cost_rows)} maliyet")
    

query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/home/jovyan/work/spark_checkpoint") \
    .start()

print("Spark Streaming started — Listening to Kafka...")
query.awaitTermination()