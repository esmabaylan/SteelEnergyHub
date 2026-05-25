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
])


df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "energy_topic") \
    .option("startingOffsets", "latest") \
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


    energy_rows = [
        (r["time"], r["usage_kwh"], r["lagging_reactive_power"],
         r["leading_reactive_power"], r["co2_tco2"], r["lagging_pf"],
         r["leading_pf"], r["nsm"], r["week_status"],
         r["day_of_week"], r["load_type"])
        for r in rows if r["time"] is not None
    ]

    if energy_rows:
        execute_batch(cur, """
            INSERT INTO main_data.energy_readings
            (time, usage_kwh, lagging_reactive_power, leading_reactive_power,
             co2_tco2, lagging_pf, leading_pf, nsm, week_status, day_of_week, load_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, energy_rows)


    anomaly_rows = [
        (r["time"], r["usage_kwh"], r["z_score"], r["load_type"])
        for r in rows
        if r["time"] is not None and r["z_score"] is not None
        and abs(r["z_score"]) > Z_THRESHOLD
    ]

    if anomaly_rows:
        execute_batch(cur, """
            INSERT INTO main_data.anomalies
            (time, usage_kwh, z_score, load_type)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, anomaly_rows)
        print(f"  [Batch {batch_id}] {len(anomaly_rows)} anomalies detected!")


    cost_rows = [
        (r["time"], r["usage_kwh"], r["tariff_type"], r["cost_tl"], r["load_type"])
        for r in rows if r["time"] is not None
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

    print(f"  [Batch {batch_id}] {len(energy_rows)} rows processed | "
          f"{len(anomaly_rows)} anomalies | "
          f"{len(cost_rows)} cost records")



query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

print("Spark Streaming started — Listening to Kafka...")
query.awaitTermination()