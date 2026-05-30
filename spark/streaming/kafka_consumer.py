from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

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
    .option("subscribe", "energy_raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_raw \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("time", col("time").cast("timestamp"))

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()

query.awaitTermination()