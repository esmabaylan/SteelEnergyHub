from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("StreamingJobTest").getOrCreate()

df=spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers","kafka:9092")\
    .option("subscribe","energydata")\
    .option("startingOffsets","latest")\
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")




