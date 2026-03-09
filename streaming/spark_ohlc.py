import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, first, last, max, min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from config.settings import KAFKA_TOPIC, KAFKA_BROKER, DATA_PATH, CHECKPOINT_PATH

spark = SparkSession.builder.appName("Crypto OHLC Streaming").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("timestamp", LongType()),
    ]
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .load()
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

ohlc = (
    parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    .withWatermark("timestamp", "10 minutes")
    .groupBy(window(col("timestamp"), "1 minute"), col("symbol"))
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
    )
)

query = (
    ohlc.writeStream.format("parquet")
    .option("path", DATA_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

query.awaitTermination()
