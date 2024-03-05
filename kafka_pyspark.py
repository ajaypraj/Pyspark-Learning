from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math
import string
import random

KAFKA_INPUT_TOPIC_NAME_CONS = "testTopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputmallstream"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started …")

    spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka") \
    .master("local[*]") \
    .getOrCreate()

    # Construct a streaming DataFrame that reads from testtopic
    sampleDataframe = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
    .option("startingOffsets", "latest") \
    .load()
    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()
