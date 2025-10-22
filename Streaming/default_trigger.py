import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#checkpoint directory
checkpoint_dir = os.path.expanduser("~/spark-checkpoints/demo_1_default")

def main():
    spark = SparkSession \
        .builder \
        .appName("DefaultTriggerDemo") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession created. Reading from Kafka topic 'Demo'")

    #Reading from Kafka
    kafka_stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "Demo") \
        .option("startingOffsets", "latest") \
        .load()

    #  Schema definition
    event_schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType())
    ])

    parsed_df = kafka_stream_df \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("event_time")))

    #Aggregation
    counts_df = parsed_df \
        .groupBy("event_type") \
        .count()

    # The default trigger processes one micro-batch right after the previous one finishes.
    # No need to specify trigger explicitly.

  
    query = counts_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("DefaultTriggerComplete") \
        .start()
    query.awaitTermination()

if __name__ == "__main__":
    main()


