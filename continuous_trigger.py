import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


checkpoint_dir = os.path.expanduser("~/spark-checkpoints/demo_3_continuous")

def main():
    spark = SparkSession \
        .builder \
        .appName("ContinuousTriggerDemo") \
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

    # Schema definition
    event_schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType())
    ])

    parsed_df = kafka_stream_df \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*")


    # #############
    # Continuous processing cannot do aggregations (like groupBy).
    # It can only do simple row-by-row operations like 'select' and 'where', because of its low-latency design.
    #Here we are filtering for 'purchase' events.
    filtered_df = parsed_df \
        .where("event_type = 'purchase'")


    # Continuous trigger
    # This trigger processes data as it arrives.
    # The '1 second' is the checkpoint interval, not a batch interval.
    #
    # We must also change the outputMode to "append" ,because we are no longer aggregating.
    query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(continuous="1 second") \
        .option("checkpointLocation", checkpoint_dir) \
        .queryName("ContinuousTrigger") \
        .start()
    
    print(f"Query started with 1-second continuous trigger(checkpointing).")
    print(f"Using checkpoint location: {checkpoint_dir}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
