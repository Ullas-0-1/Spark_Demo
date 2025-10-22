from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    spark = SparkSession \
        .builder \
        .appName("ProcessingTimeTriggerDemo") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession created. Reading from Kafka topic 'Demo' ")

    #Reading from Kafka
    kafka_stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "Demo") \
        .option("startingOffsets", "latest") \
        .load()

    #Schema definition
    event_schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType())
    ])


    parsed_df = kafka_stream_df \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*")

    #Aggregation: Count events by type
    counts_df = parsed_df \
        .groupBy("event_type") \
        .count()


    
    #TRIGGER (ProcessingTime)
    # We explicitly tell Spark to run a micro-batch every 5 seconds
    # It will process all data that has arrived since the last batch
    query = counts_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .queryName("ProcessingTimeTrigger") \
        .start()
    
    print("Query started with 5-second processing time trigger.")
    query.awaitTermination()

if __name__ == "__main__":
    main()