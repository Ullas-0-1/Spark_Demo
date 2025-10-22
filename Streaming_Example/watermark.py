import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr

#checkpoint directory
checkpoint_dir = os.path.expanduser("~/spark-checkpoints/demo_4_watermark")

def main():
    spark = SparkSession \
        .builder \
        .appName("WatermarkJoinDemo") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark Session created.")
    
    #Stream 1:Ad Impressions
    #We'll send data to port 9998 
    #example-2025-10-22T18:30:00Z,ad_123
    impressions_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9998) \
        .load()

    impressions = impressions_df \
        .select(split(col("value"), ",").alias("data")) \
        .select(
            col("data")[0].cast("timestamp").alias("impression_time"),
            col("data")[1].alias("impression_ad_id")
        ) \
        .withWatermark("impression_time", "10 seconds")
        # with watermark of 10 seconds for impressions tells Spark that it can handle data that is up to 10 seconds late.
    

    # Stream 2:Ad Clicks
    # We will send data to port 9999 
    #example -- 2025-10-22T18:30:05Z,ad_123
    clicks_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    clicks = clicks_df \
        .select(split(col("value"), ",").alias("data")) \
        .select(
            col("data")[0].cast("timestamp").alias("click_time"),
            col("data")[1].alias("click_ad_id")
        ) \
        .withWatermark("click_time", "20 seconds")
        
    print("Socket streams initialized. Waiting for data on ports 9998 and 9999.")

    
    # We join impressions and clicks.
    # A click must happen within 30 seconds of an impression to be valid
    joined_df = impressions.join(
        clicks,
        expr("""
            click_ad_id = impression_ad_id AND
            click_time >= impression_time AND
            click_time <= impression_time + interval 30 seconds
        """),
        "inner" 
    )

    
    query = joined_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_dir) \
        .queryName("WatermarkJoin") \
        .start()

    print(f"Join query started. Using checkpoint location: {checkpoint_dir}")
    query.awaitTermination()

if __name__ == "__main__":
    main()