# src/jobs/traffic_analysis.py
from pyspark.sql.functions import col, avg
from utils.spark_session import get_spark_session

def run_traffic_analysis(parquet_directory):
    spark = get_spark_session("Traffic Analysis")

    # Read all Parquet files in the directory
    df = spark.read.parquet(f"{parquet_directory}*.parquet")

    # Calculate average speed (distance divided by duration in hours)
    df = df.withColumn("avg_speed", col("trip_distance") / (col("trip_duration") / 60))

    # Average speed by time of day, day of the week, and month
    avg_speed_by_time = df.groupBy("hour", "day_of_week", "month", "year") \
                          .agg(avg("avg_speed").alias("avg_speed"))

    avg_speed_by_time.show()

    # Infer traffic conditions by analyzing average speed
    traffic_inference = df.groupBy("PULocationID", "DOLocationID", "hour", "day_of_week") \
                          .agg(avg("avg_speed").alias("avg_speed"))

    traffic_inference.show()

    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: traffic_analysis.py <parquet_directory>")
        sys.exit(-1)
    parquet_directory = sys.argv[1]
    run_traffic_analysis(parquet_directory)
