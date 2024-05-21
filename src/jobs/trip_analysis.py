# src/jobs/trip_analysis.py
from pyspark.sql.functions import col, hour, dayofweek, month, year, unix_timestamp, avg
from utils.spark_session import get_spark_session

def run_trip_analysis(parquet_directory):
    spark = get_spark_session("Trip Analysis")

    # Read all Parquet files in the directory
    df = spark.read.parquet(f"{parquet_directory}*.parquet")

    # Calculate trip duration in minutes
    df = df.withColumn("trip_duration", 
                       (unix_timestamp(col("tpep_dropoff_datetime").cast("timestamp")) - unix_timestamp(col("tpep_pickup_datetime").cast("timestamp"))) / 60)

    # Extract time features
    df = df.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
           .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
           .withColumn("month", month(col("tpep_pickup_datetime"))) \
           .withColumn("year", year(col("tpep_pickup_datetime")))

    # Group by time features and calculate average duration and distance
    avg_duration_distance = df.groupBy("hour", "day_of_week", "month", "year") \
                              .agg(avg("trip_duration").alias("avg_duration"),
                                   avg("trip_distance").alias("avg_distance"))

    # Show results
    avg_duration_distance.show()

    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: trip_analysis.py <parquet_directory>")
        sys.exit(-1)
    parquet_directory = sys.argv[1]
    run_trip_analysis(parquet_directory)
