# src/jobs/fare_analysis.py
from pyspark.sql.functions import col, avg
from utils.spark_session import get_spark_session

def run_fare_analysis(parquet_directory):
    spark = get_spark_session("Fare Analysis")

    # Read all Parquet files in the directory
    df = spark.read.parquet(f"{parquet_directory}*.parquet")

    # Average fare by pickup and dropoff location
    avg_fare_by_location = df.groupBy("PULocationID", "DOLocationID") \
                             .agg(avg("fare_amount").alias("avg_fare"))

    avg_fare_by_location.show()

    # Average fare by passenger count
    avg_fare_by_passenger = df.groupBy("passenger_count") \
                              .agg(avg("fare_amount").alias("avg_fare"))

    avg_fare_by_passenger.show()

    # Correlation between fare amount and trip distance
    fare_distance_corr = df.stat.corr("fare_amount", "trip_distance")
    print(f"Correlation between fare amount and trip distance: {fare_distance_corr}")

    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: fare_analysis.py <parquet_directory>")
        sys.exit(-1)
    parquet_directory = sys.argv[1]
    run_fare_analysis(parquet_directory)
