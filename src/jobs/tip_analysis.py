# src/jobs/tip_analysis.py
from pyspark.sql.functions import col, avg
from utils.spark_session import get_spark_session

def run_tip_analysis(parquet_directory):
    spark = get_spark_session("Tip Analysis")

    # Read all Parquet files in the directory
    df = spark.read.parquet(f"{parquet_directory}*.parquet")

    # Calculate tip percentage
    df = df.withColumn("tip_percentage", (col("tip_amount") / col("total_amount")) * 100)

    # Average tip percentage by time of day, day of the week, and month
    avg_tip_by_time = df.groupBy("hour", "day_of_week", "month", "year") \
                        .agg(avg("tip_percentage").alias("avg_tip_percentage"))

    avg_tip_by_time.show()

    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: tip_analysis.py <parquet_directory>")
        sys.exit(-1)
    parquet_directory = sys.argv[1]
    run_tip_analysis(parquet_directory)
