# src/jobs/demand_prediction.py
from pyspark.sql.functions import col, date_format
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from utils.spark_session import get_spark_session

def run_demand_prediction(parquet_directory):
    spark = get_spark_session("Demand Prediction")

    # Read all Parquet files in the directory
    df = spark.read.parquet(f"{parquet_directory}*.parquet")

    # Extract additional time features
    df = df.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
           .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
           .withColumn("day_of_month", date_format(col("tpep_pickup_datetime"), "d").cast("int")) \
           .withColumn("month", month(col("tpep_pickup_datetime"))) \
           .withColumn("year", year(col("tpep_pickup_datetime")))

    # Group by time features and count number of pickups
    feature_df = df.groupBy("hour", "day_of_week", "day_of_month", "month", "year") \
                   .count() \
                   .withColumnRenamed("count", "num_pickups")

    # Define feature columns
    feature_cols = ["hour", "day_of_week", "day_of_month", "month", "year"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Define regression model
    lr = LinearRegression(featuresCol="features", labelCol="num_pickups")

    # Create a pipeline
    pipeline = Pipeline(stages=[assembler, lr])

    # Split the data into training and testing sets
    train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    model = pipeline.fit(train_df)

    # Make predictions
    predictions = model.transform(test_df)

    # Show predictions
    predictions.select("features", "num_pickups", "prediction").show()

    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: demand_prediction.py <parquet_directory>")
        sys.exit(-1)
    parquet_directory = sys.argv[1]
    run_demand_prediction(parquet_directory)
