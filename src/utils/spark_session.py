# src/utils/spark_session.py
from pyspark.sql import SparkSession

def get_spark_session(app_name="NYC Taxi Analysis"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark
