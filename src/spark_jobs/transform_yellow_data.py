import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    unix_timestamp,
)

# Configuration
s3_BUCKET = os.getenv("s3_BUCKET_NAME")

SOURCE_PATH = f"s3://{s3_BUCKET}/raw/yellow/"
TARGET_PATH = f"s3://{s3_BUCKET}/processed/yellow/"


def main():

    spark = (
        SparkSession.builder
        .appName("Yellow-Taxi-Transformation")
        .getOrCreate()
    )

    # Read raw Yellow Taxi data
    df = spark.read.parquet(SOURCE_PATH)

    # Standardize column names
    df = (
        df
        .withColumnRenamed(
            "tpep_pickup_datetime",
            "pickup_datetime"
        )
        .withColumnRenamed(
            "tpep_dropoff_datetime",
            "dropoff_datetime"
        )
    )

    # Remove invalid records
    df = df.filter(
        (col("pickup_datetime").isNotNull()) &
        (col("dropoff_datetime").isNotNull()) &
        (col("trip_distance") > 0) &
        (col("total_amount") > 0)
    )

    # Calculate trip duration in minutes
    df = df.withColumn(
        "trip_duration_minutes",
        (
            unix_timestamp("dropoff_datetime") -
            unix_timestamp("pickup_datetime")
        ) / 60
    )

    # Remove unreasonable trip durations
    df = df.filter(
        (col("trip_duration_minutes") > 0) &
        (col("trip_duration_minutes") <= 1440)
    )

    # Add temporal columns
    df = (
        df
        .withColumn("pickup_year", year("pickup_datetime"))
        .withColumn("pickup_month", month("pickup_datetime"))
    )

    # Write transformed data
    (
        df.write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(TARGET_PATH)
    )

    print("Yellow Taxi transformation completed.")

    spark.stop()


if __name__ == "__main__":
    main()


# TODO: This is the command used to upload the spark script to s3 for use by EMR Serverless
#aws s3 cp src/spark_jobs/yellow_taxi/transform_yellow.py \
#s3://nyc-taxi-data/scripts/transform_yellow.py
