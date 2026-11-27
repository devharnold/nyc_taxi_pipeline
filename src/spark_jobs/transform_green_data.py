import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    unix_timestamp,
)
from datetime import date

# Configuration
s3_BUCKET = os.getenv("s3_BUCKET_NAME")

SOURCE_PATH = f"s3://devharnold-nyc-taxi-data-bucket/raw/green/"
TARGET_PATH = f"s3://devharnold-nyc-taxi-data-bucket/processed/green/"

START_DATE = "2021-01-01"
END_DATE = date.today().isoformat()

def main():
    spark = (
        SparkSession.builder
        .appName("Green-Taxi-Transformation")
        .getOrCreate()
    )

    # Read raw Green Taxi data
    df = spark.read.parquet(SOURCE_PATH)

    # Standardize column names
    df = (
        df
        .withColumnRenamed(
            "lpep_pickup_datetime",
            "pickup_datetime"
        )
        .withColumnRenamed(
            "lpep_dropoff_datetime",
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

    # Filter datasets from January 2021 to date: (current time of ingestion update)
    df = df.filter(
        (col("pickup_datetime") >= START_DATE) &
        (col("pickup_datetime") < date.today().isoformat())
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
    print("Green Taxi transformation completed.")
    spark.stop()


if __name__ == "__main__":
    main()