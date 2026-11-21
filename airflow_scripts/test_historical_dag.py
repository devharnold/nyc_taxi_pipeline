import os
import pendulum

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

from src.fetch_data import (
    generate_dataset_names,
    get_dataset_config,
    extract_data,
)


# AWS configuration
AWS_CONN_ID = "aws_default"
S3_BUCKET = os.getenv("NYC_TAXI_S3_BUCKET")
GLUE_DATABASE = os.getenv(
    "NYC_TAXI_GLUE_DATABASE",
    "nyc_data")
GLUE_CRAWLER_ROLE = os.getenv(
    "NYC_TAXI_GLUE_CRAWLER_ROLE",
    "GlueCrawlerRole",
)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Historical backfill range
START_YEAR = 2021
END_YEAR = 2026
END_MONTH = 9

with DAG(
    dag_id="historical_taxi_ingestion",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "historical", "s3", "glue"],
) as dag:

    @task
    def get_pending_datasets():
        """
        Generate historical Yellow and Green datasets.
        Exclude files that already exist in S3.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        datasets = generate_dataset_names(
            start_year=START_YEAR,
            end_year=END_YEAR,
            end_month=END_MONTH,
        )

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        pending = []

        for dataset in datasets:
            config = get_dataset_config(dataset)

            exists = s3_hook.check_for_key(
                key=config["s3_key"],
                bucket_name=S3_BUCKET,
            )

            if not exists:
                pending.append(dataset)

        print(f"Total datasets generated: {len(datasets)}")
        print(f"Datasets pending ingestion: {len(pending)}")

        return pending

    @task
    def extract_dataset(dataset: str):
        """
        Download the original Parquet file from the TLC website.
        """
        config = get_dataset_config(dataset)
        local_file = extract_data(dataset)

        print(f"Downloaded: {local_file}")

        return {
            "filename": str(local_file),
            "dest_key": config["s3_key"],
        }

    pending_datasets = get_pending_datasets()

    extracted_files = extract_dataset.expand(
        dataset=pending_datasets
    )

    upload_tasks = LocalFilesystemToS3Operator.partial(
        task_id="upload_taxi_data_to_s3",
        dest_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=False,
    ).expand_kwargs(extracted_files)

    update_catalog = GlueCrawlerOperator(
        task_id="update_glue_catalog",
        config={
            "Name": "nyc-taxi-data-crawler",
            "Role": GLUE_CRAWLER_ROLE,
            "DatabaseName": GLUE_DATABASE,
            "Targets": {
                "S3Targets": [
                    {"Path": f"s3://{S3_BUCKET}/yellow/"},
                    {"Path": f"s3://{S3_BUCKET}/green/"},
                ]
            },
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        pool="glue_catalog_pool",
    )

    upload_tasks >> update_catalog