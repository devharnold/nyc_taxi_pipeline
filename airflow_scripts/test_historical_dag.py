import os
import pendulum

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFileSystemToS3Operator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

from src.fetch_data import (
    generate_dataset_names,
    get_dataset_config,
    extract_data,
)


# AWS configuration
AWS_CONN_ID = "aws_default"

S3_RAW_BUCKET = os.getenv("NYC_TAXI_BUCKET")
DEST_BUCKET = os.getenv("DEST_BUCKET")
S3_TARGET_PATH = os.getenv("S3_TARGET_PATH")

GLUE_DATABASE = os.getenv("NYC_TAXI_GLUE_DATABASE")
GLUE_CRAWLER_ROLE = os.getenv("NYC_TAXI_GLUE_CRAWLER_ROLE")

EMR_EXECUTION_ROLE_ARN = os.getenv("EMR_EXECUTION_ROLE_ARN")
EMR_LOGS_BUCKET = os.getenv("EMR_LOGS")
SPARK_SCRIPT_URI = os.getenv("SPARK_SCRIPT_URI")

EMR_RELEASE_LABEL = os.getenv("EMR_RELEASE_LABEL", "emr-6.7.0")
EMR_APPLICATION_NAME = os.getenv(
    "EMR_APPLICATION_NAME",
    "nyc-taxi-spark-application",
)

# Historical backfill range
START_YEAR = 2021
END_YEAR = 2026
END_MONTH = 9


# Configuration validation
required_configs = {
    "NYC_TAXI_BUCKET": S3_RAW_BUCKET,
    "DEST_BUCKET": DEST_BUCKET,
    "S3_TARGET_PATH": S3_TARGET_PATH,
    "NYC_TAXI_GLUE_DATABASE": GLUE_DATABASE,
    "NYC_TAXI_GLUE_CRAWLER_ROLE": GLUE_CRAWLER_ROLE,
    "EMR_EXECUTION_ROLE_ARN": EMR_EXECUTION_ROLE_ARN,
    "SPARK_SCRIPT_URI": SPARK_SCRIPT_URI,
    "EMR_LOGS": EMR_LOGS_BUCKET,
}

for config_name, config_value in required_configs.items():
    if not config_value:
        raise ValueError(f"{config_name} is not configured")


# S3 and EMR configuration
if not S3_TARGET_PATH.startswith("s3://"):
    raise ValueError("S3_TARGET_PATH must be a valid S3 URI.")

bucket_and_prefix = S3_TARGET_PATH[5:].strip("/")
target_bucket = bucket_and_prefix.split("/", 1)[0]

if target_bucket != S3_RAW_BUCKET:
    raise ValueError(
        f"S3_TARGET_PATH bucket '{target_bucket}' does not match "
        f"NYC_TAXI_BUCKET '{S3_RAW_BUCKET}'."
    )

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": f"s3://{EMR_LOGS_BUCKET.rstrip('/')}/logs/"
        }
    }
}


def get_s3_key(dataset: str) -> str:
    """Build the S3 key for a raw historical dataset."""
    bucket_and_prefix = S3_TARGET_PATH[5:].strip("/")
    _, separator, prefix = bucket_and_prefix.partition("/")

    dataset_key = get_dataset_config(dataset)["s3_key"]

    if separator and prefix:
        return f"{prefix}/{dataset_key}"

    return dataset_key


def get_raw_s3_uri(key: str) -> str:
    """Return the raw dataset S3 URI."""
    return f"s3://{S3_RAW_BUCKET}/{key}"


def get_processed_s3_uri(dataset: str) -> str:
    """Build a separate processed output directory per dataset."""
    config = get_dataset_config(dataset)

    return (
        f"s3://{DEST_BUCKET}/"
        f"processed/{config['taxi_type']}/"
        f"year={config['year']}/"
        f"month={config['month']:02d}/"
    )

# DAG definition
with DAG(
    dag_id="Historical_taxi_ingestion",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc_taxi", "historical", "s3", "glue", "emr", "spark"],
) as dag:

    @task
    def get_pending_datasets():
        # Find datasets that do not already exist in the raw S3 bucket
        datasets = generate_dataset_names(
            start_year=START_YEAR,
            end_year=END_YEAR,
            end_month=END_MONTH,
        )

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        pending = []

        for dataset in datasets:
            s3_key = get_s3_key(dataset)

            exists = s3_hook.check_for_key(
                key=s3_key,
                bucket_name=S3_RAW_BUCKET,
            )
            if not exists:
                pending.append(dataset)

        print(f"Total datasets generated: {len(datasets)}")
        print(f"Datasets pending ingestion: {len(pending)}")

        return pending

    @task
    def extract_dataset(dataset: str):
        # Download one dataset and return its upload configuration
        local_file = extract_data(dataset)

        print(f"Downloaded dataset: {dataset}")
        print(f"Local file: {local_file}")

        return {
            "filename": str(local_file),
            "dest_key": get_s3_key(dataset),
        }

    # Create EMR Serverless application
    create_spark_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_application",
        job_type="SPARK",
        release_label=EMR_RELEASE_LABEL,
        config={
            "name": EMR_APPLICATION_NAME,
        },
        aws_conn_id=AWS_CONN_ID,
    )

    # XCom output containing the dynamically created EMR application ID.
    application_id = create_spark_app.output

    # Build independent Spark job configurations
    @task
    def build_spark_configs(datasets: list, emr_application_id: str) -> list:
        # Build one EMR Serverless job configuration per dataset
        configurations = []

        for dataset in datasets:
            input_uri = get_raw_s3_uri(get_s3_key(dataset))
            output_uri = get_processed_s3_uri(dataset)

            configurations.append(
                {
                    "application_id": emr_application_id,
                    "execution_role_arn": EMR_EXECUTION_ROLE_ARN,
                    "job_driver": {
                        "sparkSubmit": {
                            "entryPoint": SPARK_SCRIPT_URI,
                            "entryPointArguments": [
                                "--dataset",
                                dataset,
                                "--input",
                                input_uri,
                                "--output",
                                output_uri,
                            ],
                            "sparkSubmitParameters": (
                                "--conf spark.executor.cores=1 "
                                "--conf spark.executor.memory=4g "
                                "--conf spark.driver.cores=1 "
                                "--conf spark.driver.memory=4g "
                                "--conf spark.executor.instances=1"
                            ),
                        }
                    },
                    "configuration_overrides": DEFAULT_MONITORING_CONFIG,
                    "aws_conn_id": AWS_CONN_ID,
                    "name": f"nyc-taxi-{dataset}",
                }
            )

        return configurations

    # Task dependencies and dynamic task mapping
    pending_datasets = get_pending_datasets()
    
    extracted_files = extract_dataset.expand(
        dataset=pending_datasets,
    )
    
    upload_tasks = LocalFileSystemToS3Operator.partial(
        task_id="upload_taxi_data_to_s3",
        dest_bucket=S3_RAW_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        replace=False,
    ).expand_kwargs(extracted_files)
    
    spark_configs = build_spark_configs(
        pending_datasets,
        application_id,
    )
    
    spark_tasks = EmrServerlessStartJobOperator.partial(
        task_id="spark_transform_historical_data",
    ).expand_kwargs(spark_configs)

    # Glue Data Catalog
    update_catalog = GlueCrawlerOperator(
        task_id="update_glue_catalog",
        config={
            "Name": "nyc-taxi-data-crawler",
            "Role": GLUE_CRAWLER_ROLE,
            "DatabaseName": GLUE_DATABASE,
            "Targets": {
                "S3Targets": [
                    {"Path": f"s3://{S3_RAW_BUCKET}/yellow/raw/"},
                    {"Path": f"s3://{S3_RAW_BUCKET}/green/raw/"},
                ]
            },
        },
        aws_conn_id=AWS_CONN_ID,
        pool="glue_catalog_pool",
    )

    # Delete EMR Serverless application
    delete_spark_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_spark_application",
        application_id=application_id,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )

    # Extraction and uploads happen before Spark processing.
    pending_datasets >> extracted_files
    extracted_files >> upload_tasks

    # Create the application after uploads, then submit the mapped jobs.
    upload_tasks >> create_spark_app
    create_spark_app >> spark_configs
    upload_tasks >> spark_configs
    spark_configs >> spark_tasks

    # Delete the application after every mapped Spark job has finished.
    spark_tasks >> delete_spark_app

    # Catalog refresh requires successful Spark jobs and successful cleanup.
    spark_tasks >> update_catalog
    delete_spark_app >> update_catalog
