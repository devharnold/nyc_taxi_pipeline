import os
import pendulum
from datetime import timedelta

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFileSystemToS3Operator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessStopApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
)

from src.fetch_data import (
    generate_dataset_names,
    get_dataset_config,
    extract_data,
)


AWS_CONN_ID = "aws_default"

S3_RAW_BUCKET = os.getenv("NYC_TAXI_BUCKET")
DEST_BUCKET = os.getenv("DEST_BUCKET")
S3_TARGET_PATH = os.getenv("S3_TARGET_PATH")

GLUE_DATABASE = os.getenv("NYC_TAXI_GLUE_DATABASE")
GLUE_CRAWLER_ROLE = os.getenv("NYC_TAXI_GLUE_CRAWLER_ROLE")

EMR_EXECUTION_ROLE_ARN = os.getenv("EMR_EXECUTION_ROLE_ARN")
EMR_LOGS_BUCKET = os.getenv("EMR_LOGS")

S3_BUCKET = os.getenv("S3_BUCKET")

EMR_RELEASE_LABEL = ("emr-6.7.0")
EMR_APPLICATION_NAME = os.getenv("EMR_APPLICATION_NAME")

START_YEAR = 2021
END_YEAR = 2026
END_MONTH = 9

SPARK_SCRIPTS = {
    "yellow": "transform_yellow_data.py",
    "green": "transform_green_data.py",
}

REQUIRED_CONFIG = {
    "NYC_TAXI_BUCKET": S3_RAW_BUCKET,
    "DEST_BUCKET": DEST_BUCKET,
    "S3_TARGET_PATH": S3_TARGET_PATH,
    "NYC_TAXI_GLUE_DATABASE": GLUE_DATABASE,
    "NYC_TAXI_GLUE_CRAWLER_ROLE": GLUE_CRAWLER_ROLE,
    "EMR_EXECUTION_ROLE_ARN": EMR_EXECUTION_ROLE_ARN,
    "EMR_LOGS": EMR_LOGS_BUCKET,
    "S3_BUCKET": S3_BUCKET,
}

# Default Emr Config
DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": (
                f"s3://{EMR_LOGS_BUCKET.strip('/')}/logs/"
            )
        }
    }
}

missing_config = [
    key for key, value in REQUIRED_CONFIG.items() if not value
]

if missing_config:
    raise ValueError(
        f"Missing required environment variables: {', '.join(missing_config)}"
    )

#s3 Helpers
def get_raw_s3_key(dataset: str) -> str:
    config = get_dataset_config(dataset)
    return f"raw/{config['s3_key'].lstrip('/')}"

def get_raw_s3_uri(dataset: str) -> str:
    return f"s3://{S3_RAW_BUCKET}/{get_raw_s3_key(dataset)}"

def get_processed_s3_uri(dataset: str) -> str:
    config = get_dataset_config(dataset)

    return (
        f"s3://{DEST_BUCKET}/processed/"
        f"{config['taxi_type']}/"
        f"year={config['year']}/"
        f"month={config['month']:02d}/"
    )

def get_spark_entry_point(dataset: str) -> str:
    taxi_type = get_dataset_config(dataset)["taxi_type"]
    script = SPARK_SCRIPTS.get(taxi_type)

    if not script:
        raise ValueError(f"No Spark script configured for {taxi_type}")

    return f"s3://{S3_BUCKET}/spark_scripts/{script}"


with DAG(
    dag_id="historical_taxi_trip_ingestion",
    start_date=pendulum.datetime(2021, 1, 1, tz="Africa/Nairobi"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "taxi_trip_data",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["nyc-taxi", "historical", "spark"],
) as dag:
    @task
    def get_pending_datasets():
        datasets = generate_dataset_names(
            start_year=START_YEAR,
            end_year=END_YEAR,
            end_month=END_MONTH,
        )
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        return [
            dataset
            for dataset in datasets
            if not s3_hook.check_for_key(
                key=get_raw_s3_key(dataset),
                bucket_name=S3_RAW_BUCKET,
            )
        ]

    @task
    def extract_datasets(datasets):
        extracted_files = []

        for dataset in datasets:
            filename = extract_data(dataset)

            extracted_files.append({
                "filename": filename,
                "dest_key": get_raw_s3_key(dataset),
            })

        return extracted_files

    @task
    def build_spark_configs(datasets, application_id):
        configs = []
        for dataset in datasets:
            input_uri = get_raw_s3_uri(dataset)
            output_uri = get_processed_s3_uri(dataset)

            configs.append({
                "application_id": application_id,
                "execution_role_arn": EMR_EXECUTION_ROLE_ARN,
                "name": f"nyc-taxi-{dataset}",
                "job_driver": {
                    "sparkSubmit": {
                        "entryPoint": get_spark_entry_point(dataset),
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
                            "--conf spark.executor.memory=2g "
                            "--conf spark.driver.cores=1 "
                            "--conf spark.driver.memory=2g"
                        ),
                    }
                },
                "configuration_overrides": (DEFAULT_MONITORING_CONFIG),
                "wait_for_completion": True,
            })

        return configs

    pending_datasets = get_pending_datasets()
    extracted_files = extract_datasets(pending_datasets)
    upload_tasks = LocalFileSystemToS3Operator.partial(
        task_id="upload_raw_files",
        aws_conn_id=AWS_CONN_ID,
        dest_bucket= S3_RAW_BUCKET,
    ).expand_kwargs(extracted_files)

    create_spark_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_application",
        aws_conn_id=AWS_CONN_ID,
        job_type="SPARK",
        release_label=EMR_RELEASE_LABEL,
        config={
            "name": EMR_APPLICATION_NAME,
        },
    )

    application_id = create_spark_app.output

    spark_configs = build_spark_configs(
        pending_datasets,
        application_id,
    )

    spark_tasks = EmrServerlessStartJobOperator.partial(
        task_id="run_spark_jobs",
        aws_conn_id=AWS_CONN_ID,
    ).expand_kwargs(spark_configs)

    stop_spark_app = EmrServerlessStopApplicationOperator(
        task_id="stop_spark_application",
        aws_conn_id=AWS_CONN_ID,
        application_id=application_id,
        trigger_rule="all_done",
    )

    delete_spark_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_spark_application",
        aws_conn_id=AWS_CONN_ID,
        application_id=application_id,
        trigger_rule="all_done",
    )

    update_catalog = GlueCrawlerOperator(
        task_id="update_glue_catalog",
        aws_conn_id=AWS_CONN_ID,
        config={
            "Name": "nyc-taxi-historical-crawler",
            "Role": GLUE_CRAWLER_ROLE,
            "DatabaseName": GLUE_DATABASE,
            "Targets": {
                "S3Targets": [
                    {
                        "Path": (
                            f"s3://{DEST_BUCKET}/"
                            f"{S3_TARGET_PATH.strip('/')}/yellow/"
                        )
                    },
                    {
                        "Path": (
                            f"s3://{DEST_BUCKET}/"
                            f"{S3_TARGET_PATH.strip('/')}/green/"
                        )
                    },
                ]
            },
        },
    )

    pending_datasets >> extracted_files
    extracted_files >> upload_tasks
    upload_tasks >> create_spark_app
    create_spark_app >> spark_configs
    upload_tasks >> spark_configs
    spark_configs >> spark_tasks
    spark_tasks >> stop_spark_app
    stop_spark_app >> delete_spark_app
    spark_tasks >> update_catalog
    delete_spark_app >> update_catalog