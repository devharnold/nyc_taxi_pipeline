import os
import pendulum

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import