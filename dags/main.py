from airflow import DAG
import pendulum
import os
import sys
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.join(CURRENT_DIR, "api")
DWH_DIR = os.path.join(CURRENT_DIR, "datawarehouse")
DQ_DIR = os.path.join(CURRENT_DIR, "dataquality")
for path in (API_DIR, DWH_DIR, DQ_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from video_stats import (
    get_playlistId,
    get_videoIds,
    extract_video_data,
    save_to_json,
)

from dwh import staging_table, core_table
from soda import yt_elt_data_quality


# Define local timezone
local_tz = pendulum.timezone("America/Chicago")

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,   
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(hours=1),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(hours=1),
    'start_date': datetime(2026, 2, 16, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"

## DAG 1: produce_json
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    schedule_interval='0 14 * * *',  # Run daily at 14:00 (2 PM)
    catchup=False
) as dag_produce:

    # Define the tasks
    playlist_id = get_playlistId()
    video_ids = get_videoIds(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db',
    )

    # Define dependencies / task order
    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> trigger_update_db

## DAG 2: update_db
with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='A DAG to process JSON file and insert data into both staging and core schemas',
    schedule=None,
    catchup=False
) as dag_update:

    # Define the tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality',
    )

    # Define dependencies / task order
    update_staging >> update_core >> trigger_data_quality

# DAG 3: data_quality
with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality ion both layers in the db',
    schedule=None,
    catchup=False
) as dag_data_quality:

    # Define the tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies / task order
    soda_validate_staging >> soda_validate_core
