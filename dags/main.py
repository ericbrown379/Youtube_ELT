from airflow import DAG
import pendulum
import os
import sys
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlistId, 
    get_videoIds, 
    extract_video_data, 
    save_to_json
)

from datawarehouse.dwh import staging_table, core_table


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

with DAG(
    dag_id='produce_JSON',
    default_args=default_args,
    schedule_interval='0 14 * * *',  # Run daily at 14:00 (2 PM)
    catchup=False
) as dag:

    # Define the tasks
    playlist_id = get_playlistId()
    video_ids = get_videoIds(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    # Define dependencies / task order
    playlist_id >> video_ids >> extracted_data >> save_to_json_task

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='A DAG to process JSON file and insert data into both staging and core schemas',
    schedule='0 15 * * *',  # Run daily at 14:00 (2 PM)
    catchup=False
) as dag:

    # Define the tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies / task order
    update_staging >> update_core
