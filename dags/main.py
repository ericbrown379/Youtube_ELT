from airflow import DAG
import pendulum
import os
import sys
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.join(CURRENT_DIR, "api")
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

from video_stats import get_playlistId, get_videoIds, extract_video_data, save_to_json

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
    'end_date': datetime(2026, 2, 17, tzinfo=local_tz)
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
