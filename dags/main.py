from airflow import DAG
from airflow.decorators import task
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json
)

local_tz = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="produce_json",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 14 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def t_get_playlist_id():
        return get_playlist_id()

    @task
    def t_get_video_ids(playlist_id):
        return get_video_ids(playlist_id)

    @task
    def t_extract_video_data(video_ids):
        return extract_video_data(video_ids)

    @task
    def t_save_to_json(video_data):
        save_to_json(video_data)

    playlist_id = t_get_playlist_id()
    video_ids = t_get_video_ids(playlist_id)
    video_data = t_extract_video_data(video_ids)
    t_save_to_json(video_data)