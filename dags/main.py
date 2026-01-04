from airflow import DAG
from airflow.decorators import task
import pendulum

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("Asia/Kolkata")

# ------------------------------------------------
# DAG 1: Produce JSON
# ------------------------------------------------
with DAG(
    dag_id="produce_json",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule="0 14 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "extract"],
) as produce_json_dag:

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


# ------------------------------------------------
# DAG 2: Update DB (CORRECT & AIRFLOW-SAFE)
# ------------------------------------------------
with DAG(
    dag_id="update_db",
    description="Load JSON into staging and merge into core",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule="0 15 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "warehouse"],
) as update_db_dag:

    @task
    def t_update_staging():
        staging_table()

    @task
    def t_update_core():
        core_table()

    t_update_staging() >> t_update_core()