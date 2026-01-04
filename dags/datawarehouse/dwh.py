
#oold
from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging

logger = logging.getLogger(__name__)
TABLE_NAME = "yt_api"


def staging_table():
    schema = "staging"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        yt_data = load_data()
        table_ids = get_video_ids(cur, schema)

        for row in yt_data:
            if len(table_ids)==0:
                insert_rows(cur, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, row)    
                else:
                    insert_rows(cur, conn, schema, row)

        ids_in_json = {row["video_id"] for row in yt_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info("Staging table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table :{e}")

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


def core_table():
    schema = "core"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        current_ids = set()

        cur.execute(f'SELECT * FROM staging.{TABLE_NAME}')
        rows = cur.fetchall()  

        for row in rows:
            current_ids.add(row["video_id"])

            if len(table_ids)==0:
                transformed_row=transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row=transform_data(row)

                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)



        ids_to_delete = set(table_ids) - current_ids
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema}Core table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table :{e}")
        raise e
 

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


#old

# from datawarehouse.data_utils import (
#     get_conn_cursor,
#     close_conn_cursor,
#     create_schema,
#     create_table,
#     get_video_ids,
# )
# from datawarehouse.data_loading import load_data
# from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
# from datawarehouse.data_transformation import transform_data

# import logging

# logger = logging.getLogger(__name__)
# TABLE_NAME = "yt_api"


# # -------------------------------------------------
# # STAGING
# # -------------------------------------------------
# def staging_table():
#     schema = "staging"
#     conn, cur = None, None

#     try:
#         conn, cur = get_conn_cursor()

#         create_schema(schema)
#         create_table(schema)

#         yt_data = load_data()
#         table_ids = get_video_ids(cur, schema)

#         for row in yt_data:
#             if row["video_id"] in table_ids:
#                 update_rows(cur, conn, schema, row)
#             else:
#                 insert_rows(cur, conn, schema, row)

#         ids_in_json = {row["video_id"] for row in yt_data}
#         ids_to_delete = set(table_ids) - ids_in_json

#         if ids_to_delete:
#             delete_rows(cur, conn, schema, ids_to_delete)

#         logger.info("Staging table update completed")

#     except Exception as e:
#         logger.error(f"Error updating staging table: {e}")
#         raise

#     finally:
#         if conn and cur:
#             close_conn_cursor(conn, cur)


# # -------------------------------------------------
# # CORE
# # -------------------------------------------------
# def core_table():
#     schema = "core"
#     conn, cur = None, None

#     try:
#         conn, cur = get_conn_cursor()

#         create_schema(schema)
#         create_table(schema)

#         table_ids = get_video_ids(cur, schema)
#         current_ids = set()

#         cur.execute(f"SELECT * FROM staging.{TABLE_NAME}")
#         rows = cur.fetchall()

#         for row in rows:
#             transformed_row = transform_data(row)
#             video_id = transformed_row["video_id"]
#             current_ids.add(video_id)

#             if video_id in table_ids:
#                 update_rows(cur, conn, schema, transformed_row)
#             else:
#                 insert_rows(cur, conn, schema, transformed_row)

#         ids_to_delete = set(table_ids) - current_ids
#         if ids_to_delete:
#             delete_rows(cur, conn, schema, ids_to_delete)

#         logger.info("Core table update completed")

#     except Exception as e:
#         logger.error(f"Error updating core table: {e}")
#         raise

#     finally:
#         if conn and cur:
#             close_conn_cursor(conn, cur)
# import json
# import os
# from airflow.hooks.base import BaseHook
# import psycopg2


# DATA_PATH = "/opt/airflow/data/video_stats.json"


# def get_pg_connection():
#     """
#     Get Postgres connection using Airflow Connection
#     """
#     conn = BaseHook.get_connection("POSTGRES_DB_YT_ELT")

#     return psycopg2.connect(
#         host=conn.host,
#         port=conn.port,
#         user=conn.login,
#         password=conn.password,
#         dbname=conn.schema,
#     )


# def staging_table():
#     """
#     Load JSON into staging table
#     """
#     if not os.path.exists(DATA_PATH):
#         raise FileNotFoundError(f"{DATA_PATH} not found")

#     with open(DATA_PATH, "r") as f:
#         records = json.load(f)

#     conn = get_pg_connection()
#     cur = conn.cursor()

#     cur.execute("TRUNCATE TABLE staging.yt_api;")

#     insert_sql = """
#         INSERT INTO staging.yt_api (
#             video_id,
#             title,
#             published_at,
#             duration,
#             view_count,
#             like_count,
#             comment_count
#         )
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """

#     for r in records:
#         cur.execute(
#             insert_sql,
#             (
#                 r["video_id"],
#                 r["title"],
#                 r["published_at"],
#                 r["duration"],
#                 int(r["view_count"]),
#                 int(r["like_count"]),
#                 int(r["comment_count"]),
#             ),
#         )

#     conn.commit()
#     cur.close()
#     conn.close()


# def core_table():
#     """
#     Merge staging into core
#     """
#     conn = get_pg_connection()
#     cur = conn.cursor()

#     merge_sql = """
#         INSERT INTO core.yt_api AS c
#         SELECT *
#         FROM staging.yt_api s
#         ON CONFLICT (video_id)
#         DO UPDATE SET
#             title = EXCLUDED.title,
#             published_at = EXCLUDED.published_at,
#             duration = EXCLUDED.duration,
#             view_count = EXCLUDED.view_count,
#             like_count = EXCLUDED.like_count,
#             comment_count = EXCLUDED.comment_count;
#     """

#     cur.execute(merge_sql)
#     conn.commit()
#     cur.close()
#     conn.close()