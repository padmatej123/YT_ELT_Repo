# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import psycopg2
# from psycopg2.extras import RealDictCursor
# from psycopg2.extras import RealDictCursor
# import logging

# logger = logging.getLogger(__name__)

# table = "yt_api"


# def get_conn_cursor():
#     hook = PostgresHook(
#         postgres_conn_id="postgres_db_yt_elt",
#         database="elt_db"
#     )
#     conn = hook.get_conn()
#     cur = conn.cursor(cursor_factory=RealDictCursor)
#     return conn, cur


# def close_conn_cursor(conn, cur):
#     cur.close()
#     conn.close()


# def create_schema(schema):
#     conn, cur = get_conn_cursor()
#     try:
#         cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
#         conn.commit()
#         logger.info(f"Schema ensured: {schema}")
#     finally:
#         close_conn_cursor(conn, cur)


# def create_table(schema):
#     conn, cur = get_conn_cursor()
#     try:
#         if schema == "staging":
#             table_sql = f"""
#                 CREATE TABLE IF NOT EXISTS {schema}.{table} (
#                     "Video_ID" VARCHAR(11) PRIMARY KEY,
#                     "Video_Title" TEXT NOT NULL,
#                     "Upload_Date" TIMESTAMP NOT NULL,
#                     "Duration" VARCHAR(20),
#                     "Video_Views" INT,
#                     "Likes_Count" INT,
#                     "Comments_Count" INT
#                 );
#             """
#         else:
#             table_sql = f"""
#                 CREATE TABLE IF NOT EXISTS {schema}.{table} (
#                     "Video_ID" VARCHAR(11) PRIMARY KEY,
#                     "Video_Title" TEXT NOT NULL,
#                     "Upload_Date" TIMESTAMP NOT NULL,
#                     "Duration" TIME,
#                     "Video_Type" VARCHAR(10),
#                     "Video_Views" INT,
#                     "Likes_Count" INT,
#                     "Comments_Count" INT
#                 );
#             """

#         cur.execute(table_sql)
#         conn.commit()
#         logger.info(f"Table ensured: {schema}.{table}")

#     finally:
#         close_conn_cursor(conn, cur)


# def get_video_ids(cur, schema):
#     cur.execute(
#         f'SELECT "Video_ID" FROM {schema}.{table};'
#     )
#     rows = cur.fetchall()
#     return [row["Video_ID"] for row in rows]

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)

TABLE_NAME = "yt_api"


def get_conn_cursor():
    hook = PostgresHook(
        postgres_conn_id="postgres_db_yt_elt",
        database="elt_db"
    )
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):
    conn, cur = get_conn_cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.commit()
    finally:
        close_conn_cursor(conn, cur)


def create_table(schema):
    conn, cur = get_conn_cursor()
    try:
        if schema == "staging":
            sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{TABLE_NAME} (
                video_id VARCHAR(11) PRIMARY KEY,
                video_title TEXT,
                upload_date TIMESTAMP,
                duration VARCHAR(20),
                video_views BIGINT,
                likes_count BIGINT,
                comments_count BIGINT
            );
            """
        else:
            sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{TABLE_NAME} (
                video_id VARCHAR(11) PRIMARY KEY,
                video_title TEXT,
                upload_date TIMESTAMP,
                duration TIME,
                video_type VARCHAR(10),
                video_views BIGINT,
                likes_count BIGINT,
                comments_count BIGINT
            );
            """
        cur.execute(sql)
        conn.commit()
    finally:
        close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    cur.execute(f"SELECT video_id FROM {schema}.{TABLE_NAME}")
    return [r["video_id"] for r in cur.fetchall()]