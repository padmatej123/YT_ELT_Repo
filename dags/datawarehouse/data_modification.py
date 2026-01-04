#old
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

TABLE_NAME = "yt_api"


# -------------------------------------------------
# INSERT
# -------------------------------------------------
def insert_rows(cur, conn, schema, row):

    if schema == "staging":
        cur.execute(
            f"""
            INSERT INTO {schema}.{TABLE_NAME}
            ("Video_ID","Video_Title","Upload_Date","Duration",
             "Video_Views","Likes_Count","Comments_Count")
            VALUES (
                %(video_id)s,
                %(title)s,
                %(published_at)s,
                %(duration)s,
                %(view_count)s,
                %(like_count)s,
                %(comment_count)s
            )
            """,
            {
                **row,
                "published_at": datetime.fromisoformat(
                    row["published_at"].replace("Z", "+00:00")
                ),
                "view_count": int(row["view_count"]),
                "like_count": int(row["like_count"]),
                "comment_count": int(row["comment_count"]),
            },
        )

    else:  # core
        cur.execute(
            f"""
            INSERT INTO {schema}.{TABLE_NAME}
            ("Video_ID","Video_Title","Upload_Date","Duration","Video_Type",
             "Video_Views","Likes_Count","Comments_Count")
            VALUES (
                %(Video_ID)s,
                %(Video_Title)s,
                %(Upload_Date)s,
                %(Duration)s,
                %(Video_Type)s,
                %(Video_Views)s,
                %(Likes_Count)s,
                %(Comments_Count)s
            )
            """,
            row,
        )

    conn.commit()
    logger.info(f"Inserted row: {row.get('video_id', row.get('Video_ID'))}")


# -------------------------------------------------
# UPDATE
# -------------------------------------------------
def update_rows(cur, conn, schema, row):

    if schema == "staging":
        cur.execute(
            f"""
            UPDATE {schema}.{TABLE_NAME}
            SET
                "Video_Title" = %(title)s,
                "Video_Views" = %(view_count)s,
                "Likes_Count" = %(like_count)s,
                "Comments_Count" = %(comment_count)s
            WHERE "Video_ID" = %(video_id)s
            """,
            row,
        )

    else:  # core
        cur.execute(
            f"""
            UPDATE {schema}.{TABLE_NAME}
            SET
                "Video_Title" = %(Video_Title)s,
                "Video_Views" = %(Video_Views)s,
                "Likes_Count" = %(Likes_Count)s,
                "Comments_Count" = %(Comments_Count)s
            WHERE "Video_ID" = %(Video_ID)s
            """,
            row,
        )

    conn.commit()
    logger.info(f"Updated row: {row.get('video_id', row.get('Video_ID'))}")


# -------------------------------------------------
# DELETE
# -------------------------------------------------
def delete_rows(cur, conn, schema, ids_to_delete):

    cur.execute(
        f"""
        DELETE FROM {schema}.{TABLE_NAME}
        WHERE "Video_ID" = ANY(%s)
        """,
        (list(ids_to_delete),),
    )

    conn.commit()
    logger.info(f"Deleted rows: {ids_to_delete}")

#new

# from datetime import datetime
# import logging

# logger = logging.getLogger(__name__)
# TABLE_NAME = "yt_api"


# def insert_rows(cur, conn, schema, row):

#     if schema == "staging":
#         cur.execute(
#             f"""
#             INSERT INTO {schema}.{TABLE_NAME}
#             (video_id, video_title, upload_date, duration,
#              video_views, likes_count, comments_count)
#             VALUES (
#                 %(video_id)s,
#                 %(title)s,
#                 %(published_at)s,
#                 %(duration)s,
#                 %(view_count)s,
#                 %(like_count)s,
#                 %(comment_count)s
#             )
#             """,
#             {
#                 **row,
#                 "published_at": datetime.fromisoformat(
#                     row["published_at"].replace("Z", "+00:00")
#                 ),
#                 "view_count": int(row["view_count"]),
#                 "like_count": int(row["like_count"]),
#                 "comment_count": int(row["comment_count"]),
#             },
#         )

#     else:
#         cur.execute(
#             f"""
#             INSERT INTO {schema}.{TABLE_NAME}
#             (video_id, video_title, upload_date, duration, video_type,
#              video_views, likes_count, comments_count)
#             VALUES (
#                 %(video_id)s,
#                 %(video_title)s,
#                 %(upload_date)s,
#                 %(duration)s,
#                 %(video_type)s,
#                 %(video_views)s,
#                 %(likes_count)s,
#                 %(comments_count)s
#             )
#             """,
#             row,
#         )

#     conn.commit()


# def update_rows(cur, conn, schema, row):

#     if schema == "staging":
#         cur.execute(
#             f"""
#             UPDATE {schema}.{TABLE_NAME}
#             SET video_title=%(title)s,
#                 video_views=%(view_count)s,
#                 likes_count=%(like_count)s,
#                 comments_count=%(comment_count)s
#             WHERE video_id=%(video_id)s
#             """,
#             row,
#         )
#     else:
#         cur.execute(
#             f"""
#             UPDATE {schema}.{TABLE_NAME}
#             SET video_title=%(video_title)s,
#                 video_views=%(video_views)s,
#                 likes_count=%(likes_count)s,
#                 comments_count=%(comments_count)s
#             WHERE video_id=%(video_id)s
#             """,
#             row,
#         )

#     conn.commit()


# def delete_rows(cur, conn, schema, ids):
#     cur.execute(
#         f"DELETE FROM {schema}.{TABLE_NAME} WHERE video_id = ANY(%s)",
#         (list(ids),),
#     )
#     conn.commit()