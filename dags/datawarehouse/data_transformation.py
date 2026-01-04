#old

import re
from datetime import timedelta, datetime

def parse_duration(duration_str):
    if not duration_str:
        return timedelta(0)

    pattern = r'P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str)

    if not match:
        return timedelta(0)

    days, hours, minutes, seconds = match.groups(default="0")

    return timedelta(
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(seconds),
    )

def transform_data(row):
    duration_td = parse_duration(row.get("duration"))

    row["duration"] = (datetime.min + duration_td).time()
    row["video_type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row


#new

# from datetime import timedelta, datetime
# import re


# def parse_duration(duration_str):
#     if not duration_str:
#         return timedelta(0)

#     pattern = r'P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
#     m = re.match(pattern, duration_str)

#     if not m:
#         return timedelta(0)

#     d, h, m_, s = m.groups(default="0")
#     return timedelta(days=int(d), hours=int(h), minutes=int(m_), seconds=int(s))


# def transform_data(row):
#     duration_td = parse_duration(row["duration"])

#     return {
#         "video_id": row["video_id"],
#         "video_title": row["video_title"],
#         "upload_date": row["upload_date"],
#         "duration": (datetime.min + duration_td).time(),
#         "video_type": "Shorts" if duration_td.total_seconds() <= 60 else "Normal",
#         "video_views": row["video_views"],
#         "likes_count": row["likes_count"],
#         "comments_count": row["comments_count"],
#     }