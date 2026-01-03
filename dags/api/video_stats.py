import json
import requests
import os
from datetime import date
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL = Variable.get("CHANNEL_HANDLE")   

def get_playlist_id():
    url = (
        "https://youtube.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle={CHANNEL}&key={API_KEY}"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_video_ids(playlist_id):
    video_ids = []
    page_token = None

    while True:
        url = (
            "https://youtube.googleapis.com/youtube/v3/playlistItems"
            f"?part=contentDetails&maxResults=50&playlistId={playlist_id}"
            f"&key={API_KEY}"
        )

        if page_token:
            url += f"&pageToken={page_token}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids


def extract_video_data(video_ids):
    extracted_data = []

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        video_ids_str = ",".join(batch)

        url = (
            "https://youtube.googleapis.com/youtube/v3/videos"
            f"?part=snippet,contentDetails,statistics&id={video_ids_str}"
            f"&key={API_KEY}"
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            extracted_data.append({
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "published_at": item["snippet"]["publishedAt"],
                "duration": item["contentDetails"]["duration"],
                "view_count": item["statistics"].get("viewCount"),
                "like_count": item["statistics"].get("likeCount"),
                "comment_count": item["statistics"].get("commentCount"),
            })

    return extracted_data


def save_to_json(extracted_data):
    os.makedirs("./data", exist_ok=True)
    file_path = f"./data/YT_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)