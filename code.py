import requests

API_KEY = "AIzaSyDM-f6e5haFF8wzgbX5v9NLLyN-1PE-oyA"
VIDEO_ID = "O2V149cBbiY"

url = "https://www.googleapis.com/youtube/v3/videos"
params = {
    "part": "statistics,snippet",
    "id": VIDEO_ID,
    "key": API_KEY
}

response = requests.get(url, params=params)

print(response.json())
print(response.text)