import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='./.env')

YOUR_API_KEY=os.getenv("YOUR_API_KEY")
CHANNEL="MrBeast"


def get_playlist_id():
    try:
        url= f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL}&key={YOUR_API_KEY}"

        response = requests.get(url)

        # print(response)
        response.raise_for_status()
        data= response.json()
        # print(json.dumps(data,indent=5))

        channel_items=data["items"][0]
        channel_playlist_id=channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        # print(channel_playlist_id)
        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        raise e   
    

if __name__ == "__main__":
    print("get_playlist_id is executed")
    print(get_playlist_id())
    
else:
    print("get_playlist_is is not executed")        