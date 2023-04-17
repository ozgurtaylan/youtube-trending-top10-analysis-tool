import logging
from typing import Any, Dict, List
import requests
import os 

class Client:
    url = "https://youtube.googleapis.com/youtube/v3/videos" 
    payload = {
        "key": os.environ.get("YOUTUBE_API_KEY"),
        "chart": "mostPopular",
        "part": "snippet,contentDetails,statistics",
        "regionCode": "US",
        "maxResults": 10,
    }
    def __init__(self) -> None:
        pass

    def _get(self):

        print(self.payload)
        """
        Sends a GET request to the YouTube API with the given payload and returns the JSON response.
        """
        try:
            
            res = requests.get(self.url, params=self.payload)
            res.raise_for_status()
        except Exception as e:
            raise Exception(f"Error while retrieving based on payload: {self.payload}") from e
        else:
            return res.json()

    def get_trendings(self):

        response = self._get()
        videos = response.get("items")
        return videos
 
    def extract_dataset(self):
        top_ten_list = []

        videos = self.get_trendings()
        i = 1
        for video in videos:
            dict = {}
            dict["video_order"]=i
            dict["video_id"]=video['id']
            dict["video_name"]=video['snippet']["title"]
            dict["publish_date"]=video['snippet']["publishedAt"]
            dict["channel_name"]=video['snippet']["channelTitle"]
            dict["duration"]=video["contentDetails"]["duration"]
            dict["definition"]=video["contentDetails"]["definition"]
            dict["view_count"]=video["statistics"]["viewCount"]
            dict["like_count"]=video["statistics"]["likeCount"]
            dict["comment_count"]=video["statistics"]["commentCount"]
            dict["favorite_count"]=video["statistics"]["favoriteCount"]
            top_ten_list.append(dict)
            i+=1
        return top_ten_list