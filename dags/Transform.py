from typing import Any, Dict, List
from datetime import datetime

class Transform:

    def __init__(self) -> None:
        pass

    def duration_in_sec(self,duration_input):
        s = duration_input[2:-1]
        duration = 0
        multip = 1
        time = 1
        for c in s[::-1]:
            if c.isnumeric():
                duration += ( int(c) * multip * time)
                multip = multip*10
            else:
                multip=1
                time = time*60
        return duration
    
    def date_in_details(self,publish_date_input):
        dt = datetime.strptime(publish_date_input, '%Y-%m-%dT%H:%M:%SZ')
        return int(dt.year) , int(dt.month) , int(dt.day) , int(dt.hour), int(dt.minute) , int(dt.second)
    
    def transform_dataset(self,data):
        for i, video in enumerate(data):
            video['duration_in_sec'] = self.duration_in_sec(video['duration'])
            video.pop('duration')
            year , month , day , hour , minute ,second = self.date_in_details(video['publish_date'])
            video['publish_year'] = year
            video['publish_month'] = month
            video['publish_day'] = day
            video['publish_hour'] = hour
            video['publish_minute'] = minute
            video['publish_second'] = second
            video['view_count']=int(video['view_count'])
            video['like_count']=int(video['like_count'])
            video['comment_count']=int(video['comment_count'])
            video['favorite_count']=int(video['favorite_count'])
            video.pop('publish_date')
            data[i]=video
        return data
    
