import streamlit as st
import pandas as pd
import numpy as np
from GCPClient import BigQueryClient
from AirflowClient import Client
import time
from matplotlib import pyplot as plt

bq = BigQueryClient()
client = bq.client
dataset_id = BigQueryClient.dataset_id
table_id = BigQueryClient.table_id
project_id = bq.project_id

table_ref = client.dataset(dataset_id).table(table_id)
dataset_ref = client.dataset(dataset_id)

airflow = Client()

def check_table():
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False

def get_whole_data():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT *
    FROM {vars}
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_most_viewed_ten():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT video_name, view_count
    FROM {vars}
    ORDER BY view_count DESC
    LIMIT 10;
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_highest_engagement_rate():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT video_name, (like_count + comment_count + favorite_count) / view_count as engagement_rate
    FROM {vars}
    ORDER BY engagement_rate DESC
    LIMIT 10;
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_avg_like_and_comments():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT AVG(like_count) as avg_likes, AVG(comment_count) as avg_comments
    FROM {vars}
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_distribution_by_duration():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT duration_in_sec
    FROM {vars}
    ORDER BY duration_in_sec ASC;
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_corr_of_length_engagement():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT duration_in_sec, AVG(like_count) as avg_likes, AVG(comment_count) as avg_comments
    FROM {vars}
    GROUP BY duration_in_sec
    ORDER BY duration_in_sec ASC;
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_video_popularition():
    vars = bq.project_id+"."+bq.dataset_id+"."+bq.table_id
    query = f"""
    SELECT channel_name, SUM(view_count) as total_views
    FROM {vars}
    GROUP BY channel_name
    ORDER BY total_views DESC
    LIMIT 10;
    """
    query_job = client.query(query)
    while query_job.state != "DONE":
        query_job.reload()
        time.sleep(1)
    
    df = query_job.to_dataframe()
    return df

def get_analytics():
    airflow.start_external_triger()
    is_data_arrived = False
    while(is_data_arrived is False):
          is_data_arrived = check_table()
          time.sleep(1)
    flag = False
    while (flag is False):
        print("Checking for load...")
        jobs = client.list_jobs(project=project_id)
        job_list = list(jobs)
        if str(job_list[0]).split('<')[0]=="LoadJob":
            flag = True
        time.sleep(5)

    df_whole_data = get_whole_data()
    df_mostviewed = get_most_viewed_ten()
    df_engagement = get_highest_engagement_rate()
    df_avg_like_comment = get_avg_like_and_comments()
    df_distribution = get_distribution_by_duration()
    df_corr = get_corr_of_length_engagement()
    df_popularity = get_video_popularition()
    
    st.subheader("Dataset snippet")
    st.write(df_whole_data)

    st.subheader("View counts of videos")
    st.bar_chart(df_mostviewed, x='video_name', y=["view_count"])

    
    st.subheader("Highest engagement rate (i.e. the highest ratio of likes, comments, and favorites to views)")
    fig, ax = plt.subplots()
    ax.barh(df_engagement["video_name"], df_engagement["engagement_rate"])
    ax.set_xlabel("Engagement Rate")
    ax.set_ylabel("Video Name")
    ax.invert_yaxis()
    st.pyplot(fig)

    
    st.subheader("Average Like and Comments")
    st.table(df_avg_like_comment)

    
    st.subheader("Distribution of video durations")
    fig, ax = plt.subplots()
    ax.hist(df_distribution["duration_in_sec"], bins=5)
    ax.set_xlabel("Duration (sec)")
    ax.set_ylabel("Frequency")
    st.pyplot(fig)

    
    st.subheader("Correlation between video length and engagement")
    fig, ax = plt.subplots()
    ax.scatter(df_corr["duration_in_sec"], df_corr["avg_likes"], color="blue", label="Average Likes")
    ax.scatter(df_corr["duration_in_sec"], df_corr["avg_comments"], color="red", label="Average Comments")
    ax.set_xlabel("Duration (sec)")
    ax.set_ylabel("Average Engagement")
    ax.legend()
    st.pyplot(fig)

   
    st.subheader("Video popularity by channel")
    fig, ax = plt.subplots()
    ax.barh(df_popularity["channel_name"], df_popularity["total_views"], color="green")
    ax.set_xlabel("Total Views")
    ax.set_ylabel("Channel")
    st.pyplot(fig)

st.title('Youtube Top 10 Trending Video Analysis')

if st.button('Get Analytics'):
    st.text('Getting analytics...')
    get_analytics()
else:
    st.write('Welcome!')