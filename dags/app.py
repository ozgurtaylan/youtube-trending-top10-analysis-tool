from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.models import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom
from google.cloud.exceptions import NotFound
from google.cloud.bigquery.dataset import DatasetReference
from airflow.utils import timezone
from datetime import datetime , timedelta
import YoutubeClient 
import pytz
from GCPClient import BigQueryClient
import Transform
import requests 
import json
import pandas as pd
import os
import pendulum

local_tz = pendulum.timezone("Europe/Istanbul")

def funct_create_bq_dataset():
 
    client = BigQueryClient.client
    dataset_id = BigQueryClient.dataset_id
 
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    BigQueryClient.client.create_dataset(dataset, exists_ok=True)

def funct_create_bq_table():
    client = BigQueryClient.client
    dataset_id = BigQueryClient.dataset_id
    table_id = BigQueryClient.table_id
    schema = BigQueryClient.schema
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
     

def funct_extract_data(ti):
    youtube_client = YoutubeClient.Client()
    trending_dataset = youtube_client.extract_dataset()
    
    now = datetime.now(local_tz)
    current_time = now.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join("data", f"data_{current_time}.json")

    with open(file_path, "w") as f:
        json.dump(trending_dataset, f)
    ti.xcom_push(key = "file_path",value=file_path)
    

def funct_transform_data(ti):
    
    file_path = ti.xcom_pull(key = "file_path")
    with open(file_path,"r") as f:
        trending_dataset = f.read()
        
    transform = Transform.Transform()
    trending_dataset = json.loads(trending_dataset)
    trending_dataset = transform.transform_dataset(trending_dataset)

    with open(file_path, "w") as f:
        json.dump(trending_dataset, f)

def funct_load_data_to_bq(ti):
    file_path = ti.xcom_pull(key = "file_path")
    with open(file_path,"r") as f:
        processed_trending_dataset = f.read()

    processed_trending_dataset = json.loads(processed_trending_dataset)

    processed_trending_dataset_df = pd.DataFrame(processed_trending_dataset)
    processed_trending_dataset_df = processed_trending_dataset_df.reset_index(drop=True)

    client = BigQueryClient.client
    dataset_id = BigQueryClient.dataset_id
    table_id = BigQueryClient.table_id
    schema = BigQueryClient.schema
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=BigQueryClient.schema,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
    processed_trending_dataset_df, table_ref, job_config=job_config
    )
    job.result()


default_args = {
    "owner":"airflow",
    "email_on_failure":False,
    "email_on_retry":False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    "youtube_data_pipeline",
    default_args=default_args,
    description="Analysis of youtube top 10 trending vides",
    catchup=False,
    schedule_interval=None,
    tags=["youtube"],
    start_date=datetime(2023, 4 ,15 , tzinfo=local_tz)
) as dag:
    
    create_bq_dataset=PythonOperator(
        task_id = "create_bq_dataset",
        python_callable = funct_create_bq_dataset
    )

    create_bq_table=PythonOperator(
        task_id = "create_bq_table",
        python_callable = funct_create_bq_table
    )

    extract_data=PythonOperator(
        task_id = "extract_data",
        python_callable = funct_extract_data
    )

    transform_data=PythonOperator(
        task_id = "transform_data",
        python_callable = funct_transform_data
    )

    load_data_to_bq=PythonOperator(
        task_id = "load_data_to_bq",
        python_callable = funct_load_data_to_bq
    ) 

create_bq_dataset >> create_bq_table >> extract_data >> transform_data >> load_data_to_bq 