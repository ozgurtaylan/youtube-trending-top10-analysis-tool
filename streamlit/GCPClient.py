from google.oauth2 import service_account
from google.cloud import bigquery

class BigQueryClient:

    schema = [
        bigquery.SchemaField("video_order", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("video_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("channel_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("definition", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("view_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("like_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("comment_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("favorite_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("duration_in_sec", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publish_year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publish_month", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publish_day", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publish_hour", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publish_minute", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publish_second", "INTEGER", mode="REQUIRED")
    ]

    key_path = '/app/credentials/credentials.json'

    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = credentials.project_id
    dataset_id = "trending_dataset"
    table_id = "trending_table"

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)