import os
import csv
import pandas as pd
from airflow import models
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def copy_to_local():
    conn = GCSHook(gcp_conn_id = 'my_gcp_conn')
    bucket = 'twitter_streaming_data_bucket'
    objects_list = conn.list(bucket_name = bucket)
    with open('combined.csv', 'w') as file:
        writer = csv.writer(file)
        for object in objects_list:
            raw_data = conn.download(bucket_name = bucket, object_name = object)
            data = raw_data.decode('utf-8')
            data_list= data.split(',')
            writer.writerow(data_list)
            conn.copy(source_bucket = bucket, source_object = object, destination_bucket = 'consumed-twitter-data')
            conn.delete(bucket_name = bucket, object_name = object)

def data_cleaner():
    df = pd.read_csv('combined.csv', header = None)
    tweet = df.iloc[:,2]
    tweet = tweet.str.lower()
    tweet = tweet.str.replace(r'https?://.+\b|.+\.com|www\..+|.+\.in', '')  #Removing links
    tweet = tweet.str.replace('@', '').replace('#', '').replace('_', ' ')
    tweet = tweet.str.replace(r'[^\x00-\x7F]+', '')     #Removing emojis
    tweet = tweet.str.replace(r'[^A-Za-z0-9 ]', '')
    df.iloc[:,2] = tweet
    df.iloc[:,-1] = df.iloc[:,-1].str.replace('\n', '')
    source = df.iloc[:,3]
    source = source.str.extract(r'<.+>([\w\s]+)<.+>', expand = False) # Extacting text from HTML tag
    df.iloc[:,3] = source
    df.to_csv('combined.csv', index = False, header = None)

def delete_file():
    os.remove('combined.csv')


with DAG("twitter_DAG", start_date = days_ago(1), schedule_interval='* * * * *', catchup = False) as dag:

    gcs_to_local = PythonOperator(
        task_id = 'gcs_to_local',
        python_callable = copy_to_local
    )

    local_transformations = PythonOperator(
        task_id = 'local_transformations',
        python_callable = data_cleaner
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_to_gcs',
        gcp_conn_id = 'my_gcp_conn',
        src = 'combined.csv',
        dst = 'combined.csv',
        bucket = 'consumed-twitter-data'
    )

    remove_local = PythonOperator(
        task_id = 'remove_local',
        python_callable = delete_file
    )

    gcs_to_big_query = GCSToBigQueryOperator(
        task_id = 'gcs_to_big_query',
        google_cloud_storage_conn_id = 'my_gcp_conn',
        bigquery_conn_id = 'my_gcp_conn',
        bucket = 'consumed-twitter-data',
        source_objects = ['combined.csv'],
        destination_project_dataset_table = 'streaming_data.twitter_data',
        schema_fields = [
            {'name':'tweet_id', 'type':'INTEGER', 'mode':'REQUIRED'},
            {'name':'tweet_created_at', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'text', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'source', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'reply_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'retweet_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'favorite_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_id', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_name', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'user_location', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'user_followers_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_friends_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_listed_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_favourites_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_statuses_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'profile_created_at', 'type':'STRING', 'mode':'NULLABLE'}
        ],        
        write_disposition = 'WRITE_APPEND'
    )

    gcs_to_local >> local_transformations >> upload_to_gcs >> remove_local >> gcs_to_big_query
    