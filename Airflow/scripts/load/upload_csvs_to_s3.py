import os
from os import listdir
from datetime import datetime
import pytz
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id=os.environ.get('S3_KEY'),
    aws_secret_access_key=os.environ.get('S3_SECRET_KEY')
)

file_names = os.listdir('/opt/airflow/files/temp/csv/')

for file_name in file_names:
    try:
        file_path = f'/opt/airflow/files/temp/csv/{file_name}'
        with open(file_path, 'r') as file:
            if (file.read().strip() != ''):
                s3_folder = 'listening_history/' if file_name.startswith('recently_played') else 'song_data/'
                s3_file_name = f'{s3_folder}{file_name[:-4]}_{str(datetime.now(pytz.timezone("America/New_York")))}.csv'
                s3_client.upload_file(file_path, os.environ.get('S3_BUCKET_NAME'), s3_file_name)
                print(s3_file_name + ' uploaded')
            else:
                print('No new data in ' + file_name)
    except ClientError as e:
        print(e)