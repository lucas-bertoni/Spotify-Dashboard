import os
from datetime import datetime
import pytz
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import numpy
import psycopg2.extras as extras

# load_dotenv('C:/Users/lbert/Desktop/Airflow/.env')
load_dotenv('/opt/airflow/.env')

s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id=os.environ.get('S3_KEY'),
    aws_secret_access_key=os.environ.get('S3_SECRET_KEY')
)

bucket_name = os.environ.get("S3_BUCKET_NAME")

postgres_conn = psycopg2.connect(os.getenv('POSTGRES_CONNECTION_STRING'))
postgres_conn.autocommit = True

query = 'SELECT last_extract_time FROM "SPOTIFY_DATA"."CONFIG";'
cursor = postgres_conn.cursor()
cursor.execute(query)
LAST_EXTRACT_TIME = cursor.fetchall()[0][0]
cursor.close()

def main():
    try:
        s3_objects = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
    except:
        print('No objects in S3')

    recently_played_file_names = []
    song_file_names            = []
    artist_file_names          = []
    album_file_names           = []
    song_artists_file_names    = []
    song_album_file_names      = []
    album_artists_file_names   = []

    file_objects = [{ k:v for k,v in obj.items() if is_valid_file(k, v) } for obj in s3_objects]
    filtered = filter(lambda obj: len(obj.keys()) > 0, file_objects)
    file_names = list(map(lambda obj: obj['Key'], list(filtered)))

    for file_name in file_names:
        if (file_name.startswith('listening_history/')):
            recently_played_file_names.append(file_name)
        elif (file_name.startswith('song_data/songs_')):
            song_file_names.append(file_name)
        elif (file_name.startswith('song_data/artists_')):
            artist_file_names.append(file_name)
        elif (file_name.startswith('song_data/albums_')):
            album_file_names.append(file_name)
        elif (file_name.startswith('song_data/song_artists_')):
            song_artists_file_names.append(file_name)
        elif (file_name.startswith('song_data/song_album_')):
            song_album_file_names.append(file_name)
        elif (file_name.startswith('song_data/album_artists_')):
            album_artists_file_names.append(file_name)
        else:
            print('This should never be printed')

    recently_played_df = extract_data_to_df(recently_played_file_names)
    song_df            = extract_data_to_df(song_file_names)
    artist_df          = extract_data_to_df(artist_file_names)
    albums_df          = extract_data_to_df(album_file_names)
    song_artists_df    = extract_data_to_df(song_artists_file_names)
    song_album_df      = extract_data_to_df(song_album_file_names)
    album_artists_df   = extract_data_to_df(album_artists_file_names)

    update_last_extract_time()

    if (len(recently_played_df.index) > 0):
        run_query('insert_recently_played.sql', recently_played_df)
    else:
        print('No recently played data to insert')
    if (len(song_df.index) > 0):
        run_query('insert_songs.sql', song_df)
    else:
        print('No song data to insert')
    if (len(artist_df.index) > 0):
        run_query('insert_artists.sql', artist_df)
    else:
        print('No artist data to insert')
    if (len(albums_df.index) > 0):
        run_query('insert_albums.sql', albums_df)
    else:
        print('No album data to insert')
    if (len(song_artists_df.index) > 0):
        run_query('insert_song_artists.sql', song_artists_df)
    else:
        print('No song artist data to insert')
    if (len(song_album_df.index) > 0):
        run_query('insert_song_album.sql', song_album_df)
    else:
        print('No song album data to insert')
    if (len(album_artists_df.index) > 0):
        run_query('insert_album_artists.sql', album_artists_df)
    else:
        print('No album artist data to insert')



def run_query(script_name, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    # script_path = f'C:/Users/lbert/Desktop/Airflow/scripts/load/sql/{script_name}'
    script_path = f'/opt/airflow/scripts/load/sql/{script_name}'
    query = f'{open(script_path, "r").read()}' % (cols)

    try:
        cursor = postgres_conn.cursor()
        extras.execute_values(cursor, query, tuples)
        print(script_name + ' executed successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        postgres_conn.rollback() 
        cursor.close() 
        raise Exception('Error: %s' % error)

def extract_data_to_df(file_names):
    dfs = []

    for file_name in file_names:
        try:
            object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        except ClientError as e:
            print(f'Error getting the file: {file_name}')
            raise Exception(e)
        
        try:
            df = pd.read_csv(object['Body'])
            dfs.append(df)
        except:
            print('csv is empty')

    if (len(dfs) > 0):
        main_df = pd.concat(dfs)
        main_df.drop_duplicates()
    else:
        main_df = pd.DataFrame()

    return main_df

def is_valid_file(key, value):
    if (key == 'Key'):
        if(str(value)[-4:] == '.csv'):
            try:
                return datetime.strptime(str(value)[-36:-10], '%Y-%m-%d %H:%M:%S.%f') > LAST_EXTRACT_TIME
            except:
                return False
            
    return False

def update_last_extract_time():
    query = f'UPDATE "SPOTIFY_DATA"."CONFIG" SET last_extract_time = \'{str(datetime.now(pytz.timezone("America/New_York")))}\';'
    cursor = postgres_conn.cursor()
    cursor.execute(query)
    cursor.close()

if __name__ == '__main__':
    main()