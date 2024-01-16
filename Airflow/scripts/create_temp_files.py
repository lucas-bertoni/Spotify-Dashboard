import os

json_file_names = ['recently_played', 'tracks']
csv_file_names = ['album_artists', 'albums', 'artists', 'recently_played', 'song_album', 'song_artists', 'songs']

try:
    if (not os.path.exists('/opt/airflow/files/temp')):
        os.mkdir('/opt/airflow/files/temp')
        os.mkdir('/opt/airflow/files/temp/json')
        os.mkdir('/opt/airflow/files/temp/csv')
except:
    print('Error creating the temp folder')

for file_name in json_file_names:
    try:
        if (not os.path.exists(f'/opt/airflow/files/temp/json/{file_name}.json')):
            os.mkdir('/opt/airflow/files/temp')
    except:
        print('Error creating the temp folder')

for file_name in csv_file_names:
    try:
        if (not os.path.exists(f'/opt/airflow/files/temp/csv/{file_name}.csv')):
            os.mkdir('/opt/airflow/files/temp')
    except:
        print('Error creating the temp folder')