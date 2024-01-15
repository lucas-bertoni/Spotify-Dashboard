import requests
import json
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv('/opt/airflow/.env')

postgres_conn = psycopg2.connect(os.getenv('POSTGRES_CONNECTION_STRING'))

song_ids = []

with open('/opt/airflow/files/temp/json/recently_played.json', 'r') as json_file:
    items = json.load(json_file)

    for track_obj in items:
        song_id = track_obj['track']['id']
        if (song_id not in song_ids):
            song_ids.append(song_id)

if (len(song_ids) > 0):
    song_ids = ','.join(song_ids)

    with open('/opt/airflow/files/access_token.txt', 'r') as token_file:
        access_token = token_file.read()

        url = 'https://api.spotify.com/v1/tracks'
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'ids': song_ids
        }

        response = requests.get(url=url, headers=headers, params=params)

        if (response.status_code != 200):
            raise Exception('Error getting tracks: ' + response.text)
        else:
            try:
                tracks = response.json()['tracks']
                for track in tracks:
                    album = track['album']
                    del track['available_markets']
                    del album['available_markets']

                with open('/opt/airflow/files/temp/json/tracks.json', 'w') as file:
                    json.dump(tracks, file)
            except:
                raise Exception('There was an issue creating the JSON file')