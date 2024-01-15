import pandas as pd
from dateutil import parser
import pytz
import json

with open('/opt/airflow/files/temp/json/recently_played.json', 'r') as json_file:
    items = json.load(json_file)
    rows = []
                
    for track_obj in items:
        track = track_obj['track']

        played_at = str(parser.parse(track_obj['played_at']).astimezone(pytz.timezone('America/New_York')))

        row = {
            'played_at': played_at,
            'duration_played': track['duration_ms'],
            'song_id': track['id']
        }

        rows.append(row)

    df = pd.DataFrame(rows)
    df = df.drop_duplicates()

    df.to_csv('/opt/airflow/files/temp/csv/recently_played.csv', index=False)