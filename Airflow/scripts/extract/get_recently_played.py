import requests
import time
import os
import json
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

with open('/opt/airflow/files/access_token.txt', 'r') as file:
    access_token = file.read()

    url = 'https://api.spotify.com/v1/me/player/recently-played'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'limit': 50,
        'after': int(time.time() * 1000) - 60000 * int(os.environ.get('HISTORY_LENGTH_MINUTES'))
    }

    response = requests.get(url=url, headers=headers, params=params)

    if (response.status_code != 200):
        raise Exception('Error getting recently played: ' + response.text)
    else:
        try:
            items = response.json()['items']
            print('Num songs in recently played: ' + str(len(items)))
            with open('/opt/airflow/files/temp/json/recently_played.json', 'w') as file:
                json.dump(items, file)
        except:
            raise Exception('There was an issue creating the JSON file')