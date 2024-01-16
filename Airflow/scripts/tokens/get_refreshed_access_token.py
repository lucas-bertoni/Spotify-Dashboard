import requests
import os
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

refresh_token = os.environ.get('REFRESH_TOKEN')
client_id = os.environ.get('CLIENT_ID')
b_64 = os.getenv('B_64')

url = 'https://accounts.spotify.com/api/token'
headers = {
    'content-type': 'application/x-www-form-urlencoded',
    'Authorization': f'Basic {b_64}'
}
data = {
    'grant_type': 'refresh_token',
    'refresh_token': refresh_token,
    'client_id': client_id
}

response = requests.post(url=url, headers=headers, data=data)

if response.status_code != 200:
    raise Exception('Error getting refreshed access token: ' + response.text)
else:
    at = response.json()['access_token']

    if (os.path.exists('/opt/airflow/files/access_token.txt')):
        with open('/opt/airflow/files/access_token.txt', 'w') as file:
            file.write(at)
    else:
        with open('/opt/airflow/files/access_token.txt', 'a') as file:
            file.write(at)
    