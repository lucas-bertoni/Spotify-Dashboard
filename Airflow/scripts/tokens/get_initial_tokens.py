import requests
import os
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

code = os.environ.get('CODE')
b_64 = os.getenv('B_64')

url = 'https://accounts.spotify.com/api/token'
headers = {
    'content-type': 'application/x-www-form-urlencoded',
    'Authorization': f'Basic {b_64}'
}
data = {
    'code': code,
    'redirect_uri': 'http://localhost',
    'grant_type': 'authorization_code'
}

response = requests.post(url=url, headers=headers, data=data)

if response.status_code != 200:
    raise Exception('Error getting initial tokens: ' + response.text)
else:
    at = response.json()['access_token']
    rt = response.json()['refresh_token']

    with open('/opt/airflow/files/access_token.txt', 'w') as file:
        file.write(at)

    print(rt)