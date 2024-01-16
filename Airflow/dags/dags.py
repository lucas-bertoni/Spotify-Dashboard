import pendulum
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 8, tzinfo=pendulum.timezone('America/New_York'))
}

with DAG(dag_id='REFRESH-ACCESS-TOKEN', default_args=default_args, schedule_interval=timedelta(minutes=50), catchup=False) as dag_1:
    get_refreshed_access_token = BashOperator(
        task_id='get_refreshed_token',
        bash_command=f'python {os.getcwd()}/scripts/tokens/get_refreshed_access_token.py',
        dag=dag_1
    )

with DAG(dag_id='SPOTIFY-LISTENING-HISTORY', default_args=default_args, schedule_interval=None, catchup=False) as dag_2: # timedelta(minutes=10)
    get_recently_played = BashOperator(
        task_id='get_recently_played',
        bash_command=f'python {os.getcwd()}/scripts/extract/get_recently_played.py',
        dag=dag_2
    )

    get_tracks = BashOperator(
        task_id='get_tracks',
        bash_command=f'python {os.getcwd()}/scripts/extract/get_tracks.py',
        dag=dag_2
    )

    transform_recently_played = BashOperator(
        task_id='transform_recently_played',
        bash_command=f'python {os.getcwd()}/scripts/transform/transform_recently_played.py',
        dag=dag_2
    )

    transform_tracks = BashOperator(
        task_id='transform_tracks',
        bash_command=f'python {os.getcwd()}/scripts/transform/transform_tracks.py',
        dag=dag_2
    )

    upload_csvs_to_s3 = BashOperator(
        task_id='upload_csvs_to_s3',
        bash_command=f'python {os.getcwd()}/scripts/load/upload_csvs_to_s3.py',
        dag=dag_2
    )

    load_into_postgres = BashOperator(
        task_id='load_into_postgres',
        bash_command=f'python {os.getcwd()}/scripts/load/load_into_postgres.py',
        dag=dag_2
    )

    get_recently_played >> get_tracks >> [transform_tracks, transform_recently_played] >> upload_csvs_to_s3 >> load_into_postgres