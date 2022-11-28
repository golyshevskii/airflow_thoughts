import requests
import pathlib
import json
import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from requests.exceptions import ConnectionError, MissingSchema

dag = DAG(
    dag_id='export_launches_dag',
    start_date=days_ago(1),
    end_date=dt.datetime(2022, 11, 30),
    schedule_interval=None,
)

# task 1 (load json file with all information about launches)
export_launches = BashOperator(
    task_id='export_launches',
    bash_command=("curl -o ./export_launches/data/launches.json -L "
                  "'https://ll.thespacedevs.com/2.0.0/launch/upcoming'"),
    dag=dag,
    cwd=dag.folder
)

def _get_launches_img():
    """Function for uploading images to the images folder"""

    pathlib.Path('./dags/export_launches/data/images').mkdir(parents=True, exist_ok=True)

    with open('./dags/export_launches/data/launches.json') as file:
        launches = json.load(file)
        img_urls = [r['image'] for r in launches['results']]

        for url in img_urls:
            try:
                response = requests.get(url)
                file_name = url.split('/')[-1]
                file_path = f'./dags/export_launches/data/images/{file_name}'

                with open(file_path, 'wb') as file:
                    file.write(response.content)
            except ConnectionError:
                print(f'Could not connect to {url}')
            except MissingSchema:
                print(f'{url} appears to be an invalid URL')

# task 2 (get all launches images)
get_images = PythonOperator(
    task_id='get_images',
    python_callable=_get_launches_img,
    dag=dag
)

# task 3 (notification about amount of images)
notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls ./export_launches/data/images/ | wc -l) images"',
    dag=dag,
    cwd=dag.folder
)

# task chain
export_launches >> get_images >> notify
