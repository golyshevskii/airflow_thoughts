from urllib import request
import datetime as dt


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id='wikipedia_pageviews',
    start_date=days_ago(1),
    end_date=dt.datetime(2022, 11, 30),
    schedule_interval=None,
    template_searchpath="./dags/wikipedia_pageviews/sql"
)

# https://dumps.wikimedia.org/other/pageviews/
# {year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz
def _get_pageviews(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{str(int(day)-1):0>2}-{hour:0>2}0000.gz"
    )
    print(url)
    request.urlretrieve(url, output_path)

# task 1 (load wikipedia pageviews)
exctract_pageviews = PythonOperator(
    task_id='exctract_pageviews',
    python_callable=_get_pageviews,
    op_kwargs={
        'year': '{{ execution_date.year }}',
        'month': '{{ execution_date.month }}',
        'day': '{{ execution_date.day }}',
        'hour': '{{ execution_date.hour }}',
        'output_path': './dags/wikipedia_pageviews/data/wikipageviews.gz'
    },
    dag=dag
)

# task 2 (unzip wikipageview.gz)
extract_gz = BashOperator(
    task_id='extract_gz',
    bash_command="gunzip --force './wikipedia_pageviews/data/wikipageviews.gz'",
    dag=dag,
    cwd=dag.folder
)

def _fetch_views(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)

    with open('./dags/wikipedia_pageviews/data/wikipageviews', 'r') as files:
        for file in files:
            domain_code, page_title, view_counts, _ = file.split(' ')

            if domain_code == 'en' and page_title in pagenames:
                result[page_title] = view_counts
    
    print(result)

    with open('./dags/wikipedia_pageviews/sql/insert_pageviews.sql', 'w') as file:
        for title, views in result.items():
            sql = f"""insert into pageviews_count (pagename, pageviews, date) 
            values ('{title}', '{views}', '{execution_date}');\n"""
            file.write(sql)

# task 2 (fetch pageviews by page title)
fetch_views = PythonOperator(
    task_id='fetch_views',
    python_callable=_fetch_views,
    op_kwargs={
        'pagenames': {'Google', 'Amazon', 'Apple'}
    },
    dag=dag
)

# task 4 (insert data into table)
insert_pageviews = PostgresOperator(
    task_id='insert_pageviews',
    postgres_conn_id='postgres_localhost',
    sql='insert_pageviews.sql',
    autocommit=True,
    dag=dag
)

exctract_pageviews >> extract_gz >> fetch_views >> insert_pageviews
