import os
import gspread
import logging
import pytz

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

current_dir = os.path.dirname(__file__)

# Mengubah working directory ke direktori DAG
os.chdir(current_dir)

def convert_time(time):

    datetime_object = datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f%z')
    jakarta_timezone = pytz.timezone('Asia/Jakarta')
    time_object = datetime_object.astimezone(jakarta_timezone).strftime('%Y-%m-%d %H:%M:%S')
    return time_object

def send_to_sheet(ti, **context):
    try:
        gc = gspread.service_account(filename="google_secret.json")
        sh = gc.open_by_key("1CDqjVawY8aKLQD6ycQoQ6mUQ4TSbdrO5lh6MuxdaLhQ").sheet1
        
        context['please1']
        data = ti.xcom_pull(task_ids='get_data')
        data = [[str(value) for value in row] for row in data]

        for new_row in data:
            convert_date = [convert_time(ts) for ts in new_row[12:13]]
            new_row[12:13] = convert_date
            logging.info(new_row)
            
            sh.append_row(new_row)
    except Exception as e:
        logging.error(f"Error occurred while sending data to sheet: {str(e)}")
        raise

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 2, 20),
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'schedule_interval': '@hourly'
# }

with DAG(
    dag_id="get_data_postgree",
    start_date=datetime(2024, 2, 19),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    get_data = PostgresOperator(
        task_id = 'get_data',
        postgres_conn_id = 'postgres_conn',
        dag=dag,
        sql="""
            select 
            t.id as "TackId",
            t."name" as "Track",
            t."name" as "Name",
            s.title as "AlbumName",
            a."name" as "ArtisName",
            t.media_type_id as "Media Type",
            t.genre_id as "GenreId",
            t.composer as "Composer",
            t.milliseconds as "Miliseconds",
            t.bytes as "Bytes",
            t.unit_price as "UnitPrice",
            '5' as DurationMinutes,
            now() as "etl_timestamp",
            'Uhuy' as StudentName
            from albums s
            inner join artists a on s.artist_id = a.id 
            inner join tracks t on s.id = t.album_id  
            limit 2
        """
    )

    send_data_sheet = PythonOperator(
        task_id = 'Send_data_to_sheet',
        python_callable = send_to_sheet,
        op_kwargs={
            "please1": "{{ ti.xcom_pull(task_ids='get_data') }}",
        },
        provide_context = True,
        dag=dag
    )

    get_data >> send_data_sheet