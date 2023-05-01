from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum
import vertica_python

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

conn_info = {'host': 'vertica.tgcloudenv.ru',
             'port': '5433',
             'user': 'RINATSHAKBASAROVYANDEXRU',
             'password': 'VOYeNOLwZycdhz7',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

def load_data(key: str, rows: str):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f""" COPY RINATSHAKBASAROVYANDEXRU__STAGING.{key}({rows}) 
                        FROM LOCAL '/data/{key}.csv'
                        DELIMITER ',' enclosed '"'  REJECTED DATA AS TABLE {key}_rej ;""")
        cur.close()
        conn.close()



def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    bucket_files = ['groups.csv', 'users.csv', 'dialogs.csv']

    task1 = PythonOperator(
        task_id='fetch_groups_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )

    task2 = PythonOperator(
        task_id='fetch_users_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )

    task3 = PythonOperator(
        task_id='fetch_dialogs_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command="for file in /data/*.csv; do echo $file && head -n 10 $file; done",
        params={'files': [f'/data/{f}' for f in bucket_files]}
    )

    load_data_users = PythonOperator(
        task_id='load_data_users',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'users', 'rows': 'id,chat_name,registration_dt,country,age'}
    )

    load_data_groups = PythonOperator(
        task_id='load_data_groups',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups', 'rows': 'id,admin_id,group_name,registration_dt,is_private'}
    )
    load_data_dialogs = PythonOperator(
        task_id='load_data_dialogs',
        python_callable=load_data,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs', 'rows': 'message_id,message_ts,message_from,message_to,message,message_type'}
    )

    # Define the task dependencies
    [task1, task2, task3] >> print_10_lines_of_each >> [load_data_users, load_data_groups, load_data_dialogs]


get_data_dag = sprint6_dag_get_data()