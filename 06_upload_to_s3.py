from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
from dotenv import load_dotenv
import os

load_dotenv('/home/ubuntu/airflow/.env')

# s3접근 함수
def upload_to_s3():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name='ap-northeast-2'
    )

    local_dir = os.path.expanduser('~/damf2/data/bitcoin')
    bucket_name = 'damf2-och' # 내 버킷이 안만들어져서 강사님 버킷으로
    s3_prefix = 'bitcoin-ydh/' # 버킷 안의 새로운 폴더 만들기

    files = []
    for file in os.listdir(local_dir):
        files.append(file)

    for file in files:
        local_file_path = os.path.join(local_dir, file)
        s3_path = f'{s3_prefix}{file}'

        s3.upload_file(local_file_path, bucket_name, s3_path) # 파일의 경로, 버킷의 폴더 경로 지정

        os.remove(local_file_path)

with DAG(
    dag_id='06_upload_to_s3',
    description='s3',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5)
) as dag:
    t1 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    t1