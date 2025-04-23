# review_data 읽어오기, 하둡에 업로드하기, 업로드 했으면 파일 지우기
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess # 파이썬 코드 안에서 리눅스 명령어를 사용할 수 있는 라이브러리

def upload_to_hdfs():
    local_dir = os.path.expanduser('~/damf2/data/review_data')
    hdfs_dir = '/input/review_data'

    # hdfs dfs -mkdir -p /input/review_data
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir])

    files = []

    # 파일 읽어오기
    # os.listdir('path') : 해당 경로에 들어있는 모든 파일을 출력하는 함수
    for file in os.listdir(local_dir):
        files.append(file)

    for file in files:
        # os.path.join() : 앞의 경로와 뒤의 경로를 하나로 합쳐주는 함수
        # ~/damf2/data/review_data + 114148.csv
        local_file_path = os.path.join(local_dir, file)
        hdfs_file_path = f'{hdfs_dir}/{file}'

        # 하둡에 업로드
        # hdfs dfs -put local_file_path hdfs_file_path
        subprocess.run(['hdfs', 'dfs', '-put', local_file_path, hdfs_file_path])

        # 업로드한 파일 지우기
        os.remove(local_file_path)


with DAG(
    dag_id='04_upload_to_hdfs',
    description='upload',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5)
) as dag:
    t1 = PythonOperator(
        task_id='upload',
        python_callable=upload_to_hdfs
    )

    t1