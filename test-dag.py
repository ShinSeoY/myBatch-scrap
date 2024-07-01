# 필요한 모듈 Import
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

# 디폴트 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _first_step():
    print("first_step")

def _run_producer():
    result = subprocess.run(['python3', '/opt/airflow/dags/kafka/producer.py'], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

def _complete():
    print("완료~~~~~~")

# DAG 틀 설정
with DAG(
    dag_id="test-dag-03",
    schedule_interval="* * * * *", 
    default_args=default_args,
    # tags=["naver", "search", "local", "api", "pipeline"],
    # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
    catchup=False
    ) as dag:

    # 대그 완료 출력
    print_complete = PythonOperator(
            task_id="print_complete",
            python_callable=_complete # 실행할 파이썬 함수
    )

    print_first = PythonOperator(
            task_id="print_first",
            python_callable=_first_step # 실행할 파이썬 함수
    )
    
    run_producer = PythonOperator(
        task_id='run_producer',
        python_callable=_run_producer, # 실행할 파이썬 함수
    )
    
    print_date = BashOperator(
        task_id='print_date',
        bash_command='echo "~~~~~~~~Current date is $(date)"',
        dag=dag,
    )
    
    # 파이프라인 구성하기
    # creating_table >> is_api_available >> crawl_naver >> preprocess_result >> store_result >> print_complete
    print_date >> print_first >> run_producer >> print_complete