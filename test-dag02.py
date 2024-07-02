# 필요한 모듈 Import
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import subprocess


def checkBranch():
    isMine = True
    return "mine" if isMine else "yours"

def mine():
    print("it's mine!!!!!!!!!!!!")
    
def yours():
    print("its yours!!!!!!!!!!!!")

default_args = {
    'owner': 'seoyoung',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 틀 설정
with DAG(
    dag_id="testDag02",
    schedule_interval="0 * * * *", 
    default_args=default_args,
    catchup=False
    ) as dag:

    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )
    
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=checkBranch
    )
    
    mine = PythonOperator(
        task_id="mine",
        python_callable=mine
    )
    
    yours = PythonOperator(
        task_id="yours",
        python_callable=yours
    )
    
    mine_ko = BashOperator(
        task_id="mine_ko",
        bash_command="echo '이건 내꺼야!!!!!!!!!!!!!!!!'",
        trigger_rule=TriggerRule.NONE_FAILED
    )    
    
    completed = BashOperator(
        task_id="completed",
        bash_command="echo 'finish!!!!!!!!!!!!!!!!!'",
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    print_date >> branch >> mine >> mine_ko >> completed
    print_date >> branch >> yours >> completed