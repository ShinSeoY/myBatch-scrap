import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

import requests
from scrap_exchange import get_webpage, extract_information
from spring_server_api import saveBatchStatus
from database.db_config import SessionLocal, get_db, engine
from database.model.exchange import Base, Exchange
# from database.model.batch_status import Base, BatchStatus
from database.dto.batch_status import BatchStatus
from kafka_util.producer import sender
import time
import random

# Base.metadata.create_all(bind=engine)
# db = next(get_db())

# scraping
def parsing(**kwargs):
    logging.info("*** parsing step")
    # 1-3분 사이의 랜덤한 시간 동안 대기
    wait_time = random.randint(60, 180)
    time.sleep(wait_time)
    
    exchangeObjList = []
    scrapData = extract_information(get_webpage())
    
    for key,value in scrapData.items():
        exchangeObj = {
            "unit":value.get("exchangeCode"),
            "name":value.get("name"),
            "kr_unit":value.get("currencyName"),
            "deal_basr":float(value.get("calcPrice"))
        }
        exchangeObjList.append(exchangeObj)
    
    context=kwargs['task_instance']
    context.xcom_push(key='exchangeObjList', value=exchangeObjList)
    
# kafka producer   
def saveExchange(**kwargs):
    logging.info("*** saveExchange step")
    context=kwargs['task_instance']
    exchangeObjList = context.xcom_pull(key='exchangeObjList')
    logging.info(f"*** exchangeObjList : {exchangeObjList}")
    
    sender(exchangeObjList)

def taskFailureHandler(context):
    ti = context['task_instance']
    ti.xcom_push(key='failed_task', value={
        'task_id': ti.task_id,
        'error': str(context.get('exception'))
    })
    
def dagSuccessCallback(context):
    saveBatchStatus(buildBatchStatus(context, False))

def dagFailureCallback(context):
    saveBatchStatus(buildBatchStatus(context, True))

def buildBatchStatus(context, isFailed):
    dag_run = context['dag_run']
    failedTaskInstance = context['task_instance'].xcom_pull(key='failed_task')
    
    batchStatus = BatchStatus(
        workflow_id=dag_run.run_id,
        workflow_name = dag_run.dag_id,
        status = dag_run.state,
        start_time= dag_run.start_date,
        end_time= dag_run.end_date,
        duration = int((dag_run.end_date - dag_run.start_date).total_seconds() * 1000)
    )
    
    if isFailed:
        batchStatus.failed_step_name = failedTaskInstance.get("task_id", "-") if isinstance(failedTaskInstance, dict) else "-"
        batchStatus.err_msg = failedTaskInstance.get("error", "-") if isinstance(failedTaskInstance, dict) else "-"
    return batchStatus

default_args = {
    'owner': 'seoyoung',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="save-exchange",
    schedule_interval="* * * * *", # 1분마다 실행
    default_args=default_args,
    catchup=False,
    on_success_callback=dagSuccessCallback,
    on_failure_callback=dagFailureCallback
    ) as dag:

    parsing_task = PythonOperator(
        task_id="parsing_task",
        python_callable=parsing,
        on_failure_callback=taskFailureHandler
    )
    
    save_exchange_task = PythonOperator(
        task_id="save_exchange_task",
        python_callable=saveExchange,
        on_failure_callback=taskFailureHandler,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    completed_task = BashOperator(
        task_id = "completed_task",
        bash_command="echo 'completed!!!!!!!!!!'",
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    parsing_task >> save_exchange_task >> completed_task



# def saveBatchStatus(batchStatus):
#     logging.info("*** save batch status")
#     with next(get_db()) as db:
#         try:
#             db.add(batchStatus)
#             db.commit()
#             db.refresh(batchStatus)
#             logging.info(f"*** batchStatus : {str(batchStatus)}")
#         except Exception as e:
#             db.rollback()
#             logging.error(f"*** Error saving BatchStatus: {e}")
 