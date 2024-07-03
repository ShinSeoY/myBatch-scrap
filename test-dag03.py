import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

def parsing(**kwargs):
    logging.info("....parsing step")
    v = [{"unit": 'USD', "krUnit":'달러'}, {"unit": 'YEN', "krUnit":'엔'}]
    context=kwargs['task_instance']
    context.xcom_push(key='result', value=v)
    raise TypeError("my erororororoororororororoorooorr")
    
def saveExchange(**kwargs):
    logging.info("....saveExchange step")
    context=kwargs['task_instance']
    result = context.xcom_pull(key='result')
    logging.info(f"result........ : {result}")

def taskFailureHandler(**kwargs):
    context=kwargs['task_instance']
    context.xcom_push(key='failed_task', value={
        'task_id': context.task_id,
        'error': str(context.get('exception'))
    })
    
def dagSuccessCallback(**kwargs):
    saveBatchStatus(buildBatchStatus(kwargs, False))

def dagFailureCallback(**kwargs):
    saveBatchStatus(buildBatchStatus(kwargs, True))

def buildBatchStatus(context, isFailed):
    dagRun = context['dag_run']
    failedTaskInstance = context['task_instance'].xcom_pull(key='failed_task')
    
    batchStatus = {
        "dagName":dagRun.dag_id,
        "status": dagRun.state,
        "startTime":dagRun.start_date,
        "endTime":dagRun.end_date,
        "runId//DAG실행의고유식별자" :dagRun.run_id,
        "duration": (dagRun.end_date - dagRun.start_date).total_seconds(),
    }
    
    if isFailed:
        batchStatus.update({
            "failedStepName":failedTaskInstance.get("task_id", "-") if isinstance(failedTaskInstance, dict) else "-", 
            "errMsg":failedTaskInstance.get("error", "-") if isinstance(failedTaskInstance, dict) else "-"
        })
    return batchStatus
    
def saveBatchStatus(batchStatus):
    logging.info("*" * 100)
    logging.info(batchStatus)
    # # Airflow connection ID를 사용하여 PostgreSQL 연결
    # pg_hook = PostgresHook(postgres_conn_id='batch_status_db')
    # engine = create_engine(pg_hook.get_uri())
    # Session = sessionmaker(bind=engine)
    
    # with Session() as session:
    #     session.add(batch_status)
    #     session.commit()
 
default_args = {
    'owner': 'seoyoung',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 틀 설정
with DAG(
    dag_id="testDag-------03",
    schedule_interval="0 * * * *", 
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
    
    # 'dagName': 'testDag----03', 
    # 'status': 'failed', 
    # 'startTime': datetime.datetime(2024, 7, 3, 7, 0, 1, 25571, tzinfo=Timezone('UTC')), 
    # 'endTime': datetime.datetime(2024, 7, 3, 7, 5, 4, 413273, tzinfo=Timezone('UTC')), '
    # runId//DAG실행의고유식별자': 'scheduled__2024-07-03T06:00:00+00:00', 
    # 'duration': 303.387702, 
    # 'failedStepName': 'parsing_task', 
    # 'errMsg': "parsing() missing 1 required positional argument: 'context'"}
