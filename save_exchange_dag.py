import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from scrap_exchange import get_webpage, extract_information
from database.db_config import get_db, engine
from database.model.exchange import Base, Exchange
from kafka_util.producer import sender

Base.metadata.create_all(bind=engine)
db = next(get_db())

def parsing(**kwargs):
    #여기서 scrap
    logging.info("....parsing step")
    exchangeObjList = []
    scrapData = extract_information(get_webpage())
    
    for key,value in scrapData.items():
        exchangeObj = {
            "unit":value.get("exchangeCode"),
            "name":value.get("name"),
            "kr_unit":value.get("currencyName"),
            "deal_basr":float(value.get("calcPrice"))
            # ,
            # exchange_rate=float(data.get("calcPrice")),  # Assuming exchange_rate is same as calcPrice
            # ttb=0.0,  # Assuming ttb and tts are not provided in the JSON
            # tts=0.0
        }
        exchangeObjList.append(exchangeObj)
    
    context=kwargs['task_instance']
    context.xcom_push(key='exchangeObjList', value=exchangeObjList)
    # raise TypeError("my erororororoororororororoorooorr")
    
def saveExchange(**kwargs):
    # 여기서는 producer로 전송해야함
    logging.info("....saveExchange step")
    context=kwargs['task_instance']
    exchangeObjList = context.xcom_pull(key='exchangeObjList')
    logging.info(f"exchangeObjList........ : {exchangeObjList}")
    
    # exchangeList = [Exchange(**exchangeObj) for exchangeObj in exchangeObjList]
    
    sender(exchangeObjList)
    # for exchange in exchangeList:
    #     db.add(exchange)
    # db.commit()
    # for exchange in exchangeList:
    #     db.refresh(exchange)

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
    # 여기서는 디비에 저장해야함 
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
    dag_id="dag-test-02",
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


# {'dagName': 'testDag--------03', 
#  'status': 'success', 
#  'startTime': datetime.datetime(2024, 7, 3, 8, 20, 17, 548249, tzinfo=Timezone('UTC')),
#  'endTime': datetime.datetime(2024, 7, 3, 8, 20, 20, 836149, tzinfo=Timezone('UTC')), 
#  'runId//DAG실행의고유식별자': 'manual__2024-07-03T08:20:17.433288+00:00', 
#  'duration': 3.2879}