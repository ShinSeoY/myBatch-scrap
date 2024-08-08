from datetime import datetime, timedelta
import json
import os
import logging
import requests
from dotenv import load_dotenv
load_dotenv()

from database.dto.batch_status import BatchStatus


def saveBatchStatus(batch_status):
    logging.info('**** start save batch status')
    try:
        # url = "http://localhost:8083/api/exchange/batch-status" # in local
        url = "http://my-exchange:8082/api/exchange/batch-status" # in docker
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": os.environ.get("SPRING_X_API_KEY")
        }
        
        data = json.dumps(BatchStatus.to_dict(batch_status))

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 201 or 200:
            print("BatchStatus successfully sent to Spring application")
        else:
            logging.info('*** not save')
            print(f"Status code: {response.status_code}")
    except Exception as e:
        print(e)
        logging.error(e)


# def main():
#     start_date = datetime.now() - timedelta(minutes=1)
#     end_date = datetime.now()
    
#     batch_status = BatchStatus(
#         dag_name="example_dag",
#         job_name="example_job",
#         status="COMPLETED",
#         duration=int((end_date - start_date).total_seconds() * 1000),
#         start_time=start_date,
#         end_time=end_date
#     )
#     saveBatchStatus(batch_status)

# if __name__ == "__main__":
#     main()
