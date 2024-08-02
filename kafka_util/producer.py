from kafka import KafkaProducer
import json

def sender(data):
    # Kafka 설정
    brokers = ['kafka1:19091', 'kafka2:19092', 'kafka3:19093']      # docker에서 실행
    # brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']  # 로컬에서 실행
    topic = 'save_exchange'

    producer = KafkaProducer(
        acks=1,
        compression_type='gzip',  # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
        bootstrap_servers=brokers,  # 전달하고자 하는 카프카 브로커의 주소 리스트
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
        batch_size=16384,  # 배치 크기 설정 (default 바이트 크기)
        linger_ms=100,  # 배치를 모으는 대기 시간
        max_in_flight_requests_per_connection=5,  # 동시에 처리할 수 있는 요청 수
        retries=3,  # 재시도 횟수
    )
    
    try:
        if isinstance(data, list):
            for item in data:
                producer.send(topic, value=item)
        else:
            producer.send(topic, value=data)
        print('[Data sent to Kafka]')
    except Exception as e:
        print(e)
    finally:
        producer.flush()


# from kafka import KafkaProducer
# import json
# import asyncio
# from concurrent.futures import ThreadPoolExecutor

# async def send_message(producer, topic, item):
#     future = producer.send(topic, value=item)
#     try:
#         record_metadata = await asyncio.to_thread(future.get)
#         print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
#     except Exception as e:
#         print(f"Error sending message: {e}")

# async def sender(data):
#     # Kafka 설정 (이전과 동일)
#     producer = KafkaProducer(
#         # 이전 설정과 동일
#     )
    
#     try:
#         if isinstance(data, list):
#             await asyncio.gather(*[send_message(producer, topic, item) for item in data])
#         else:
#             await send_message(producer, topic, data)
#         print('[Data sent to Kafka]')
#     except Exception as e:
#         print(e)
#     finally:
#         await asyncio.to_thread(producer.flush)

# # 실행
# if __name__ == "__main__":
#     data = [{"key": f"value{i}"} for i in range(10)]  # 예시 데이터
#     asyncio.run(sender(data))