from kafka import KafkaProducer
import json

def sender(data):
    # Kafka 설정
    brokers = ['kafka1:19091', 'kafka2:19092', 'kafka3:19093']      # docker에서 실행
    # brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']  # 로컬에서 실행
    topic = 'my-topic5'

    producer = KafkaProducer(
        acks=1,
        compression_type='gzip',  # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
        bootstrap_servers=brokers,  # 전달하고자 하는 카프카 브로커의 주소 리스트
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
        batch_size=16384,  # 배치 크기 설정 바이트
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


