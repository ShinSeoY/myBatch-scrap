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
        connections_max_idle_ms=60000,  # 60초
        request_timeout_ms=30000,  # 30초
        max_block_ms=60000,  # 60초
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

