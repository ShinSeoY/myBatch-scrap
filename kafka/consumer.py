from kafka import KafkaConsumer
from json import loads

isLocal = False
hostname = ''
if(isLocal):
    hostname = 'localhost'
else:
    hostname = '13.60.3.214'


brokers = [hostname + ':9091', hostname + ':9092', hostname + ':9093']
topic = 'my-topic'

consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

consumer = KafkaConsumer(
    topic, # 토픽명
    bootstrap_servers=brokers, # 카프카 브로커 주소 리스트
    auto_offset_reset='earliest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
    enable_auto_commit=True, # 오프셋 자동 커밋 여부
    # group_id='test-group', # 컨슈머 그룹 식별자
    value_deserializer=lambda x: loads(x.decode('utf-8')), # 메시지의 값 역직렬화
    consumer_timeout_ms=1000 # 데이터를 기다리는 최대 시간
)

for message in consumer:
    print(message)