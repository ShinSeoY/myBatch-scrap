from kafka import KafkaProducer
from json import dumps
import time

isLocal = False
hostname = ''
if(isLocal):
    hostname = 'localhost'
else:
    hostname = '13.60.3.214'


brokers = [hostname + ':9091', hostname + ':9092', hostname + ':9093']
topic = 'my-topic'

producer = KafkaProducer(
    acks=1,
    compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
    bootstrap_servers=brokers, # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x:dumps(x).encode('utf-8') # 메시지의 값 직렬화
)

start = time.time()

for i in range(100):
    data = {'str' : 'result'+str(i)}
    producer.send(topic, value=data)
    producer.flush()
 
print('[Done Time]:', time.time()-start)