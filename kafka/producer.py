from kafka import KafkaProducer
from json import dumps
import requests
from bs4 import BeautifulSoup
import json

# Kafka 설정
brokers = ['kafka1:19091', 'kafka2:19092', 'kafka3:19093']
# brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']
topic = 'my-topic5'

producer = KafkaProducer(
    acks=1,
    compression_type='gzip',  # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
    bootstrap_servers=brokers,  # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x: dumps(x).encode('utf-8')  # 메시지의 값 직렬화
)

# 웹페이지에서 데이터 가져오기
def get_webpage(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to retrieve webpage. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during requests to {url}: {e}")
        return None

# JSON으로 데이터 추출
def extract_information(html_content):
    if html_content is None:
        return None
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tag = soup.find('script', id='__NEXT_DATA__')
        
        if script_tag:
            data = json.loads(script_tag.string)
            exchange_data = data['props']['pageProps']['dehydratedState']['queries'][2]['state']['data']['result']
            return exchange_data
    except Exception as e:
        print(f"Error during parsing: {e}")
        return None

# 메인 함수
def main():
    url = 'https://m.stock.naver.com/marketindex/home/exchangeRate/exchange'
    html_content = get_webpage(url)
    if html_content:
        extracted_data = extract_information(html_content)
        if extracted_data:
            print(extracted_data)
            producer.send(topic, value=extracted_data)
            producer.flush()
            print('[Data sent to Kafka]')
        else:
            print("Failed to extract data from the webpage.")
    else:
        print("Failed to retrieve the webpage.")

if __name__ == "__main__":
    main()
