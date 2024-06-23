# 지피티한테 이렇게 검색
# 만약 1분에 한번씩 스크랩핑을 해서 몽고디비에 저장한다고 했을 때 이 로직대로 예시 코드를 작성해줘 
# 1. 웹 스크랩핑으로 환율정보 실시간 수집
# 2. 이를 kafka로 전송 
# 3. kafka에서 수신한 데이터를 apache Spark Streaming을 사용하여 실시간으로 처리 spark를 통해 데이터 전처리 및 분석 수행
# 4. 처리된 데이터 mongoDB에 저장 (원시데이터는 s3 저장) => 데이터 레이크 구축

import requests
from bs4 import BeautifulSoup
import json

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

# json으로 데이터 추출
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

def main():
    url = 'https://m.stock.naver.com/marketindex/home/exchangeRate/exchange'
    html_content = get_webpage(url)
    if html_content:
        extracted_data = extract_information(html_content)
        if extracted_data:
            print(extracted_data)
        else:
            print("Failed to extract data from the webpage.")
    else:
        print("Failed to retrieve the webpage.")

if __name__ == "__main__":
    main()
