import requests
from bs4 import BeautifulSoup
import json

url = 'https://m.stock.naver.com/marketindex/home/exchangeRate/exchange'

# 웹페이지에서 데이터 가져오기
def get_webpage():
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

# def main():
#     url = 'https://m.stock.naver.com/marketindex/home/exchangeRate/exchange'
#     html_content = get_webpage(url)
#     if html_content:
#         extracted_data = extract_information(html_content)
#         if extracted_data:
#             print(extracted_data)
#         else:
#             print("Failed to extract data from the webpage.")
#     else:
#         print("Failed to retrieve the webpage.")

# if __name__ == "__main__":
#     main()
