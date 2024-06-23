{
  "props": {
    "pageProps": {
      "userAgent": "python-requests/2.32.3",
      "_sentryTraceData": "62559f2fd292405181d2e1d2bc68232e-80d9444d41fd31cc-1",
      "_sentryBaggage": "sentry-environment=real,sentry-release=stock-web%402024.06.05,sentry-transaction=%2Fmarketindex%2Fhome%2FexchangeRate%2F%5BtabState%5D,sentry-public_key=eab941495e2a457dad78b49208e8db3f,sentry-trace_id=62559f2fd292405181d2e1d2bc68232e,sentry-sample_rate=1",
      "dehydratedState": {
        "mutations": [],
        "queries": [
          {
            "state": {
              "data": {
                "result": {
                  "userId": "",
                  "isLogin": false,
                  "isAgreementYn": ""
                }
              },
              "dataUpdateCount": 1,
              "dataUpdatedAt": 1718353066034,
              "error": "",
              "errorUpdateCount": 0,
              "errorUpdatedAt": 0,
              "fetchFailureCount": 0,
              "fetchFailureReason": "",
              "fetchMeta": "",
              "isInvalidated": false,
              "status": "success",
              "fetchStatus": "idle"
            },
            "queryKey": [{ "url": "/auth/userInfo", "method": "get" }],
            "queryHash": "[{\"method\":\"get\",\"url\":\"/auth/userInfo\"}]"
          },
          {
            "state": {
              "data": {
                "isSuccess": true,
                "detailCode": "",
                "message": "",
                "result": [
                  {
                    "name": "미국 USD",
                    "closePrice": "1,381.50",
                    "fluctuations": "3.50",
                    "fluctuationsType": {
                      "code": "2",
                      "text": "상승",
                      "name": "RISING"
                    },
                    "fluctuationsRatio": "0.25",
                    "localTradedAt": "2024-06-14T17:16:26+09:00",
                    "marketStatus": "OPEN",
                    "reutersCode": "FX_USDKRW",
                    "priceDataType": "NOTICE_ROUND",
                    "unit": "KRW",
                    "exchangeCode": "USD",
                    "categoryType": "exchange",
                    "symbolCode": "USD"
                  },
                  {
                    "name": "유럽 EUR",
                    "closePrice": "1,479.45",
                    "fluctuations": "-0.52",
                    "fluctuationsType": {
                      "code": "5",
                      "text": "하락",
                      "name": "FALLING"
                    },
                    "fluctuationsRatio": "-0.04",
                    "localTradedAt": "2024-06-14T17:16:26+09:00",
                    "marketStatus": "OPEN",
                    "reutersCode": "FX_EURKRW",
                    "priceDataType": "NOTICE_ROUND",
                    "unit": "KRW",
                    "exchangeCode": "EUR",
                    "categoryType": "exchange",
                    "symbolCode": "EUR"
                  },
                  {
                    "name": "일본 JPY",
                    "closePrice": "876.89",
                    "fluctuations": "-0.59",
                    "fluctuationsType": {
                      "code": "5",
                      "text": "하락",
                      "name": "FALLING"
                    },
                    "fluctuationsRatio": "-0.07",
                    "localTradedAt": "2024-06-14T17:16:26+09:00",
                    "marketStatus": "OPEN",
                    "reutersCode": "FX_JPYKRW",
                    "priceDataType": "NOTICE_ROUND",
                    "unit": "KRW",
                    "exchangeCode": "JPY",
                    "categoryType": "exchange",
                    "symbolCode": "JPY"
                  },
                  {
                    "name": "중국 CNY",
                    "closePrice": "190.01",
                    "fluctuations": "0.48",
                    "fluctuationsType": {
                      "code": "2",
                      "text": "상승",
                      "name": "RISING"
                    },
                    "fluctuationsRatio": "0.25",
                    "localTradedAt": "2024-06-14T17:16:26+09:00",
                    "marketStatus": "OPEN",
                    "reutersCode": "FX_CNYKRW",
                    "priceDataType": "NOTICE_ROUND",
                    "unit": "KRW",
                    "exchangeCode": "CNY",
                    "categoryType": "exchange",
                    "symbolCode": "CNY"
                  },
                  {
                    "name": "유로/달러",
                    "closePrice": "1.0738",
                    "fluctuations": "-0.0070",
                    "fluctuationsType": {
                      "code": "5",
                      "text": "하락",
                      "name": "FALLING"
                    },
                    "fluctuationsRatio": "-0.65",
                    "localTradedAt": "2024-06-13T00:00:00+09:00",
                    "marketStatus": "",
                    "delayTimeName": "",
                    "reutersCode": "EURUSD",
                    "month": "",
                    "priceDataType": "CLOSING_PRICE",
                    "delayTime": "",
                    "unit": "USD",
                    "nationType": "",
                    "categoryType": "exchangeWorld",
                    "symbolCode": "EURUSD"
                  },
                  {
                    "name": "영국 파운드/달러",
                    "closePrice": "1.2759",
                    "fluctuations": "-0.0038",
                    "fluctuationsType": {
                      "code": "5",
                      "text": "하락",
                      "name": "FALLING"
                    },
                    "fluctuationsRatio": "-0.30",
                    "localTradedAt": "2024-06-13T00:00:00+09:00",
                    "marketStatus": "",
                    "delayTimeName": "",
                    "reutersCode": "GBPUSD",
                    "month": "",
                    "priceDataType": "CLOSING_PRICE",
                    "delayTime": "",
                    "unit": "USD",
                    "nationType": "",
                    "categoryType": "exchangeWorld",
                    "symbolCode": "GBPUSD"
                  }
                ]
              },
              "dataUpdateCount": 1,
              "dataUpdatedAt": 1718353066055,
              "error": "",
              "errorUpdateCount": 0,
              "errorUpdatedAt": 0,
              "fetchFailureCount": 0,
              "fetchFailureReason": "",
              "fetchMeta": "",
              "isInvalidated": false,
              "status": "success",
              "fetchStatus": "idle"
            },
            "queryKey": [
              { "url": "/marketIndex/exchange/main", "method": "get" }
            ],
            "queryHash": "[{\"method\":\"get\",\"url\":\"/marketIndex/exchange/main\"}]"
          },
          {
            "state": {
              "data": {
                "isSuccess": true,
                "detailCode": "",
                "message": "",
                "result": {
                  "KRW": {
                    "countryName": "대한민국",
                    "exchangeCode": "KRW",
                    "name": "대한민국 KRW",
                    "calcPrice": "1",
                    "currencyName": "원"
                  },
                  "ZAR": {
                    "countryName": "남아프리카",
                    "currencyName": "랜드",
                    "exchangeCode": "ZAR",
                    "name": "남아프리카 ZAR",
                    "calcPrice": "75.29"
                  },
                  "NPR": {
                    "countryName": "네팔",
                    "currencyName": "네팔 루피",
                    "exchangeCode": "NPR",
                    "name": "네팔 NPR",
                    "calcPrice": "10.34"
                  },
                  "NOK": {
                    "countryName": "노르웨이",
                    "currencyName": "크로네",
                    "exchangeCode": "NOK",
                    "name": "노르웨이 NOK",
                    "calcPrice": "129.41"
                  },
                  "NZD": {
                    "countryName": "뉴질랜드",
                    "currencyName": "달러",
                    "exchangeCode": "NZD",
                    "name": "뉴질랜드 NZD",
                    "calcPrice": "848.66"
                  },
                  "TWD": {
                    "countryName": "대만",
                    "currencyName": "달러",
                    "exchangeCode": "TWD",
                    "name": "대만 TWD",
                    "calcPrice": "42.71"
                  },
                  "DKK": {
                    "countryName": "덴마크",
                    "currencyName": "크로네",
                    "exchangeCode": "DKK",
                    "name": "덴마크 DKK",
                    "calcPrice": "198.3"
                  },
                  "RUB": {
                    "countryName": "러시아",
                    "currencyName": "루블",
                    "exchangeCode": "RUB",
                    "name": "러시아 RUB",
                    "calcPrice": "15.49"
                  },
                  "RON": {
                    "countryName": "루마니아",
                    "currencyName": "레우",
                    "exchangeCode": "RON",
                    "name": "루마니아 RON",
                    "calcPrice": "297.24"
                  },
                  "LYD": {
                    "countryName": "리비아",
                    "currencyName": "리비아 디나르",
                    "exchangeCode": "LYD",
                    "name": "리비아 LYD",
                    "calcPrice": "284.94"
                  },
                  "MOP": {
                    "countryName": "마카오",
                    "currencyName": "파타카",
                    "exchangeCode": "MOP",
                    "name": "마카오 MOP",
                    "calcPrice": "171.66"
                  },
                  "MYR": {
                    "countryName": "말레이시아",
                    "currencyName": "링깃",
                    "exchangeCode": "MYR",
                    "name": "말레이시아 MYR",
                    "calcPrice": "292.91"
                  },
                  "MXN": {
                    "countryName": "멕시코",
                    "currencyName": "페소",
                    "exchangeCode": "MXN",
                    "name": "멕시코 MXN",
                    "calcPrice": "74.39"
                  },
                  "MNT": {
                    "countryName": "몽골",
                    "currencyName": "투그릭",
                    "exchangeCode": "MNT",
                    "name": "몽골 MNT",
                    "calcPrice": "0.41"
                  },
                  "USD": {
                    "countryName": "미국",
                    "currencyName": "달러",
                    "exchangeCode": "USD",
                    "name": "미국 USD",
                    "calcPrice": "1381.5"
                  },
                  "MMK": {
                    "countryName": "미얀마",
                    "currencyName": "짯",
                    "exchangeCode": "MMK",
                    "name": "미얀마 MMK",
                    "calcPrice": "0.66"
                  },
                  "BHD": {
                    "countryName": "바레인",
                    "currencyName": "디나르",
                    "exchangeCode": "BHD",
                    "name": "바레인 BHD",
                    "calcPrice": "3665.04"
                  },
                  "BDT": {
                    "countryName": "방글라데시",
                    "currencyName": "타카",
                    "exchangeCode": "BDT",
                    "name": "방글라데시 BDT",
                    "calcPrice": "11.76"
                  },
                  "VND": {
                    "countryName": "베트남",
                    "currencyName": "동",
                    "exchangeCode": "VND",
                    "name": "베트남 VND",
                    "calcPrice": "0.054299999999999994"
                  },
                  "BRL": {
                    "countryName": "브라질",
                    "currencyName": "레알",
                    "exchangeCode": "BRL",
                    "name": "브라질 BRL",
                    "calcPrice": "257.58"
                  },
                  "BND": {
                    "countryName": "브루나이",
                    "currencyName": "달러",
                    "exchangeCode": "BND",
                    "name": "브루나이 BND",
                    "calcPrice": "1021.25"
                  },
                  "SAR": {
                    "countryName": "사우디",
                    "currencyName": "리얄",
                    "exchangeCode": "SAR",
                    "name": "사우디 SAR",
                    "calcPrice": "368.22"
                  },
                  "LKR": {
                    "countryName": "스리랑카",
                    "currencyName": "스리랑카 루피",
                    "exchangeCode": "LKR",
                    "name": "스리랑카 LKR",
                    "calcPrice": "4.55"
                  },
                  "SEK": {
                    "countryName": "스웨덴",
                    "currencyName": "크로나",
                    "exchangeCode": "SEK",
                    "name": "스웨덴 SEK",
                    "calcPrice": "131.28"
                  },
                  "CHF": {
                    "countryName": "스위스",
                    "currencyName": "프랑",
                    "exchangeCode": "CHF",
                    "name": "스위스 CHF",
                    "calcPrice": "1547.29"
                  },
                  "SGD": {
                    "countryName": "싱가포르",
                    "currencyName": "달러",
                    "exchangeCode": "SGD",
                    "name": "싱가포르 SGD",
                    "calcPrice": "1021.25"
                  },
                  "AED": {
                    "countryName": "아랍에미리트",
                    "currencyName": "디르함",
                    "exchangeCode": "AED",
                    "name": "아랍에미리트 AED",
                    "calcPrice": "376.11"
                  },
                  "DZD": {
                    "countryName": "알제리",
                    "currencyName": "알제리 디나르",
                    "exchangeCode": "DZD",
                    "name": "알제리 DZD",
                    "calcPrice": "10.24"
                  },
                  "ETB": {
                    "countryName": "에티오피아",
                    "currencyName": "비르",
                    "exchangeCode": "ETB",
                    "name": "에티오피아 ETB",
                    "calcPrice": "23.96"
                  },
                  "GBP": {
                    "countryName": "영국",
                    "currencyName": "파운드",
                    "exchangeCode": "GBP",
                    "name": "영국 GBP",
                    "calcPrice": "1758.65"
                  },
                  "OMR": {
                    "countryName": "오만",
                    "currencyName": "리알",
                    "exchangeCode": "OMR",
                    "name": "오만 OMR",
                    "calcPrice": "3588.4"
                  },
                  "JOD": {
                    "countryName": "요르단",
                    "currencyName": "디나르",
                    "exchangeCode": "JOD",
                    "name": "요르단 JOD",
                    "calcPrice": "1948.52"
                  },
                  "UZS": {
                    "countryName": "우즈베키스탄",
                    "currencyName": "숨",
                    "exchangeCode": "UZS",
                    "name": "우즈베키스탄 UZS",
                    "calcPrice": "0.11"
                  },
                  "EUR": {
                    "countryName": "유럽",
                    "currencyName": "유로",
                    "exchangeCode": "EUR",
                    "name": "유럽 EUR",
                    "calcPrice": "1479.45"
                  },
                  "ILS": {
                    "countryName": "이스라엘",
                    "currencyName": "세켈",
                    "exchangeCode": "ILS",
                    "name": "이스라엘 ILS",
                    "calcPrice": "371.32"
                  },
                  "EGP": {
                    "countryName": "이집트",
                    "currencyName": "파운드",
                    "exchangeCode": "EGP",
                    "name": "이집트 EGP",
                    "calcPrice": "28.96"
                  },
                  "INR": {
                    "countryName": "인도",
                    "currencyName": "루피",
                    "exchangeCode": "INR",
                    "name": "인도 INR",
                    "calcPrice": "16.54"
                  },
                  "IDR": {
                    "countryName": "인도네시아",
                    "currencyName": "루피아",
                    "exchangeCode": "IDR",
                    "name": "인도네시아 IDR",
                    "calcPrice": "0.08439999999999999"
                  },
                  "JPY": {
                    "countryName": "일본",
                    "currencyName": "엔",
                    "exchangeCode": "JPY",
                    "name": "일본 JPY",
                    "calcPrice": "8.7689"
                  },
                  "CNY": {
                    "countryName": "중국",
                    "currencyName": "위안",
                    "exchangeCode": "CNY",
                    "name": "중국 CNY",
                    "calcPrice": "190.01"
                  },
                  "CZK": {
                    "countryName": "체코",
                    "currencyName": "코루나",
                    "exchangeCode": "CZK",
                    "name": "체코 CZK",
                    "calcPrice": "59.87"
                  },
                  "CLP": {
                    "countryName": "칠레",
                    "currencyName": "페소",
                    "exchangeCode": "CLP",
                    "name": "칠레 CLP",
                    "calcPrice": "1.51"
                  },
                  "KZT": {
                    "countryName": "카자흐스탄",
                    "currencyName": "텡게",
                    "exchangeCode": "KZT",
                    "name": "카자흐스탄 KZT",
                    "calcPrice": "3.06"
                  },
                  "QAR": {
                    "countryName": "카타르",
                    "currencyName": "리얄",
                    "exchangeCode": "QAR",
                    "name": "카타르 QAR",
                    "calcPrice": "378.88"
                  },
                  "KHR": {
                    "countryName": "캄보디아",
                    "currencyName": "리엘",
                    "exchangeCode": "KHR",
                    "name": "캄보디아 KHR",
                    "calcPrice": "0.34"
                  },
                  "CAD": {
                    "countryName": "캐나다",
                    "currencyName": "달러",
                    "exchangeCode": "CAD",
                    "name": "캐나다 CAD",
                    "calcPrice": "1004.87"
                  },
                  "KES": {
                    "countryName": "케냐",
                    "currencyName": "실링",
                    "exchangeCode": "KES",
                    "name": "케냐 KES",
                    "calcPrice": "10.73"
                  },
                  "COP": {
                    "countryName": "콜롬비아",
                    "currencyName": "페소",
                    "exchangeCode": "COP",
                    "name": "콜롬비아 COP",
                    "calcPrice": "0.33"
                  },
                  "KWD": {
                    "countryName": "쿠웨이트",
                    "currencyName": "디나르",
                    "exchangeCode": "KWD",
                    "name": "쿠웨이트 KWD",
                    "calcPrice": "4505.28"
                  },
                  "TZS": {
                    "countryName": "탄자니아",
                    "currencyName": "실링",
                    "exchangeCode": "TZS",
                    "name": "탄자니아 TZS",
                    "calcPrice": "0.53"
                  },
                  "THB": {
                    "countryName": "태국",
                    "currencyName": "바트",
                    "exchangeCode": "THB",
                    "name": "태국 THB",
                    "calcPrice": "37.6"
                  },
                  "TRY": {
                    "countryName": "튀르키예",
                    "currencyName": "뉴리라",
                    "exchangeCode": "TRY",
                    "name": "튀르키예 TRY",
                    "calcPrice": "42.32"
                  },
                  "PKR": {
                    "countryName": "파키스탄",
                    "currencyName": "루피",
                    "exchangeCode": "PKR",
                    "name": "파키스탄 PKR",
                    "calcPrice": "4.96"
                  },
                  "PLN": {
                    "countryName": "폴란드",
                    "currencyName": "즈워티",
                    "exchangeCode": "PLN",
                    "name": "폴란드 PLN",
                    "calcPrice": "339.75"
                  },
                  "FJD": {
                    "countryName": "피지",
                    "currencyName": "피지 달러",
                    "exchangeCode": "FJD",
                    "name": "피지 FJD",
                    "calcPrice": "617.25"
                  },
                  "PHP": {
                    "countryName": "필리핀",
                    "currencyName": "페소",
                    "exchangeCode": "PHP",
                    "name": "필리핀 PHP",
                    "calcPrice": "23.56"
                  },
                  "HUF": {
                    "countryName": "헝가리",
                    "currencyName": "포린트",
                    "exchangeCode": "HUF",
                    "name": "헝가리 HUF",
                    "calcPrice": "3.73"
                  },
                  "AUD": {
                    "countryName": "호주",
                    "currencyName": "달러",
                    "exchangeCode": "AUD",
                    "name": "호주 AUD",
                    "calcPrice": "914.41"
                  },
                  "HKD": {
                    "countryName": "홍콩",
                    "currencyName": "달러",
                    "exchangeCode": "HKD",
                    "name": "홍콩 HKD",
                    "calcPrice": "176.89"
                  }
                }
              },
              "dataUpdateCount": 1,
              "dataUpdatedAt": 1718353066067,
              "error": "",
              "errorUpdateCount": 0,
              "errorUpdatedAt": 0,
              "fetchFailureCount": 0,
              "fetchFailureReason": "",
              "fetchMeta": "",
              "isInvalidated": false,
              "status": "success",
              "fetchStatus": "idle"
            },
            "queryKey": [
              {
                "url": "/marketIndex/exchange/bank",
                "method": "get",
                "params": { "bankType": "HNB" }
              }
            ],
            "queryHash": "[{\"method\":\"get\",\"params\":{\"bankType\":\"HNB\"},\"url\":\"/marketIndex/exchange/bank\"}]"
          }
        ]
      }
    },
    "__N_SSP": true
  },
  "page": "/marketindex/home/exchangeRate/[tabState]",
  "query": { "tabState": "exchange" },
  "buildId": "oLgGk4Mqt2_8_UqlKy51W",
  "assetPrefix": "https://ssl.pstatic.net/imgstock/fn/real",
  "runtimeConfig": {
    "URL": {
      "DTHUMB": "https://phinf.pstatic.net/dthumb/",
      "NAVER": "https://m.naver.com",
      "STOCK": "https://m.stock.naver.com",
      "PAY": "https://new-m.pay.naver.com",
      "NID": "https://nid.naver.com",
      "SRP": "https://srp2.naver.com",
      "COMMON_COMMENT": {
        "RESOURCE": "https://ssl.pstatic.net/static.cbox",
        "DISCUSS": "https://apis.naver.com/commentBox/cbox12",
        "NEWS": "https://apis.naver.com/commentBox/cbox5"
      },
      "NAVER_ECONOMIC": "https://m.naver.com/naverapp/?cmd=onMenu&menuCode=ECONOMIC-INDEX&showToast=false",
      "MY_ASSET": "https://new-m.pay.naver.com",
      "KEYPAD": "https://new-m.pay.naver.com/auth/check",
      "REAL_ESTATE": "https://m.land.naver.com",
      "NAVER_NEWS": "https://n.news.naver.com/article"
    },
    "GLAD_SDK": "https://ssl.pstatic.net/tveta/libs/glad/prod/gfp-core.js",
    "ASSETS_PREFIX": "https://ssl.pstatic.net/imgstock/fn/real",
    "PROFILE": "real"
  },
  "isFallback": false,
  "gssp": true,
  "customServer": true,
  "appGip": true,
  "scriptLoader": []
}
