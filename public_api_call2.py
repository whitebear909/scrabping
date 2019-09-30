from bs4 import BeautifulSoup
import requests
import pandas as pd
from time import sleep

to_path = "D:/ML_Data/air/total/"
file_name = "airport_code"

api_key = "JkGavxVwFoxgxC3v6A01YkCXDBuD71WCG1UOzuXDsxnEvsBnl43EJzrQcaIAeCkXVj3xnJAH4wKgh9zdMieh%2Bw%3D%3D"
page_no = 1
url_format = "http://openapi.airport.co.kr/service/rest/AirportCodeList/getAirportCodeList?ServiceKey={api_key}&pageNo={page_no}"
headers = {'content-type': 'application/json;charset=utf-8'}

airport_code = {}
cityenglist, citycodelist = [], []


while True:
    url = url_format.format(api_key=api_key, page_no=page_no)
    response = requests.get(url, headers=headers)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    rescode = response.status_code
    # 제대로 데이터가 수신됐는지 확인하는 코드 성공시 200

    if (rescode == 200):
        soup = BeautifulSoup(html, 'html.parser')
        cityengs = soup.find_all("cityeng")
        citycodes = soup.find_all("citycode")

        for cityeng in cityengs:
            cityenglist.append(cityeng.text)
        for citycode in citycodes:
            citycodelist.append(citycode.text)
    else : break
    page_no += 1
    sleep(0.3)

airport_code['eng'] = cityenglist
airport_code['code'] = citycodelist
df = pd.DataFrame(airport_code)
df.to_json(to_path + file_name + ".json", index=False, orient='table')

# 칼럼순서변경
# df = df[['Code','Name', 'Total Number','Male Total Number','Female Total Number','Age 10s Number','Age 20s Number','Age 30s Number','Age 40s Number','Age 50s Number','Age above 60s Number']]

