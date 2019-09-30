import os
import sys
import urllib.request
import pandas as pd
import numpy as np
import re
from tqdm import tqdm_notebook

import pdb
import time
from bs4 import BeautifulSoup

FILE_NAME = 'AIR_city_supply'

data1 = "command=LIST&PAGE_CUR=1&S_STARTDATE="
data2 = "&S_ENDDATE="
data3 = "&AIRPORT_NATION="

url = "http://www.nlic.go.kr/nlic/frghtAirplane0020.action"

def get_data(url, data):
    request = urllib.request.Request(url)
    # request.add_header("X-Naver-Client-Id", client_id)
    # request.add_header("X-Naver-Client-Secret", client_secret)
    response = urllib.request.urlopen(request, data=data.encode('utf-8'))
    rescode = response.getcode()
    response_body = response.read()
    soup = BeautifulSoup(response_body, 'html.parser')
    return soup

def fine_lastday(month):
    return {"02" : "28", "04" : "30", "06" : "30", "09" : "30", "11" : "30"}.get(month,"31")


def airsupply_Crawling(year, month, html):
    #     pdb.set_trace()
    temp_list = []
    index = 0
    nation = ""
    port_name = ""

    for row in html.select('td'):
        tr = row.text

        if index == 0 and str(row).find('rowspan="3"') > 0:
            temp1 = tr
            index += 1
        elif index == 2 and str(row).find('rowspan="3"') > 0:
            nation = temp1
            index = 1
            temp1 = ""

        #         print(index, tr, str(row).find('rowspan="3"'), row)
        tr = re.sub(',', '', tr)
        #         rank = int(tr.find('td',{'class':'MMLItemRank'}).find('span').text.strip('위'))
        if index == 0:
            nation = tr
            index += 1
        elif index == 1:
            port_name = tr
            index += 1
        elif index == 2:
            in_out = tr
            index += 1
        elif index == 3:
            flights = int(tr)
            index += 1
        elif index == 4:
            regular_freight = int(tr)
            index += 1
        elif index == 5:
            regular_luggage = int(tr)
            index += 1
        elif index == 6:
            regular_mail = int(tr)
            index += 1
        elif index == 7:
            occasional_freight = int(tr)
            index += 1
        elif index == 8:
            occasional_luggage = int(tr)
            index += 1
        elif index == 9:
            occasional_mail = int(tr)
            if in_out == "계":
                index = 0
            else:
                index = 2
            temp_list.append([year, month, nation, port_name, in_out,
                              flights, regular_freight, regular_luggage, regular_mail,
                              occasional_freight, occasional_luggage, occasional_mail
                              ])
    return temp_list

# 출처: https://db-log.tistory.com/entry/31-크롤링한-데이터를-리스트화-List-사전화-Dict?category=766620 [떡빵로그]

# current_dt_from


# current_dt_to

# target_year = ["2010","2011","2012","2013","2014","2015","2016","2017", "2018", "2019"]
# '12년 3월부터 '12년 7월까지 데이터 누락
# '14년 5월 데이터 누락

target_year = ["2012", "2014"]

target_month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]




for row_year in tqdm_notebook(target_year, desc='year'):
    for row_month in tqdm_notebook(target_month, desc='month'):
        data_list = []
        current_dt_from = row_year + "-" + row_month + "-" + "01"
        current_dt_to = row_year + "-" + row_month + "-" + fine_lastday(row_month)
        if row_year == "2019" and row_month in ("09", "10", "11", "12"): continue

        data = data1 + current_dt_from + data2 + current_dt_to + data3
        soup = get_data(url, data)
        temp = soup.find('div', {'id': 'listWrap'})
        data_list = airsupply_Crawling(row_year, row_month, temp)

        df = pd.DataFrame(data_list)

        print(row_year + row_month + "쓰기 시작")
        #         pdb.set_trace()

        df_header = ["year", "month", "nation", "port_name", "in_out",
                     "flights", "regular_freight", "regular_luggage", "regular_mail",
                     "occasional_freight", "occasional_luggage", "occasional_mail"
                     ]
        try:
            df.to_csv("D:/ML_Data/" + FILE_NAME + row_year + row_month + ".csv", header=df_header, index=False,
                      encoding="euc-kr")
            time.sleep(10)
            print(row_year + row_month + "쓰기 종료")
        except:
            print(row_year + "-" + row_month + "데이터 오류")
            continue