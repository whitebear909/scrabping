from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from time import sleep
from tqdm import tqdm_notebook
from tqdm import tqdm_gui
import calendar

to_path = "D:/ML_Data/air/app/"
file_name = "air_schedule_app"

target_year = ["2010","2011","2012","2013","2014","2015","2016","2017", "2018", "2019"]
target_month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
target_date = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", \
               "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", \
               "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]

url_format = "http://www.airportal.go.kr/servlet/aips.mobile.MobileRbHanCTL?cmd=c_getList&index=0&count=500&depArr=D&current_date={search_date}&tm={search_hour}&airport=ICN"
# headers = {'content-type': 'application/json;charset=utf-8'}

for p_year in tqdm_gui(target_year, desc='target_year'):
    for p_month in tqdm_gui(target_month, desc='target_month'):
        last_day = calendar.monthrange(int(p_year), int(p_month))[1]  # 30
        for p_date in tqdm_gui(target_date, desc='target_date'):
            # 해당월의 마지막 날짜 체크
            if last_day < (int(p_date) + 1) : break
            search_date = p_year + p_month + p_date
            df = pd.DataFrame
            for p_hour in range(24):

                url = url_format.format(search_date=search_date, search_hour=p_hour)
                # response = requests.get(url, headers=headers)
                response = requests.get(url)
                rescode = response.status_code

                if (rescode == 200):
                    html = response.text
                    soup = BeautifulSoup(html, 'html.parser')

                    newDictionary = json.loads(str(soup))
                    df_temp = pd.DataFrame(newDictionary["result"])
                    if df_temp.empty: break

                    df_temp["search_date"] = search_date
                    df_temp["year"] = p_year
                    df_temp["month"] = p_month
                    df_temp["date"] = p_date
                    df_temp["hour"] = p_hour

                    if p_hour == 0 :
                        df = df_temp.copy()
                    else :
                        df = pd.concat([df, df_temp])
                        df = df.sort_values(by=['year', 'month','date', 'hour'])
                else:
                    print("search_date{}, search_hour={} search_error".format(search_date, p_hour))
                sleep(5)
            # df.to_json(to_path + file_name + ".json", index=False, orient='table')
            df = df[["search_date","year", "month", "date", "hour",
                         "company_id", "company_name", "flt_no", "aircraft_type",
                         "dli", "dln", "ali", "aln", "estimate_time", "arr_time",
                         "dep_time", "status", "type", "irr_title"
                         ]]

            df.to_csv(to_path + file_name + search_date + ".csv", index=False, encoding="euc-kr")
            print("search_date: {} complete".format(search_date))
# # 칼럼순서변경
# # df = df[['Code','Name', 'Total Number','Male Total Number','Female Total Number','Age 10s Number','Age 20s Number','Age 30s Number','Age 40s Number','Age 50s Number','Age above 60s Number']]
#
# df = pd.read_json("D:/ML_Data/air/total/IflightScheduleList.json", orient='table')
# df.to_csv("D:/ML_Data/air/total/IflightScheduleList.csv", index=False, encoding="euc-kr")