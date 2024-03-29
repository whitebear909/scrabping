# 용도 : 스마트 항공 스케줄 APP 데이터 추출
# 방법 : 1) APP의 Apk 파일 추출 후 안드로이드 스튜디오를 통한 접속 경로 확인
#        2) 접속경로에서 시간대별 데이터를 수집 후 일자별로 저장하는 스크랩핑 로직 개발
# 생성일자 : 2019/10/01

from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from time import sleep
# from tqdm import tqdm_notebook
from tqdm import tqdm_gui
import calendar
import os
import time

# Error log 경로 설정 및 파일명 설정
error_path = "D:/ML_Data/air/app/error/"
error_time = time.strftime('%Y%m%d_%H%M', time.localtime(time.time()))
error_file = error_path + "{}_error_log.txt".format(error_time)

# Main 경로 및 파일명 설정
to_path = "D:/ML_Data/air/app/"
file_name = "air_schedule_app"

# 제외 리스트 읽어오기
f = open(to_path+"empty/empty_list.txt", 'a+')
read = f.read()
f.close()
empty_list = read.split()

# 받은 제외처리를 위한 파일 리스트 추출
# 1) path에 존재하는 파일 읽기
# 2) 파일 이름순서로 정렬
file_list = os.listdir(to_path)
file_list.sort()
for index, file in enumerate(file_list):
    file_list[index] = file_list[index].replace("air_schedule_app","").replace(".csv","")

# Target 기간 설정
target_year = ["2010","2011","2012","2013","2014","2015","2016","2017", "2018", "2019"]
target_month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
target_date = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", \
               "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", \
               "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]

# Target url
url_format = "http://www.airportal.go.kr/servlet/aips.mobile.MobileRbHanCTL?cmd=c_getList&index=0&count=500&depArr=D&current_date={search_date}&tm={search_hour}&airport=ICN"
# headers = {'content-type': 'application/json;charset=utf-8'}

# ScrabpingMain Loop
for p_year in tqdm_gui(target_year, desc='target_year'):
    for p_month in target_month:
        # 종료 조건 추가 '2019년 9월'까지 데이터 수집
        if p_year == "2019" and p_month == "10" : break
        last_day = calendar.monthrange(int(p_year), int(p_month))[1]  # 30
        for p_date in target_date:
            # 해당월의 마지막 날짜 체크
            exception_tf = False
            if last_day < (int(p_date)) : break

            search_date = p_year + p_month + p_date

            if search_date in empty_list :
                print("search_date{} is no data".format(search_date))
                continue
            if search_date in file_list :
                print("search_date{} is already existed".format(search_date))
                continue


            df = pd.DataFrame
            for p_hour in range(24):
                url = url_format.format(search_date=search_date, search_hour=p_hour)
                try :
                    response = requests.get(url)
                    rescode = response.status_code
                    sleep(1.2)
                except Exception:
                    print("search_date{}, search_hour={} Response_error_code={}".format(search_date, p_hour, rescode))
                    exception_tf = True
                    break

                if (rescode == 200):
                    html = response.text
                    soup = BeautifulSoup(html, 'html.parser')

                    newDictionary = json.loads(str(soup))
                    df_temp = pd.DataFrame(newDictionary["result"])
                    if newDictionary["rcmsg"] == "데이터가 없습니다." :
                        continue
                    #데이터가 없으면 continue 로직 추가

                    df_temp["search_date"] = search_date
                    df_temp["year"] = p_year
                    df_temp["month"] = p_month
                    df_temp["date"] = p_date
                    df_temp["hour"] = p_hour

                    if df.empty :
                        df = df_temp.copy()
                    else :
                        df = pd.concat([df, df_temp])
                        df = df.sort_values(by=['year', 'month','date', 'hour'])
                elif (rescode == 404):
                    # 404 에러 발생 시 해당 시간 Pass 처리
                    error_message = "search_date{}, search_hour={} Response_error_code={}, Pass\n".format(search_date, p_hour, rescode)
                    error_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    print(error_message)
                    f = open(error_file, 'a+')
                    f.write(error_time+"\t"+error_message)
                    f.close()
                    continue
                else:
                    # 에러 발생 시 해당 일자 재처리 필요
                    print("search_date{}, search_hour={} Response_error_code={}".format(search_date, p_hour, rescode))
                    exception_tf = True

            if exception_tf: continue

            if df.empty:
                f = open(to_path + "empty/empty_list.txt", 'a+')
                f.write(search_date)
                f.close()
                continue

            df.to_csv(to_path + file_name + search_date + ".csv", index=False, encoding="euc-kr")
            print("search_date: {} complete".format(search_date))

            # df.to_json(to_path + file_name + ".json", index=False, orient='table')
            # df = df[["search_date","year", "month", "date", "hour",
            #              "company_id", "company_name", "flt_no", "aircraft_type",
            #              "dli", "dln", "ali", "aln", "estimate_time", "arr_time",
            #              "dep_time", "status", "type", "irr_title"
            #              ]]

 # # 칼럼순서변경
# # df = df[['Code','Name', 'Total Number','Male Total Number','Female Total Number','Age 10s Number','Age 20s Number','Age 30s Number','Age 40s Number','Age 50s Number','Age above 60s Number']]
#
# df = pd.read_json("D:/ML_Data/air/total/IflightScheduleList.json", orient='table')
# df.to_csv("D:/ML_Data/air/total/IflightScheduleList.csv", index=False, encoding="euc-kr")