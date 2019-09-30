from bs4 import BeautifulSoup
import requests
import pandas as pd
from time import sleep
from tqdm import tqdm_notebook
from tqdm import tqdm_gui


to_path = "D:/ML_Data/air/total/"
file_name = "IflightScheduleList"

api_key = "JkGavxVwFoxgxC3v6A01YkCXDBuD71WCG1UOzuXDsxnEvsBnl43EJzrQcaIAeCkXVj3xnJAH4wKgh9zdMieh%2Bw%3D%3D"
page_no = 1
url_format = "http://openapi.airport.co.kr/service/rest/FlightScheduleList/getIflightScheduleList?ServiceKey={api_key}&schDeptCityCode=ICN&schArrvCityCode=&pageNo={page_no}"
headers = {'content-type': 'application/json;charset=utf-8'}

IflightSchedule = {}
airlinekoreanlist, airportlist, citylist, internationalstdatelist, internationaleddatelist = [], [], [], [], []
internationalnumlist, internationaliotypelist, internationaltimelist = [], [], []
internationalmonlist, internationaltuelist, internationalwedlist, internationalthulist, internationalfrilist,\
                                                internationalsatlist, internationalsunlist = [], [], [], [], [],[], []

url = url_format.format(api_key=api_key, page_no=page_no)
response = requests.get(url, headers=headers)
html = response.text
soup = BeautifulSoup(html, 'html.parser')
rescode = response.status_code

# 제대로 데이터가 수신됐는지 확인하는 코드 성공시 200
if (rescode == 200):
    soup = BeautifulSoup(html, 'html.parser')
    total_count = int(soup.find("totalcount").text)
else : print("search_error")

# for page_no in tqdm_notebook(range(total_count), desc = 'page'):
for page_no in tqdm_gui(range(total_count), desc='page'):

    print(page_no)

    url = url_format.format(api_key=api_key, page_no=page_no+1)
    response = requests.get(url, headers=headers)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    rescode = response.status_code
    # 제대로 데이터가 수신됐는지 확인하는 코드 성공시 200

    if (rescode == 200):
        soup = BeautifulSoup(html, 'html.parser')

        airlinekorean = soup.find_all("airlinekorean")
        airport = soup.find_all("airport")
        city = soup.find_all("city")
        internationalstdate = soup.find_all("internationalstdate")
        internationaleddate = soup.find_all("internationaleddate")

        internationalnum = soup.find_all("internationalnum")
        internationaliotype = soup.find_all("internationaliotype")

        internationaltime = soup.find_all("internationaltime")
        internationalmon = soup.find_all("internationalmon")
        internationaltue = soup.find_all("internationaltue")
        internationalwed = soup.find_all("internationalwed")
        internationalthu = soup.find_all("internationalthu")
        internationalfri = soup.find_all("internationalfri")
        internationalsat = soup.find_all("internationalsat")
        internationalsun = soup.find_all("internationalsun")

        for item in airlinekorean:
            airlinekoreanlist.append(item.text)

        for item in airport:
            airportlist.append(item.text)

        for item in city:
            citylist.append(item.text)

        for item in internationalstdate:
            internationalstdatelist.append(item.text)

        for item in internationaleddate:
            internationaleddatelist.append(item.text)

        for item in internationalnum:
            internationalnumlist.append(item.text)

        for item in internationaliotype:
            internationaliotypelist.append(item.text)

        for item in internationaltime:
            internationaltimelist.append(item.text)

        for item in internationalmon:
            internationalmonlist.append(item.text)

        for item in internationaltue:
            internationaltuelist.append(item.text)

        for item in internationalwed:
            internationalwedlist.append(item.text)

        for item in internationalthu:
            internationalthulist.append(item.text)

        for item in internationalfri:
            internationalfrilist.append(item.text)

        for item in internationalsat:
            internationalsatlist.append(item.text)

        for item in internationalsun:
            internationalsunlist.append(item.text)

    else : break
    sleep(1.0)

IflightSchedule['airlinekorean'] = airlinekoreanlist
IflightSchedule['airport'] = airportlist
IflightSchedule['city'] = citylist

IflightSchedule['internationalstdate'] = internationalstdatelist
IflightSchedule['internationaleddate'] = internationaleddatelist

IflightSchedule['internationalnum'] = internationalnumlist
IflightSchedule['internationaliotype'] = internationaliotypelist
IflightSchedule['internationaltime'] = internationaltimelist
IflightSchedule['internationalmon'] = internationalmonlist
IflightSchedule['internationaltue'] = internationaltuelist
IflightSchedule['internationalwed'] = internationalwedlist
IflightSchedule['internationalthu'] = internationalthulist
IflightSchedule['internationalfri'] = internationalfrilist
IflightSchedule['internationalsat'] = internationalsatlist
IflightSchedule['internationalsun'] = internationalsunlist

df = pd.DataFrame(IflightSchedule)
df.to_json(to_path + file_name + ".json", index=False, orient='table')
df.to_csv(to_path + file_name + ".csv", index=False, encoding="euc-kr")
#
# # 칼럼순서변경
# # df = df[['Code','Name', 'Total Number','Male Total Number','Female Total Number','Age 10s Number','Age 20s Number','Age 30s Number','Age 40s Number','Age 50s Number','Age above 60s Number']]
#
# df = pd.read_json("D:/ML_Data/air/total/IflightScheduleList.json", orient='table')
# df.to_csv("D:/ML_Data/air/total/IflightScheduleList.csv", index=False, encoding="euc-kr")