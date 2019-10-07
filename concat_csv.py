import os
import pandas as pd
from tqdm import tqdm_gui

TARGET_ALL = 0
TARGET_CITY = 1
TARGET_PORT = 2
TARGET_APP = 3
TARGET = TARGET_APP

def make_file(to_path, file_name, source_path, target):
    df = concat_csv(source_path)
    if target == TARGET_PORT :
        df['from_port'] = 'RKSI'
    df.to_csv(to_path + file_name + "_total" + ".csv", index=False, encoding="euc-kr")
    df.to_json(to_path + file_name + "_total" + ".json", index=False, orient='table')
    return df

def concat_csv(path_dir):
    file_list = os.listdir(path_dir)  # path에 존재하는 파일 읽기
    file_list.sort()  # 파일 이름순서로 정렬

    for i in tqdm_gui(range(len(file_list)), desc='Merging progress'):
        file = file_list[i]
        path = path_dir + '{}'.format(file)
        df_temp = pd.read_csv(path, sep=',', encoding="euc-kr")
        if i == 0:
            df = df_temp.copy()
        else :
            df = pd.concat([df, df_temp], axis = 0, sort=False)

    return df

if __name__ == '__main__':
    to_path = "D:/ML_Data/air/total/"

    if TARGET == TARGET_ALL :
        source_path = "D:/ML_Data/air/port/"
        file_name = 'air_port_supply'
        make_file(to_path, file_name, source_path, TARGET_CITY)

        source_path = "D:/ML_Data/air/city/"
        file_name = 'air_city_supply'
        make_file(to_path, file_name, source_path, TARGET_PORT)

        source_path = "D:/ML_Data/air/app/"
        file_name = 'air_schedule_app'
        make_file(to_path, file_name, source_path, TARGET_APP)
    elif TARGET == TARGET_PORT:
        # 포트별 항공 수요
        source_path = "D:/ML_Data/air/port/"
        file_name = 'air_port_supply'
        make_file(to_path, file_name, source_path, TARGET_CITY)
    elif TARGET == TARGET_CITY:
        # 도시별 항공 수요
        source_path = "D:/ML_Data/air/city/"
        file_name = 'air_city_supply'
        make_file(to_path, file_name, source_path, TARGET_PORT)
    elif TARGET == TARGET_APP:
        # 항공 스케줄 공급
        source_path = "D:/ML_Data/air/app/data/"
        file_name = 'air_schedule_app'
        df = make_file(to_path, file_name, source_path, TARGET_APP)
        # df = pd.read_json(to_path + file_name + "_total" + ".json", orient='table')



