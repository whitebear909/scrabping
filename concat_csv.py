import os
import pandas as pd

def concat_csv(path_dir):
    file_list = os.listdir(path_dir)  # path에 존재하는 파일 읽기
    file_list.sort()  # 파일 이름순서로 정렬

    for index, file in enumerate(file_list):
        path = path_dir + '{}'.format(file)
        df_temp = pd.read_csv(path, sep=',', encoding="euc-kr")
        if index == 0: df = df_temp.copy()
        else : df = pd.concat([df, df_temp])

    return df

if __name__ == '__main__':
    # directory 조회 및 파일 리스트 확보
    to_path = "D:/ML_Data/air/total/"
    path_dir = "D:/ML_Data/air/port/"
    file_name = 'air_port_supply'
    df = concat_csv(path_dir)
    df['from_port'] = 'RKSI'
    df.to_csv(to_path + file_name + "_total" + ".csv", index=False, encoding="euc-kr")
    df.to_json(to_path + file_name + "_total" + ".json", index=False, orient='table')
    # 도시별 항공 수요
    path_dir = "D:/ML_Data/air/city/"
    file_name = 'air_city_supply'
    df = concat_csv(path_dir)
    df.to_json(to_path + file_name + "_total" + ".json", index=False, orient='table')
    df.to_csv(to_path + file_name + "_total" + ".csv", index=False, encoding="euc-kr")

    df = pd.read_json(to_path + file_name + "_total" + ".json", orient='table')

    # df.to_csv(path_dir + file_name + "_total" + ".csv", index=False, encoding="euc-kr")


