
# 数据处理模块
# 导入python包
import os
import time
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

# 测试数据
testdataPath = '../data/'
# testdataFilename = 'testdata.csv'
testdataFilename = 'test_app_freq.csv'
testFile = os.path.join(testdataPath, testdataFilename)

# 数据
def dataProcess(filename):
    with open(filename, 'r') as file:
        lineData = file.readline()
        # 统计样本数量
        i = 0
        # 定义样本个体的DataFrame数据结构
        df_data = DataFrame(columns=['mid', 'sex', 'age', 'income', 'province', 'city', 'l_province', 'l_city', 'l_area', 'app_freq', 'label_freq', 'op', 'm'])
        while lineData:
            print(lineData)
            print(type(lineData))
            listData = lineData.split(' ')
            print(type(listData))
            print(listData)
            # 去除空元素
            listData = [i for i in listData if i != '']
            print(listData)
            print(listData[0])
            df_data.loc[i] = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, '11', 12]
            df_data.loc[i]['mid'] = listData[0]
            df_data.loc[i]['sex'] = int(listData[1])
            df_data.loc[i]['age'] = int(listData[2])
            df_data.loc[i]['income'] = int(listData[3])
            df_data.loc[i]['province'] = int(listData[3])
            df_data.loc[i]['city'] = int(listData[3])
            df_data.loc[i]['l_province'] = int(listData[3])
            df_data.loc[i]['l_city'] = int(listData[3])
            df_data.loc[i]['l_area'] = int(listData[3])
            df_data.loc[i]['app_freq'] = int(listData[3])
            df_data.loc[i]['label_freq'] = int(listData[3])
            df_data.loc[i]['op'] = listData[-2]
            df_data.loc[i]['m'] = int(listData[-1])
            i += 1
            lineData = file.readline()

    print("样本数量: ", i)
    print(df_data)

    # if filename.split('.')[-1] == 'csv':
    #     df_data = pd.read_csv(filename, sep='')
    # elif filename.split('.')[-1] == 'xlsx':
    #     pass


if __name__ == '__main__':
    print(testFile)
    dataProcess(testFile)
    pass
