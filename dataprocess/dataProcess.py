
# 数据处理模块
# 导入python包
import os
import time
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

# 测试数据
testdataPath = '../data/'
testdataFilename = 'testdata.csv'
outdataPath = '../data/'
outdataFilename_excel = 'outdata.xlsx'
outdataFilename_csv = 'outdata.csv'
testFile = os.path.join(testdataPath, testdataFilename)
outFile_excel = os.path.join(outdataPath, outdataFilename_excel)
outFile_csv = os.path.join(outdataPath, outdataFilename_csv)

# 数据预处理函数，生成DataFrame类型数据
def dataProcess(filename):
    with open(filename, 'r') as file:
        lineData = file.readline()
        # 统计样本数量
        i = 0
        ############# [string, int,  int,   int ,     string,      string, string,      string,    string,  string,     string,    string, int]
        columnslist = ['mid', 'sex', 'age', 'income', 'province', 'city', 'l_province', 'l_city', 'l_area', 'app_freq', 'label_freq', 'op', 'm']
        df_data = DataFrame(columns=columnslist)
        # df_data的shape
        # print(df_data.shape)
        while lineData:
            listData = lineData.split(' ')
            # 去除空元素
            listData = [i for i in listData if i != '']
            # 判断是否包含app使用信息，若缺失，则删去该样本
            # app_freq 中值为字符串类型 '{"3004":{"daycount":1,"pv":3,"freq":3}'
            # 可使用 eval()返回传入字符串的表达式的结果
            if len(listData[-3]) > 36:
                df_data.loc[i, 'label_freq'] = listData[-3]
                df_data.loc[i, 'app_freq'] = listData[-4]
                # province city l_province l_city l_area 字段全缺失
                if len(listData[-5]) == 1:
                    df_data.loc[i]['province'] = None
                    df_data.loc[i]['city'] = None
                    df_data.loc[i]['l_province'] = None
                    df_data.loc[i]['l_city'] = None
                    df_data.loc[i]['l_area'] = None
                # province city 字段缺失
                elif len(listData) == 11:
                    df_data.loc[i]['province'] = None
                    df_data.loc[i]['city'] = None
                    df_data.loc[i]['l_province'] = listData[-7]
                    df_data.loc[i]['l_city'] = listData[-6]
                    df_data.loc[i]['l_area'] = listData[-5]
                # l_province l_city l_area 字段全缺失
                elif len(listData) == 10:
                    df_data.loc[i]['province'] = listData[4]
                    df_data.loc[i]['city'] = listData[5]
                    df_data.loc[i]['l_province'] = None
                    df_data.loc[i]['l_city'] = None
                    df_data.loc[i]['l_area'] = None
                # province city l_province l_city l_area 字段均不缺失
                else:
                    df_data.loc[i]['province'] = listData[4]
                    df_data.loc[i]['city'] = listData[5]
                    df_data.loc[i]['l_province'] = listData[6]
                    df_data.loc[i]['l_city'] = listData[7]
                    df_data.loc[i]['l_area'] = listData[8]
            else:
                i += 1
                lineData = file.readline()
                continue
            df_data.loc[i, 'mid'] = listData[0]
            df_data.loc[i, 'sex'] = int(listData[1])
            df_data.loc[i]['age'] = int(listData[2])
            df_data.loc[i]['income'] = int(listData[3])
            df_data.loc[i]['op'] = listData[-2]
            df_data.loc[i]['m'] = int(listData[-1])
            i += 1
            lineData = file.readline()

    # 重置df_data中的index
    df_data.index = range(1, len(df_data)+1)
    # 样本数量j
    j = len(df_data)
    # print("样本数量: ", j)
    # df_data 展示
    # print('df_data:\n', df_data)
    # df_data 中列名展示
    # print('df_data ', df_data.columns)

    # if filename.split('.')[-1] == 'csv':
    #     df_data = pd.read_csv(filename, sep='')
    # elif filename.split('.')[-1] == 'xlsx':
    #     pass

    # 删除已存在文件
    if os.path.exists(outFile_excel):
        os.remove(outFile_excel)
    if os.path.exists(outFile_csv):
        os.remove(outFile_csv)

    # 输出为excel格式
    df_data.to_excel(outFile_excel, sheet_name='outData')
    # 输出为csv格式
    df_data.to_csv(outFile_csv, encoding='utf-8', )

if __name__ == '__main__':
    dataProcess(testFile)
