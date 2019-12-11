
import pandas as pd
from pandas import Series, DataFrame
import os

# 读取df_data 数据表，传入mid参数，返回相应的app列表
def user2app():


    pass


# 读取 appoutdata 数据表，生成mid～app相对应的文件
# inputdata 输入数据路径及相应文件
# outputpath 生成文件路径
def user2app1(inputdata, outputPth):
    if inputdata.split('.')[-1] == 'csv':
        df_inputdata = pd.read_csv(inputdata, index_col=0)
        columnslist = ['mid', 'appRuleCode']
        df_outputdata = DataFrame()



    elif inputdata.split('.')[-1] == 'xlsx':
        pass

    elif inputdata.split('.')[-1] == 'txt':
        pass


    pass


if __name__ == '__main__':
    appoutdata = '../data/appoutdata.csv'
    output_dataPath = '../data/'
    user2app1(appoutdata, output_dataPath)

    user2app()

    pass
