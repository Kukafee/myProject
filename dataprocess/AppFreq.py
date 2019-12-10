import json
import os
import pandas as pd
from pandas import Series, DataFrame

# 测试数据
dataPath = '../data/'
testfileName = 'outdata.csv'
# 路径拼接
filePath = os.path.join(dataPath, testfileName)
# print(filePath)
# 测试用df_data数据
df_data = pd.read_csv(filePath, index_col=1)    # index_col=0 表示设置第0列为索引index
# print(type(df_data))
# print(df_data)
# print(df_data.columns)

# 测试数据
# strs = ['{"3004":{"daycount":1,"pv":3,"freq":3}, "3003":{"daycount":1,"pv":3,"freq":3}, "3002":{"daycount":1,"pv":3,"freq":3}}']
# df_data = DataFrame({'app_freq':strs}, index=['P_7925911BF4SXMURP'])
# mid = 'P_79259119CAWHDHRY'


# 提取app_freq特征的函数，接收mid参数，返回mid样本的app_freq的Series
def appFreq(mid):
    # 新建空的 Series
    # Ser_appFreq = Series([], index=[])
    # Ser_appFreq.index = ['mid']
    # Ser_appFreq['mid'] = mid

    # print(Ser_appFreq)
    # 读取每个样本的app_freq数据，类型为 <class 'str'>
    app_freq = df_data.loc[mid]['app_freq']
    # 使用eval()函数返回传入字符串的表达式的结果，即将字符串类型转化为字典类型
    app_freq = eval(app_freq)    # {"3004":{"daycount":1,"pv":3,"freq":3}, "3003":{"daycount":1,"pv":3,"freq":3}}
    # 提取app_freq字典中的键，生成listKeys列表
    ListKeys = list(app_freq.keys())    # ["3004", "3003"]

    # ListData的每个元素每个app所对应的一个列表[],分别表示"daycount", "pv", "freq"的值，如[1, 3, 3]
    ListData = []
    for key in ListKeys:
        values = app_freq[key]  # 类型为 <class 'dict'>
        # print("***", type(values))  # {"daycount":1,"pv":3,"freq":3}
        listkeys = list(values.keys())  # ["daycount", "pv", "freq"]
        listvalues = list(values.values())  # [1, 3, 3]
        # print(listkeys)
        # print(listvalues)
        # 添加每个app的相应的值
        ListData.append(listvalues)
    Ser_appFreq = Series(ListData, index=ListKeys)
    # 为Ser_appFreq 添加 name属性
    Ser_appFreq.name = mid
    Ser_appFreq.index.name = 'rule_code'
    # print(Ser_appFreq)


    # for i in range(1, len(df_data)+1):
    #     # print(i)
    #     # 读取每个样本的app_freq数据，类型为 <class 'str'>
    #     app_freq = df_data.loc[i]['app_freq']
    #     # print(app_freq)
    #     # print(type(app_freq))
    #     # 使用eval()函数返回传入字符串的表达式的结果，即将字符串类型转化为字典类型
    #     app_freq = eval(app_freq)
    #     # print(type(app_freq))
    #     # 提取app_freq字典中的键，生成listKeys列表
    #     ListKeys = list(app_freq.keys())
    #
        # for key in ListKeys:
        #     values = app_freq[key]      # 类型为 <class 'dict'>
        #     # print("***", type(values))  # {"daycount":1,"pv":3,"freq":3}
        #     listkeys = list(values.keys())     # ["daycount", "pv", "freq"]
        #     listvalues = list(values.values())         # [1, 3, 3]
        #     print(listkeys)
        #     print(listvalues)


    return Ser_appFreq


    # app_freq = df_data['app_freq']
    # print(type(app_freq))


if __name__ == '__main__':
    test_P_79259119CAWHDHRY_app_freq = appFreq('P_79259119CAWHDHRY')
    print(test_P_79259119CAWHDHRY_app_freq)
    print(type(test_P_79259119CAWHDHRY_app_freq))
    print(len(test_P_79259119CAWHDHRY_app_freq))

    test_P_7925911D61ZMKZLN_app_freq = appFreq('P_7925911D61ZMKZLN')
    print(test_P_7925911D61ZMKZLN_app_freq)
    print(type(test_P_7925911D61ZMKZLN_app_freq))
    print(len(test_P_7925911D61ZMKZLN_app_freq))
