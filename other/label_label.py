#
# 导入python包
import numpy as np
import pandas as pd
import time
import os
import openpyxl
import xlrd
import matplotlib.pyplot as plt
from pandas import Series, DataFrame
from label.ArticleClassificationLabel import ArticleCalssification


# 定义文件路径
excelPath = './file/Databank_channel_V26.xlsx'
outputPath = './file/outPutLabel.xlsx'

# 利用pandas读取excel表单
df_Databank = pd.read_excel(excelPath, sheet_name='output')
# print(df_Databank.head(3))

# 获取df_Databank 的app标签列表
data_labelapp = df_Databank.loc[:, ['APP name']].values
# i = len(data_labelapp)
labelapp = []
for label in data_labelapp:
    if label not in labelapp:
        labelapp.append((label))
print('app label shape: ', np.shape(labelapp))

# 获取df_Databank 的一级标签列表
data_label1 = df_Databank.loc[:, ['一级标签']].values
i = len(data_label1)
print('总行数', data_label1.shape)
label1 = []
for label in data_label1:
    if label not in label1:
        label1.append(label)
print('一级标签 shape ', np.shape(label1))
# print(label1, end='\n')
# for item in label1:
#     print(item)

# 获取df_Databank 的二级标签列表
data_label2 = df_Databank.loc[:, ['二级标签']].values
# i = len(data_labelapp)
label2 = []
for label in data_label2:
    if label not in label2:
        label2.append((label))
print('二级标签 shape ', np.shape(label2))
print(len(data_label1))

print(df_Databank.shape)

# data_label1_label2 = []
# for i in np.arange(len(data_label1)):
#     label1_label2 = str(df_Databank.loc[i, '一级标签']) + '_' + str(df_Databank.loc[i, '二级标签'])
#     # print(label1_label2)
#     if label1_label2 not in data_label1_label2:
#         data_label1_label2.append(label1_label2)
# print('data_label1_label2 len:', len(data_label1_label2))
# print(data_label1_label2)

    # if label1_label2 not in data_label1_label2:
    #     data_label1_label2.append(list(label1_label2))
# print('data_label1_label2 shape: ', np.shape(data_label1_label2))

# print(data_label1_label2)
# unite = label1 + '_' + label2
# if unite not in



print(df_Databank.head())
df_Databank['文章分类标签'] = ''
for i in range(len(df_Databank)):
    label1_label2 = str(df_Databank.loc[i, '一级标签']) + '_' + str(df_Databank.loc[i, '二级标签'])
    df_Databank.loc[i, '文章分类标签'] = ArticleCalssification(label1_label2)








# label_list = ArticleCalssification('新闻资讯_垂直新闻')
# print(label_list)





if os.path.exists(outputPath):
    os.remove(outputPath)
df_Databank.to_excel(outputPath, index=False, sheet_name='output')
