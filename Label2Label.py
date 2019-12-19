# -*-coding:utf-8 -*-
# 导入python包
# import numpy as np
import pandas as pd
# import time
# import os
# import openpyxl
# import xlrd
# import matplotlib.pyplot as plt
# from pandas import Series, DataFrame
from label.ArticleClassificationLabel import ArticleCalssification
from label.APPbehaviorLabel import APPbehaviorClassifi
from label.InterestLabel_1 import Interest_1
from label.InterestLabel_1_2 import Interest_1_2
from Path import *
from label.AgeLabel import ageLabel
from label.SexLabel import sexLabel

# excelPath = './outPutFile/'
# excelName = 'Databank_channel_V26.xlsx'
# excelFile = os.path.join(excelPath, excelName)
#
# outputPath = './outPutFile/'
# # 生成excel格式的文件
# outputFileName = 'outPutLabel.xlsx'
# outputFileName_csv = 'outPutLabel_csv.csv'
# # 生成csv格式的文件
# outputFile = os.path.join(outputPath, outputFileName)
# outputFile_csv = os.path.join(outputPath, outputFileName_csv)
# sheetname = 'output'

# inputPath 需要映射excel文件路径
# sheetname excel中表单名字
# outputPath 标签映射生成后文件路径

def Lable2label(input, insheet, output, outsheet):
    # 重定义输出名字
    outputName_xlsx = output + '.xlsx'
    outputName_csv = output + '.csv'

    # 利用pandas读取excel表单，即DataFrame格式的df_Databank
    df_Databank = pd.read_excel(input, sheet_name=insheet[0])

    # 获取df_Databank 的一级标签列表
    data_label1 = df_Databank.loc[:, ['一级标签']].values
    label1 = []
    for label in data_label1:
        if label not in label1:
            label1.append(label)

    # 获取df_Databank 的二级标签列表
    data_label2 = df_Databank.loc[:, ['二级标签']].values
    # i = len(data_labelapp)
    label2 = []
    for label in data_label2:
        if label not in label2:
            label2.append((label))

    # 在df_Databank中增加一列 '文章分类标签'
    df_Databank['文章分类标签'] = ''
    for i in range(len(df_Databank)):
        label1_label2 = str(df_Databank.loc[i, '一级标签']) + '_' + str(df_Databank.loc[i, '二级标签'])
        df_Databank.loc[i, '文章分类标签'] = ArticleCalssification(label1_label2)

    # 在df_Databank中增加一列 'APP行为标签'
    df_Databank['App行为'] = ''
    for i in range(len(df_Databank)):
        label1_label2 = str(df_Databank.loc[i, '一级标签']) + '_' + str(df_Databank.loc[i, '二级标签'])
        df_Databank.loc[i, 'App行为'] = APPbehaviorClassifi(label1_label2)

    # 在df_Databank中增加一列 '兴趣定向(一级标签）'
    df_Databank['兴趣定向(一级)'] = ''
    for i in range(len(df_Databank)):
        label1_label2 = str(df_Databank.loc[i, '一级标签']) + '_' + str(df_Databank.loc[i, '二级标签'])
        df_Databank.loc[i, '兴趣定向(一级)'] = Interest_1(label1_label2)

    # 在df_Databank中增加一列 '兴趣定向(一级&二级标签）'
    df_Databank['兴趣定向(一级&二级)'] = ''
    for i in range(len(df_Databank)):
        label1_label2 = str(df_Databank.loc[i, '一级标签']) + '_' + str(df_Databank.loc[i, '二级标签'])
        df_Databank.loc[i, '兴趣定向(一级&二级)'] = Interest_1_2(label1_label2)

    # 删除df_Databank中指定的列， 重新生成为 df_Databank_csv，最后写入csv格式的文件
    # df_Databank_csv = df_Databank.drop(['APP name', '一级标签', '二级标签', 'Consolidated label code'], axis=1)

    # 将重新生成的df_Databank 写入 excel文件
    if os.path.exists(outputName_xlsx):
        os.remove(outputName_xlsx)
    df_Databank.to_excel(outputName_xlsx, index=False, sheet_name=outsheet[0])

    # # 将重新生成的df_Databank_csv数据写入 csv 格式的文件
    if os.path.exists(outputName_csv):
        os.remove(outputName_csv)
    #  sep='-' 设置分割符    index=False 设置不保留索引     header=False 设置不保留列名
    # df_Databank_csv.to_csv(outputFile_csv, index=False, sep=',', header=False)
    df_Databank.to_csv(outputName_csv, index=False, sep=',', header=False, columns=['Rule Code','APP name', '文章分类标签', 'App行为', '兴趣定向(一级)', '兴趣定向(一级&二级)'])

    # 输出app_local app本地一级标签和二级标签(appid  appname  label1  label2)
    df_Databank.to_csv(app_localData, index=False, sep=',', header=False,
                       columns=['Rule Code', 'APP name', '一级标签', '二级标签'])


if __name__ == '__main__':
    # 生成 app标签文件 app_label.csv 和 .xlsx 两种格式 ( ./output/outputtable/ )
    Lable2label(inputData, inputSheet, outputData, outputSheet)
    # 生成 性别映射标签文件 sex_ttsex.csv （ ./output/outputtable/ ）
    sexLabel(sex_ttsexData)
    # 生成 年龄映射标签文件 age_ttage.csv ( ./output/outputtable/ )
    ageLabel(age_ttageData)

    print('Done!')