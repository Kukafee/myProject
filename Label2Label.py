# -*-coding:utf-8 -*-
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
from label.APPbehaviorLabel import APPbehaviorClassifi
from label.InterestLabel_1 import Interest_1
from label.InterestLabel_1_2 import Interest_1_2

excelPath = './outPutFile/'
excelName = 'Databank_channel_V26.xlsx'
excelFile = os.path.join(excelPath, excelName)

outputPath = './outPutFile/'
outputFileName= 'outPutLabel.xlsx'
outputFile = os.path.join(outputPath, outputFileName)
sheetname = 'output'

# inputPath 需要映射excel文件路径
# sheetname excel中表单名字
# outputPath 标签映射生成后文件路径
def Lable2label(excelFile, sheetname, outputPath):

    # 利用pandas读取excel表单，即DataFrame格式的df_Databank
    df_Databank = pd.read_excel(excelPath, sheet_name='output')

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

    # 将重新生成的df_Databank 写入 excel文件
    if os.path.exists(outputPath):
        os.remove(outputPath)
    df_Databank.to_excel(outputFile, index=False, sheet_name='output')


if __name__ == '__main__':
    Lable2label(excelPath, sheetname, outputPath)
    print('Done!')