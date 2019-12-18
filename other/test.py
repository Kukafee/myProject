from Path import *
import pandas as pd
from pandas import Series, DataFrame
import os
import time

# test_exce = '../input/appid_table.xlsx'
itest_exce = '/Users/edz/pang/code/pycharm/myProject/input/Databank_channel_V26.xlsx'

df_test = pd.read_excel(test_exce, sheet_name=inputSheet[0])
print('df_test 类型', type(df_test))
print(df_test.head())
print('df_test 长度', len(df_test))
print('df_test 列名', df_test.columns.values.tolist())
appcode_list = []
appname_list = []
app_label_1 = []
app_label_2 = []
app_label1_2 = []
for i in range(len(df_test)):
    if df_test.loc[i]['Rule Code'] not in appcode_list:
        appcode_list.append(df_test.loc[i]['Rule Code'])

    if df_test.loc[i]['APP name'] not in appname_list:
        appname_list.append(df_test.loc[i]['APP name'])

    if df_test.loc[i]['一级标签'] not in app_label_1:
        app_label_1.append(df_test.loc[i]['一级标签'])

    if df_test.loc[i]['二级标签'] not in app_label_2:
        app_label_2.append(df_test.loc[i]['二级标签'])

    unite_label = str(df_test.loc[i]['一级标签']) + '_' + str(df_test.loc[i]['二级标签'])
    if unite_label not in app_label1_2:
        app_label1_2.append(unite_label)

print('appcode_list 数量: ', len(appcode_list))
print('appname 数量: ', len(appname_list))
print('app_label_1 数量: ', len(app_label_1))
print('app_label_2 数量: ', len(app_label_2))
print('app_label1_2: ', len(app_label1_2))

############################################################################################

idf_test = pd.read_excel(itest_exce, sheet_name=inputSheet[0])
print('idf_test 类型', type(idf_test))
print(idf_test.head())
print('idf_test 长度', len(idf_test))
print('idf_test 列名', idf_test.columns.values.tolist())
iappcode_list = []
iappname_list = []
iapp_label_1 = []
iapp_label_2 = []
iapp_label1_2 = []
for i in range(len(idf_test)):
    if idf_test.loc[i]['Rule Code'] not in iappcode_list:
        iappcode_list.append(idf_test.loc[i]['Rule Code'])

    if idf_test.loc[i]['APP name'] not in iappname_list:
        iappname_list.append(idf_test.loc[i]['APP name'])

    if idf_test.loc[i]['一级标签'] not in iapp_label_1:
        iapp_label_1.append(idf_test.loc[i]['一级标签'])

    if idf_test.loc[i]['二级标签'] not in iapp_label_2:
        iapp_label_2.append(idf_test.loc[i]['二级标签'])

    iunite_label = str(idf_test.loc[i]['一级标签']) + '_' + str(idf_test.loc[i]['二级标签'])
    if iunite_label not in iapp_label1_2:
        iapp_label1_2.append(iunite_label)

print('iappcode_list 数量: ', len(iappcode_list))
print('iappname 数量: ', len(iappname_list))
print('iapp_label_1 数量: ', len(iapp_label_1))
print('iapp_label_2 数量: ', len(iapp_label_2))
print('iapp_label1_2: ', len(iapp_label1_2))

#
print('只属于appid_table(新)的数据: ')
new_app_label_1 = []
new_app_label_2 = []
new_app_label1_2 = []
for i in app_label_1:
    if i not in iapp_label_1:
        new_app_label_1.append(i)

for i in app_label_2:
    if i not in iapp_label_2:
        new_app_label_2.append(i)
for i in app_label1_2:
    if i not in iapp_label1_2:
        new_app_label1_2.append(i)
print('new_app_label_1: ', new_app_label_1)
print('new_app_label_2: ', new_app_label_2)
print('new_app_label1_2: ', new_app_label1_2)

print('i只属于Databank_channel_V26(旧)的数据: ')
old_app_label_1 = []
old_app_label_2 = []
old_app_label1_2 = []
for i in iapp_label_1:
    if i not in app_label_1:
        old_app_label_1.append(i)
for i in iapp_label_2:
    if i not in app_label_2:
        old_app_label_2.append(i)
for i in iapp_label1_2:
    if i not in app_label1_2:
        old_app_label1_2.append(i)
print('old_app_label_1: ', old_app_label_1)
print('old_app_label_2: ', old_app_label_2)
print('old_app_label1_2: ', old_app_label1_2)








