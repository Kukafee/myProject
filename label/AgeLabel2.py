
import numpy as np
import pandas as pd
import time
import os
import openpyxl
import xlrd
import matplotlib.pyplot as plt
from pandas import Series, DataFrame

list1 = range(-1, 101)
df_age = DataFrame(list1)
df_age['label'] = ''
for i in range(len(df_age)):
    age = df_age.loc[i][0]
    label = ''
    if age == -1:
        label = '未知'
    elif age >= 18 and age <= 23:
        label = '18-23'
    elif age >= 24 and age <= 30:
        label = '24-30'
    elif age >= 31 and age <= 40:
        label = '31-40'
    elif age >= 41 and age <= 49:
        label = '41-49'
    elif age >= 50:
        label = '50+'
    else:
        label = ''
    df_age.loc[i, 'label'] = label

print(df_age)
print(type(df_age))
outputFile_csv = '/Users/edz/pang/zoc7/upload/age_ttage.csv'
if os.path.exists(outputFile_csv):
    os.remove(outputFile_csv)

#  sep='-' 设置分割符    index=False 设置不保留索引     header=False 设置不保留列名
df_age.to_csv(outputFile_csv, index=False, sep=',', header=False)
