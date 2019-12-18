from Path import *
from pandas import Series, DataFrame

# 年龄标签映射函数
def ageLabel(fileData):
    # 定义年龄时间段
    list1 = range(-1, 101)
    df_age = DataFrame(list1)
    df_age['label'] = ''
    # 遍历年龄段
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

    # 输出 df_age 到 age_ttage.csv 文件
    if os.path.exists(fileData):
        os.remove(fileData)
    #  sep='-' 设置分割符，index=False 设置不保留索引，header=False 设置不保留列名
    df_age.to_csv(fileData, index=False, sep=',', header=False)


if __name__ == '__main__':
    # age_ttageData = '../output/outputtable/age_ttage.csv'
    ageLabel(age_ttageData)
