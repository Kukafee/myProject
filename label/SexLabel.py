from Path import *
from pandas import Series, DataFrame

# 性别标签映射函数
def sexLabel(fileData):
    # 定义性别变量
    list1 = [-1, 0, 1]
    df_sex = DataFrame(list1)
    df_sex['label'] = ''
    # 遍历性别值
    for i in range(len(df_sex)):
        sex = df_sex.loc[i][0]
        label = ''
        if sex == -1:
            label = '未知'
        elif sex == 0:
            label = '女'
        elif sex == 1:
            label = '男'
        else:
            label = ''
        df_sex.loc[i, 'label'] = label

    if os.path.exists(fileData):
        os.remove(fileData)
    df_sex.to_csv(fileData, index=False, sep=',', header=False)


if __name__ == '__main__':
    # sex_ttsexData = '../output/outputtable/sex_ttsex.csv'
    sexLabel(sex_ttsexData)
