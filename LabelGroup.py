from Path import *
import pandas as pd


def labelGroup(inputfile, sheet, outputfile):
    df_labelGroup = pd.read_excel(inputfile, sheet_name=sheet)

    # print(type(df_labelGroup))
    # print(df_labelGroup.head(1))
    # print(list(df_labelGroup.columns.values))
    # print(df_labelGroup.loc[0]['文章分类'].split('\n'))
    # n = len(df_labelGroup.loc[0]['文章分类'].split('\n'));
    # sum = 0.
    # for i in df_labelGroup.loc[0]['文章分类'].split('\n'):
    #     num = float(i.split(':')[1])
    #     sum += num  # sum = sum + num
    #     # print(num)
    # print(sum / n)
    #
    # print(n)
    # print(len(df_labelGroup))

    columnsList = ['性别', '年龄', '文章分类', 'app行为', '兴趣定向(一级&二级）', '预估人数',
                   '今日头条_广告展示数', '今日头条_用户覆盖数', '组合TGI', '加权组合TGI']
    for i in range(len(df_labelGroup)):

        df_labelGroup.loc[i, columnsList[6]] = df_labelGroup.loc[i][columnsList[5]].split('\n')[0].split('-')[1].split(' ')[0].split(':')[1]
        df_labelGroup.loc[i, columnsList[7]] = df_labelGroup.loc[i][columnsList[5]].split('\n')[2].split('-')[1].split(' ')[0].split(':')[1]

        averTgi = 0.
        for j in range(5):
            # print(j)
            n = len(df_labelGroup.loc[i][columnsList[j]].split('\n'))  # 类目中包含项目的个数
            sum = 0.   # 类目中包含项目的TGI加和
            for k in df_labelGroup.loc[i][columnsList[j]].split('\n'):
                num = float(k.split(':')[1])    # 每一个项目TGI值
                sum += num
            averTgi += (sum / n)
        averTgi = float('%.6f' %averTgi)    # 保留6位小数
        # averTgi = round(averTgi, 6)
        df_labelGroup.loc[i, columnsList[8]] = averTgi

    # 将重新生成的 df_labelGroup 数据写入文件
    if os.path.exists(outputfile):
        os.remove(outputfile)
    # df_labelGroup.to_csv(outputfile, index=False, sep=',')
    df_labelGroup.to_excel(outputfile, index=False, sheet_name=sheetname)


if __name__ == '__main__':
    sheetname = 'labelgroup'
    labelGroup(ilabelGroupData, sheetname, olabelGroupData)

