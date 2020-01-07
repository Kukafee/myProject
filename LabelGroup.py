from Path import *
import pandas as pd
import os


def labelGroup(inputfile, sheet, outputpath):

    # 遍历 sheet 数组中的表单
    for isheet in sheet:
        # 输出文件名称
        outputLabelGroup = 'out_'+ isheet + '.xlsx'
        outputFath = os.path.join(outputpath, outputLabelGroup)



        df_labelGroup = pd.read_excel(inputfile, sheet_name=isheet)

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
        # if os.path.exists(outputfile):
        #     os.remove(outputfile)
        # df_labelGroup.to_csv(outputfile, index=False, sep=',')
        df_labelGroup.to_excel(outputFath, index=False, sheet_name=isheet)






if __name__ == '__main__':
    sheetname = ['labelgroup', 'labelgroup_male', 'labelgroup_female']
    labelGroup(ilabelGroupData, sheetname, outputPath)







