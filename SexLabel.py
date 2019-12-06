
# 性别标签映射函数
def sexLabel(keys):
    labelDic = {
        '-1':'不限',
        '0':'女',
        '1':'男'
    }
    return labelDic[keys]


if __name__ == '__main__':
    testLabel = sexLabel('1')
    print(testLabel)
