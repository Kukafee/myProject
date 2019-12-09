
# 年龄标签映射函数，参数可以是字符串格式，也可以是数值类型
def ageLabel(age):
    age = int(age)
    label = ''
    # 如果年龄定义为 0， 则表示为年龄不限
    if age == -1:
        label = '不限'
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
        label = '不限'

    return label


if __name__ == '__main__':
    testLabel_1 = ageLabel('22')
    testLabel_2 = ageLabel(22)
    print(testLabel_1)
    print(testLabel_2)