import os
import time

# 获取当前时刻时间
current_time =  time.strftime('%Y%m%d_%H%M%S')

# 输入数据表路径
inputPath = '/Users/edz/pang/code/pycharm/myProject/input/'
# 输出数据表路径
outputPath = '/Users/edz/pang/code/pycharm/myProject/output/outputtable/'

# 输入 原始数据表 名称
# inputName = 'Databank_channel_V26.xlsx'
inputName = 'appid_table.xlsx'
# 输出 app标签数据表 名称 ppid_table
outputName = 'appid_table_' + current_time
# 输出 性别映射表 名称
sex_ttsex = 'sex_ttsex.csv'
# 输出 年龄映射表 名称
age_ttage = 'age_ttage.csv'
# 输出 app标签映射表 名称
app_label = 'app_label.csv'

# 合成输入原始数据
inputData = os.path.join(inputPath, inputName)
# 合成输出app标签数据表
outputData = os.path.join(outputPath, outputName)
# 合成输出性别映射表
sex_ttsexData = os.path.join(outputPath, sex_ttsex)
# 合成输出年龄映射表
age_ttageData = os.path.join(outputPath, age_ttage)
# 合成输出app标签映射表
app_labelData = os.path.join(outputPath, app_label)

# 输入数据单元表名称
inputSheet = ['output', '名字映射', 'Consolidate标签去重', 'standard rule V39', 'channel-触点标签映射表', '备注', 'notes']
# 输出数据单元表名称
outputSheet = ['output', '名字映射', 'Consolidate标签去重', 'standard rule V39', 'channel-触点标签映射表', '备注', 'notes']

# 测试用原始数据
test_exce = '/Users/edz/pang/code/pycharm/myProject/input/appid_table.xlsx'

if __name__ == '__main__':

    current_time = time.strftime('%Y%m%d_%H%M%S')
    print(current_time)
    print(type(current_time))
