import json
import os
import pandas as pd
from pandas import Series, DataFrame


dataPath = '../data/'
fileName = 'test_app_freq.csv'
# 路径拼接
filePath = os.path.join(dataPath, fileName)

def json(filePath):
    csv_data = pd.read_csv(filePath)
    print(csv_data.shape)
    print(type(csv_data))

    pass


if __name__ == '__main__':
    print(filePath)
    json(filePath)

    pass
