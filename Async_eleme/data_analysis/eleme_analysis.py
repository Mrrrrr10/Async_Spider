# -*- coding:utf-8 -*-

import pymongo
import numpy as np
from snownlp import SnowNLP
from collections import Counter
import matplotlib.pyplot as plt


class Data_Analysis(object):
    def __init__(self):
        clinet = pymongo.MongoClient(host="localhost", port=27017)
        db = clinet.eleme
        self.collection = db.eleme_guangzhou_comment
        self.counter = Counter()

    def run(self):
        self.analysis_comment()

    def analysis_comment(self):
        cursor = self.collection.find({"content": {"$elemMatch": {"$ne": ""}}}, {"content": 1, "_id": 0})
        content = [item.get('content') for item in cursor if "小龙虾" in ''.join(item.get('content'))]
        content = sum(content, [])
        sentiments_list = [SnowNLP(text).sentiments for text in content]
        plt.hist(sentiments_list, bins=np.arange(0, 1, 0.02))
        plt.show()

if __name__ == '__main__':
    analysis = Data_Analysis()
    analysis.run()












