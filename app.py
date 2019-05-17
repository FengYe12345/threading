#coding=utf-8

#coding:utf-8

#-*- coding:utf-8 -*-
import datetime
from datetime import date, datetime
import pymongo
from flask import Flask
import pymysql
from pymongo import MongoClient
from flask_pymongo import PyMongo
from multiprocessing.dummy import Pool as ThreadPool #多线程
import time

app = Flask(__name__)
# 查询的mongoDb配置
app.config['MONGO_DBNAME'] = 'DATA'
app.config['MONGO_URI'] = 'mongodb://172.16.31.171:27017/DATA'
mongo = PyMongo(app)

# 查询收集名的配置
mongo_client = pymongo.MongoClient(host="172.16.31.171",port=27017)
USER_MONGO = mongo_client["DATA"]

# mysql数据库游标设置
conn = pymysql.connect(host='172.16.31.171', port=3306, user='select', password='select', db='test', charset='utf8')
cursor = conn.cursor()
cursor.execute('set names utf8')
cursor.execute('set autocommit = 1')

# 获取收集名
def col_names():
        result = USER_MONGO.list_collection_names(session=None)
        return result

print(col_names())

# 从mongoDb获取最新一条数据并更新至mysql
def find_first_data(SN):
    try:
        result = mongo.db[SN].find().sort([("t", -1)]).limit(1)[0]
        result.pop('_id', '404')
        result.pop('t', '404')
        result['sn'] = SN
        # result["t"] = t1 = datetime.timestamp(result["t"]) # 转换时间戳
        sql = "insert into data(sn) values(%s) " % (result['sn'])
        # sql = "update data set rs =%s where sn = %s" %(result['rs'],result['sn'])
        cursor.execute(sql)
    except Exception as e:
        print('异常')
    # print('数据更新异常')
    return result


start2 = time.time()
pool = ThreadPool(4)
results2 = pool.map(find_first_data, col_names())
pool.close()
pool.join()

# print(find_first_data())
# print(type(find_first_data()))
@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
