#coding:utf-8
# 监控new scrapy 的预警模块，还未完成，坚持
import time
import datetime
import redis
from pymongo.mongo_client import MongoClient
import redis
import sys
sys.setdefaultencoding('utf-8')

class RedisQueue(object):
    def __init__(self):
        self.__db = redis.Redis(host='127.0.0.1',port=6379)

    def spop(self,key,field):
        return self.__db.spop(key,field)

    def scard(self,key):
        return self.__db.scard(key)

    def smembers(self,key):
        return self.__db.smembers(key)


class MonitorMessage(object):
    def __init__(self):
        self.conn = MongoClient(host="127.0.0.1",port=27017).streaming
        self.task_queue = RedisQueue()

    def check_data_total(self,old_data,new_data):


    def CheckFinishSpider(self,table,spider_name,uuid,data_total_stat,data_abnormal_stat,request_stat):
        for spider  in self.conn.table.find({"spider_name":spider_name,"uuid":uuid},{"_id":0,"uuid":uuid}):
            spider_dict = spider
        cursor = self.conn.alert_spider_info.find({"spider_name":spider_name})
        if cursor.count() == 0:
            self.conn.alert_spider_info.insert(spider_dict)
            # 在这里不需要比较了，积累第一次的数据
        else:
             now_data = int(spider_dict["data_stats"]["total"])
             for i in cursor:
                 pre_data = int(i["data_stats"]["total"])
             if now_data > pre_data:
                 self.conn.alert_spider_info.insert(spider_dict)
             if data_total_stat == "True":





    def Run(self):
        while True:
            monitor_item = self.task_queue.smembers()
            print " redis size %s" % monitor_item
            if monitor_item:
                for item in monitor_item:
                    item_stat = item.split(':')[0]
                    item_name = item.split(':')[1]
                    item_uuid = item.split(':')[2]
                    judge_cursor = self.conn.project_limit_table.find({"spider_name": spider_name})
                    if judge_cursor.count() == 0:
                        break
                    else:
                        for k in judge_cursor:
                            data_total_stat = k["data_total"]
                            data_abnormal_stat = k["data_abnormal"]
                            request_stat = k["request_stats"]
                        if item_stat == "finish":
                            collection_name = "spiderCollectHistory"

                    elif item_stat == "action":
                        collection_name = "spiderCollect"
                    curcor = self.conn.collection_name.find({"uuid":item_uuid})
                    if curcor.count() != 0:
                        for i in curcor:
                            spider_name = i["spider_name"]
                            uuid = i["uuid"]
                            item_data_dict = i["data_stats"]
                            request_stats_dict = i["request_stats"]
                            if data_total_stat == "True":
                                # 进行下面的工作
                            if data_abnormal_stat == "True":
                                # 进行下面的工作
                            if request_stat == "True":
                                # 进行下面的工作
                        else:
                            break
                    else:
                        break















