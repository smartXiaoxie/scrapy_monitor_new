#coding:utf-8
#这里是进行实时数据处理的部分，其他的都不需要，待优化
import redis
import tornado.httpserver
import tornado.ioloop
import tornado.options
import datetime
import tornado.web
import json
from tornado.ioloop import PeriodicCallback
from tornado.options import define, options
from pymongo import MongoClient
from bson.objectid import ObjectId
import time
import os
from handler.indexhandler import BeseHandler

route = [
    (r'/index',BeseHandler)
]


class mongodbQueue(object):
    def __init__(self):
        self.client = MongoClient('mongodb://10.10.92.98:27017').streaming

    def spiderInfo(self):
        return self.client.spiderInfo

    def spiderInfoHistory(self):
        return self.client.spiderInfoHistory

    def spiderCollect(self):
        return  self.client.spiderCollect

    def spiderCollectHistory(self):
        return self.client.spiderCollectHistory


class redisQueue(object):
    def __init__(self):
        self.__db = redis.Redis(host='127.0.0.1',port=6379)

    def smembers(self,key):
        return  self.__db.smembers(key)

    def hget(self,key,field):
        return self.__db.hget(key,field)

    def hlen(self,key):
        return self.__db.hlen(key)

    def hkeys(self,key):
        return self.__db.hkeys(key)

    def hexists(self,key,field):
        return self.__db.hexists(key,field)

    def hdel(self,key,field):
        return self.__db.hdel(key,field)

    def srem(self,key,field):
        return self.__db.srem(key,field)

    def get(self,key):
        return self.__db.get(key)

    def delete(self,key):
        return self.__db.delete(key)

class Application(tornado.web.Application):

    def __init__(self):
        tornado.web.Application.__init__(self,handlers=route)

                                         #template_path=os.path.join(os.path.dirname(__file__), "templates"),
                                         #static_path=os.path.join(os.path.dirname(__file__), "static")
                                         #)
        self.period = 120000
        self.redis_queue = redisQueue()
        self.mongodbQueue = mongodbQueue()

        def spider_info_collect():
            spider_key_set = self.redis_queue.smembers("spider_set")
            for keys in spider_key_set:
                spider_name = keys.split(':')[0]
                spider_uuid = keys.split(':')[1]
                spider_data_dict = {}
                spider_id_list = [self.redis_queue.hget(keys, field) for field in self.redis_queue.hkeys(keys)]
                spider_id_num = len(spider_id_list)
                spider_data_dict[keys] = {
                    "operating": spider_id_num,"stopped": 0,
                    "uuid": spider_uuid,
                    "start_time": 0,"finish_time": None,
                    "time_total": None,
                    "spider_name":None,
                    "data_stats" : {"total": 0, "abnormal": 0},
                    "rate_avg": 0,
                    "request_stats":{},
                }

                for id in spider_id_list:
                    for spider in self.mongodbQueue.spiderInfo().find({'_id': ObjectId(id)}):
                        spider_data_dict[keys]["start_time"] = spider["start_time"]
                        spider_data_dict[keys]["spider_name"] = spider["spider_name"]
                        spider_data_dict[keys]["spider_name"] = spider["spider_name"]
                        spider_data_dict[keys]["data_stats"]["total"] += spider["data_stats"]["total"]
                        spider_data_dict[keys]["data_stats"]["abnormal"] += spider["data_stats"]["abnormal"]
                        spider_data_dict[keys]["rate_avg"] += spider["spider_rate"]
                        for spider_request, num in spider["request_stats"]["not_pass"]["info"].iteritems():
                            spider_data_dict[keys]["request_stats"].setdefault(spider_request,0)
                            spider_data_dict[keys]["request_stats"][spider_request] += num
                        spider_data_dict[keys]["request_stats"].setdefault("request_total", 0)
                        spider_data_dict[keys]["request_stats"]["request_total"] += spider["request_stats"]["request_total"]
                        spider_data_dict[keys]["request_stats"].setdefault("request_failed", 0)
                        spider_data_dict[keys]["request_stats"]["request_failed"] += spider["request_stats"]["not_pass"]["num"]

                finish_time = None
                if not self.redis_queue.hexists("finish_uuid", keys):
                    spider_stop_num = 0
                    spider_data_dict[keys]["stopped"] = spider_stop_num
                    maintenance_status = self.redis_queue.get('maintenance_mode')
                    if maintenance_status == "False":
                        spider_timeout = int(self.redis_queue.hget("spider:timeout",str(spider_name)))
                        run_time = int(time.time()) - int(time.mktime(time.strptime(spider_data_dict[keys]["start_time"],'%Y-%m-%d %H:%M:%S')))
                        if run_time >= spider_timeout:
                            finish_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                else:
                    spider_stop_num = int(self.redis_queue.hget("finish_uuid",keys))
                    spider_stop_time = int(time.time()) - int(self.redis_queue.hget("finish_time",keys))
                    spider_timeout = int(self.redis_queue.hget("spider:timeout",str(spider_name)))
                    if spider_stop_num == spider_id_num:
                        finish_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(int(self.redis_queue.hget("finish_time",keys))))
                    else:
                        if spider_stop_num >= spider_id_num * 0.3 and spider_stop_time >= 600:
                            finish_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(int(self.redis_queue.hget("finish_time",keys))))
                        elif spider_stop_num < spider_id_num * 0.3 and spider_stop_time >= spider_timeout:
                            finish_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                spider_data_dict[keys]["stopped"] = spider_stop_num
                spider_data_dict[keys]["finish_time"] = finish_time
                #如果爬虫已经处于结束状态，那么就进行更新到 spiderConllect 并更新到 spiderCollectHistory , 并删除 redis 里面finish_uuid 和finish_time

                if self.mongodbQueue.spiderCollect().find({"spider_name":spider_name,"uuid":spider_uuid}).count():
                    self.mongodbQueue.spiderCollect().update({"spider_name":spider_name,"uuid":spider_uuid},
                                                                {"$set":{"data_stats":spider_data_dict[keys]["data_stats"],
                                                                "rate_avg":spider_data_dict[keys]["rate_avg"],
                                                                "request_stats":spider_data_dict[keys]["request_stats"],
                                                                "finish_time":spider_data_dict[keys]["finish_time"],
                                                                "stopped":spider_data_dict[keys]["stopped"],
                                                                "operating":spider_data_dict[keys]["operating"]}})
                else:
                    if self.mongodbQueue.spiderCollect().find({"spider_name":spider_name,"finish_time":{"$ne":None}}).count():
                        self.mongodbQueue.spiderCollect().remove({"spider_name":spider_name,"finish_time":{"$ne":None}})
                    self.mongodbQueue.spiderCollect().insert(spider_data_dict[keys])
                if finish_time:
                    self.redis_queue.srem("spider_set",keys)
                    self.redis_queue.delete(keys)
                    self.redis_queue.hdel("finish_uuid",keys)
                    self.redis_queue.hdel("finish_time",keys)
                    finish_tamp = int(time.mktime(time.strptime(spider_data_dict[keys]["finish_time"],'%Y-%m-%d %H:%M:%S')))
                    start_tamp = int(time.mktime(time.strptime(spider_data_dict[keys]["start_time"],'%Y-%m-%d %H:%M:%S')))
                    time_total = (finish_tamp - start_tamp) / 60
                    spider_data_dict[keys]["time_total"] = time_total
                    self.mongodbQueue.spiderCollectHistory().insert(spider_data_dict[keys])
        spider_info_collect_period = PeriodicCallback(spider_info_collect, self.period)
        spider_info_collect_period.start()

if __name__=='__main__':
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(8082)
    tornado.ioloop.IOLoop.instance().start()
