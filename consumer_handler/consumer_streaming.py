#coding:utf-8
# 用多线程的生产者消费者并发进行处理
# 用于kafka 第二次消费
from kafka import KafkaConsumer
import time
import ast
from pymongo import MongoClient
from Queue import Queue
import threading
import redis
from bson.objectid import ObjectId

class HashQueue(object):
    def __init__(self):
        self.__db = redis.Redis(host='10.10.92.98',port=6379)

    def get_id(self,redis_key,ip):
        if self.__db.exists(redis_key):
            if self.__db.hexists(redis_key,ip):
                return self.__db.hget(redis_key,ip)
            else:
                return None
        else:
            return None

    def hincrby(self,redis_key,key,amount=1):
        return self.__db.hincrby(redis_key,key,amount)

    def hexists(self,redis_key,key):
        return  self.__db.hexists(redis_key,key)

    def hset(self,redis_key,key,value):
        return self.__db.hset(redis_key,key,value)

    def sadd(self,redis_key,value):
        return self.__db.sadd(redis_key,value)


class pyqueue(threading.Thread):
    def __init__(self,que):
        threading.Thread.__init__(self)
        self.daemon = False
        self.q = que
        self.HashQueue = HashQueue()
        self.spider_list = []

    def conn_mongodb(self):
        try:
            conn = MongoClient('mongodb://10.10.92.98:27017')
            database = conn.streaming
            return database
        except:
            return None

    def handle_request_status(self,request_stats):
        stats_dict = {}
        request_count = int(request_stats.get('downloader/request_count', 0))
        request_succeed = int(request_stats.get('downloader/response_status_count/200', 0))
        stats_dict['request_total'] = request_count
        stats_dict['not_pass'] = {'num': request_count - request_succeed, 'info': {}}

        for key in request_stats.keys():
            if key.find('downloader/exception_type_count') >= 0:
                stats_dict['not_pass']['info'][key.replace('.', '_')] = request_stats[key]
            elif key.find('downloader/response_status') >= 0 > key.find('200'):     # 找到包含 downloader/response_status 的  但不包含 200
                stats_dict['not_pass']['info'][key.replace('.', '_')] = request_stats[key]
        return stats_dict

    def run(self):
        while True:
            conn = self.conn_mongodb()
            consumer_data = self.q.get()
            self.spider_list = []
            if consumer_data:
                for value in consumer_data.values():
                    #value["request_stats"] = self.handle_request_status(value["request_stats"])
                    spider_name = value["spider_name"]
                    uuid =  value["uuid"]
                    ip = value["ip"]
                    action = value["action"]
                    spider_uuid_hash = str(spider_name) + ':' + str(uuid)
                    id = self.HashQueue.get_id(spider_uuid_hash,ip)
                    if id:
                        if action == "spider_closed":
                            try:
                                conn.spiderInfo.update({'_id': ObjectId(id)},{"$set":{
                                    "request_stats":value["request_stats"],
                                    "data_stats":value["data_stats"],
                                    "send_time":value["send_time"],
                                    "action":value["action"],
                                    "finish_time":value["finish_time"],
                                    "spider_rate":value["spider_rate"]
                                }})
                            except:
                                print " 更新字段失败"
                            else:
                                # 将关闭的closed 放入到 finish_uuid hash key 中
                                self.HashQueue.hincrby("finish_uuid",spider_uuid_hash,1)
                                if not self.HashQueue.hexists("finish_time",spider_uuid_hash):
                                    now_finish_time = int(time.time())
                                    self.HashQueue.hset("finish_time",spider_uuid_hash,now_finish_time)
                        else:
                             send_time_stamp = int(time.mktime(time.strptime(value["send_time"],'%Y-%m-%d %H:%M:%S')))
                             cursor = conn.spiderInfo.find({'_id': ObjectId(id)})
                             for i in cursor:
                                 db_send_time = int(time.mktime(time.strptime(i["send_time"],'%Y-%m-%d %H:%M:%S')))
                                 if send_time_stamp >= db_send_time:
                                     try:
                                         conn.spiderInfo.update({'_id': ObjectId(id)},{"$set":{
                                             "request_stats":value["request_stats"],
                                             "data_stats":value["data_stats"],
                                             "send_time":value["send_time"],
                                             "action":value["action"],
                                             "finish_time":value["finish_time"],
                                             "spider_rate":value["spider_rate"]
                                         }})
                                     except:
                                         print "更新字段失败"
                    else:
                        try:
                            result = conn.spiderInfo.insert(value)
                            self.HashQueue.hset(spider_uuid_hash,ip,result)
                            if spider_uuid_hash not in self.spider_list:
                                self.HashQueue.sadd("spider_set",spider_uuid_hash)
                                self.spider_list.append(spider_uuid_hash)
                        except:
                            pass
                try:
                    conn.close()
                except:
                    pass
                time.sleep(3)
            else:
                time.sleep(1)

# 定义生产者
class consumer_scrapy(threading.Thread):
    def __init__(self,que, topic="None", interval=60):
        threading.Thread.__init__(self)
        self.daemon = False
        self.topic = topic
        self.interval = interval
        self.data_dict = {}
        self.pre_time = 0
        self.q = que
        self.data_queue = {}

    def read_kafka(self):
        group_id = str(self.topic) + '-' + "test"
        try:
            consumer = KafkaConsumer(group_id=group_id,bootstrap_servers=['10.10.183.246:9092', '10.10.61.29:9092', '10.10.40.200:9092'])
            consumer.subscribe(topics=(self.topic,))
            return consumer
        except:
            return None

    def run(self):
        while True:
            consumer = self.read_kafka()
            if not consumer:
                break
            self.data_queue = {}
            self.data_dict = {}
            self.pre_time = int(time.time()) + self.interval
            while True:
                message = consumer.poll(timeout_ms=5)
                for msg in message.values():
                    value = ast.literal_eval(msg[0].value)
                    uuid = value["uuid"]
                    if not uuid:
                        continue
                    key = str(value["spider_name"]) + ':' + str(value["ip"]) + ':' + str(uuid)
                    action = value["action"]
                    if action == "spider_open":
                        if key not in self.data_dict.keys():
                            value["finish_time"] = None
                            self.data_dict[key] = value
                        else:
                            value["finish_time"] = None
                            self.data_dict[key] = value
                    elif action == "spider_stats":
                        if key not in self.data_dict.keys():
                            value["finish_time"] = None
                            self.data_dict[key] = value
                        else:
                            tmp = self.data_dict[key]
                            now_send = int(time.mktime(time.strptime(value["send_time"],'%Y-%m-%d %H:%M:%S')))
                            pre_send = int(time.mktime(time.strptime(tmp["send_time"],'%Y-%m-%d %H:%M:%S')))
                            if now_send >= pre_send:
                                tmp["data_stats"] = value["data_stats"]
                                tmp["request_stats"] = value["request_stats"]
                                tmp["spider_rate"] = value["spider_rate"]
                                tmp["finish_time"] = None
                                tmp["action"] = value["action"]
                                tmp["send_time"] = value["send_time"]
                                self.data_dict[key] = tmp
                    elif action == "spider_closed":
                        if key not in self.data_dict.keys():
                            self.data_dict[key] = value
                        else:
                            tmp = self.data_dict[key]
                            tmp["data_stats"] = value["data_stats"]
                            tmp["request_stats"] = value["request_stats"]
                            tmp["spider_rate"] = value["spider_rate"]
                            tmp["finish_time"] = value["finish_time"]
                            tmp["action"] = value["action"]
                            tmp["send_time"] = value["send_time"]
                            self.data_dict[key] = tmp
                now_time = int(time.time())
                if now_time >= self.pre_time:
                    try:
                        consumer.close()
                    except:
                        pass
                    self.data_queue = self.data_dict.copy()
                    self.q.put(self.data_queue)
                    time.sleep(10)
                    break

if __name__=='__main__':
    que = Queue()
    print "start........."
    consumer_scrapy = consumer_scrapy(que,"spider_stream_again",interval=60)
    consumer_scrapy.start()
    pyqueue = pyqueue(que)
    pyqueue.start()
