#coding:utf-8
# 预警模块，主程序
import threading
import time
import multiprocessing
#from Conn_database import MysqlConn
from Conn_database import RedisConn
from Conn_database import MongoConn
from gevent import monkey
from gevent import pool
import ast
from Alert_inform import handler
import os
monkey.patch_all()


class MonitorBigdata(multiprocessing.Process):

    def __init__(self):
        multiprocessing.Process.__init__(self)
        print "ini"
        self.conn_redis = RedisConn.Redisaction()
        self.conn_mongo = MongoConn.Mongo_client()
        self.monitor_table = "MonitorBigdataItem"
        self.BigAlertAction = handler.BigAlertAction()

    def Table_monitor(self,table_name,queue_time):
        print table_name
        print queue_time
        client_mongo = self.conn_mongo.mongo_conn
        try:
            cur_data = client_mongo.MonitorBigdataItem.find({"table_name":table_name,"monitor_state":"true"})
        except:
            print " 没有取到数据"
            exit()
        if cur_data.count() != 0:
            for item in cur_data:
                item_id = str(item["_id"])
                item_uniq_dict = item["unique_item"]
                item_item = item["item"]
                item_judge_state = item["judge_state"]
                item_value = item["judge_value"]
                item_module = item["normal_module"]
                item_inform = item["inform_item"]
                item_handler = item["inform_handler"]
                item_handler_manager = item["inform_handler_manager"]
                item_table = item["table_name"]
                collect_client = client_mongo[item_table]
                item_uniq_dict["t_time"] = queue_time
                if item_module == "general":
                    try:
                        check_data = collect_client.find(item_uniq_dict,{item_item:1})
                    except:
                        continue
                    if check_data.count() != 0:
                        for data in check_data:
                            check_item_data = data[item_item]
                    else:
                        continue
                    if isinstance(check_item_data,(int,float)):
                        judge_path = "int(" + str(check_item_data) +")" + " " + str(item_judge_state) + " " + "int(" + str(item_value) + ")"
                    else:
                        judge_path = "str(" + '"' + str(check_item_data) + '"' + ")" + " " + str(item_judge_state) + " "  + "str(" + '"' +  str(item_value) + '"' + ")"
                    if eval(judge_path):
                        # 调用推送信息模块
                        self.BigAlertAction.main(item_id,item_inform,item_handler,item_handler_manager)
                    else:
                        # 解除预警记录
                        self.BigAlertAction.Relieve_alert(item_id)
                elif item_module == "HdfsNodeModule":
                    try:
                        check_data = collect_client.find(item_uniq_dict,{item_item:1,"node_ip":1})
                    except:
                        continue
                    if check_data.count() != 0:
                        for data in check_data:
                            inform = ""
                            print data
                            node_item_id = ""
                            node_ip = data["node_ip"]
                            node_item = data[item_item]
                            if isinstance(node_item,(int,float)):
                                judge_path = "int(" + str(node_item) + ")" + " " + str(item_judge_state) + " " + "int(" + str(item_value) + ")"
                            else:
                                judge_path = "str(" + '"' + str(node_item) + '"' + ")" + " " + str(item_judge_state) + " "  + "str(" + '"' +  str(item_value) + '"' + ")"
                            if eval(judge_path):
                                # 调用推送信息
                                node_item_id = str(item_id) + str(node_ip)
                                inform = item_inform % node_ip
                                self.BigAlertAction.main(node_item_id, inform, item_handler, item_handler_manager)
                            else:
                                # 解除预警记录
                                self.BigAlertAction.Relieve_alert(node_item_id)
                    else:
                        continue
        else:
            print " 没有数据"

    def run(self):
        while True:
            redis_queue = self.conn_redis.redis_client.rpop("hdfs_queue")
            if redis_queue:
                queue_time = ""
                queue_time = redis_queue.split('+')[1].split('_')[0] + ' ' + redis_queue.split('+')[1].split('_')[1]
                print queue_time
                pre_time = int(time.mktime(time.strptime(queue_time,'%Y-%m-%d %H:%M:%S'))) + 1200
                now_time = int(time.time())
                if pre_time <  now_time:
                    time.sleep(60)
                    self.conn_redis.redis_client.delete(redis_queue)
                    continue
                table_queue = self.conn_redis.redis_client.smembers(redis_queue)
                print table_queue
                if table_queue:
                    for queue in table_queue:
                        self.Table_monitor(queue,queue_time)
                    time.sleep(120)
                self.conn_redis.redis_client.delete(redis_queue)
            else:
                time.sleep(60)
                continue


# 监控 爬虫实时数据流
#class MonitorSpider(multiprocessing.Process):

 #   def __init__(self):
  #      multiprocessing.Process.__init__(self)

# 监控系统层面
#class MonitorSystem(multiprocessing.Process):

    #def __init__(self):
        #multiprocessing.Process.__init__(self)
        #self.conn_redis = RedisConn.Redisaction()
        #self.conn_mongo = MongoConn.Mongo_client()
        #self.monitor_table = "MonitorBigdataItem"
        #self.BigAlertAction = handler.BigAlertAction()





if __name__ == '__main__':
    bigdata = MonitorBigdata()
    print "start"
    bigdata.start()
