#coding:utf-8
#这是推送信息的规则模块，待完善

from pymongo.mongo_client import MongoClient
import time
from Common_dingding import DingdingActiveApi


def conn_mongo(collect_name):
     try:
         mongo_conn = MongoClient('mongodb://10.10.92.98:27017').streaming
         collection = mongo_conn[collect_name]
         return collection
     except:
         return None
        
class BigAlertAction(object):

    def __init__(self):
        self.dingdingApi = DingdingActiveApi()

    def main(self,item_id,inform_item,inform_handler,inform_handler_manager):
        handler = inform_handler
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        conn_display_alert = conn_mongo("deplay_alert")
        cur = conn_display_alert.find({"date":date,"item_id":item_id})
        if cur.count() != 0:
            for cur_data in cur:
                num = 0
                alert_num = cur_data["alert_num"]
                num = alert_num + 1
                result = num & (num - 1)
                if result == 0:          # 是2 的幂次方进行报警
                    print "yes"
                    alert_status = "true"
                    alert_time = int(time.time()) - int(time.mktime(time.strptime(cur_data["insert_time"],'%Y-%m-%d %H:%M:%S')))
                    if alert_time >= 3600:
                        handler = inform_handler_manager
                else:
                    # 不是2 的幂次方进行叠加
                    alert_status = "no"
                conn_display_alert.update({"date": date, "item_id": item_id}, {"$set": {"alert_num": num}})
        else:
            alert_status = "true"
            insert_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            conn_display_alert.insert({"date":date,"item_id":item_id,"alert_num":1,"insert_time":insert_time})

        if alert_status == "true":    # 进行推送信息
            self.dingdingApi.filter_handler(handler,inform_item)

    def Relieve_alert(self,item_id):
        conn_relieve_alert = conn_mongo("deplay_alert")
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        cur = conn_relieve_alert.find({"date":date,"item_id":item_id})
        if cur.count() != 0:
            conn_relieve_alert.remove({"date":date,"item_id":item_id})


