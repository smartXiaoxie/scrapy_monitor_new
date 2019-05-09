#-*-coding:utf-8-*-
# import urllib
#推送信息接口，微信或者钉钉
import urllib2
import json
import sys
reload(sys)
from pymongo.mongo_client import MongoClient
sys.setdefaultencoding('utf-8')

class DingdingActiveApi(object):

    def __init__(self):
        self.header_type = {'Content-Type': 'application/json', 'charset': 'utf-8'}

    def conn_mongo(self):
        mongo_conn = MongoClient('mongodb://10.10.92.98:27017').streaming
        collection = mongo_conn["spider_map_user"]
        return collection

    def push_dingding_message(self,url,msgtype,content,touser):
        data = {
            "msgtype": msgtype,
            "text": {
                "content": content
            },
            "at": {
                "atMobiles": [
                    touser
                ],
                "isAtAll": "false"
            }
        }
        print data
        jdata = json.dumps(data, ensure_ascii=False).encode("utf-8")
        print jdata
        request = urllib2.Request(url=url,headers=self.header_type,data=jdata)
        response = json.loads(urllib2.urlopen(request).read())
        return response['errcode'], response['errmsg'],response

    def filter_handler(self,handler,content):
        print handler,  content
        handler_alert_conn = self.conn_mongo()
        for user_data in handler_alert_conn.find({"user_name":handler}):
            mobile = user_data["mobiles"]
            url = user_data["url"]
        # 开始的推送信息
        content = "EVENT: " + str(content) + "\n" + "handler: " + str(handler)
        try:
            self.push_dingding_message(url,"text",content,mobile)
            print "推送信息成功"
        except:
            print " 推送信息失败"

