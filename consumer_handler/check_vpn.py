#coding:utf-8
# 统计vpn 的利用率,消费kafka 的topic进行格式化处理到redis 里面
from kafka import KafkaConsumer
import redis
import mysql.connector
import threading
from Queue import Queue
import time
import ast
import datetime

class consumer_vpn_streaming(threading.Thread):
    def __init__(self,que,interval=60):
        threading.Thread.__init__(self)
        self.interval = interval
        self.data_dict = {}
        self.data_queue = []
        self.que = que

    def read_kafka(self):
        try:
            consumer = KafkaConsumer(bootstrap_servers=['10.10.183.246:9092', '10.10.61.29:9092', '10.10.40.200:9092'])
            consumer.subscribe(topics=("mc_scrapy_vpn",))
            return consumer
        except:
            return consumer

    def run(self):
        while True:
            consumer = self.read_kafka()
            if not consumer:
                print " 连接不上kafka"
                break
            print " 能连接上kafka"
            self.data_dict = {}
            self.data_dict = {}
            self.ssy_100_list = []
            self.ziqie_yjxy_list = []
            self.vpn_106_list = []
            self.pre_time = int(time.time()) + self.interval
            while True:
                message = consumer.poll(timeout_ms=5)
                for msg in message.values():
                    vpn_item_list = []
                    value = ast.literal_eval(msg[0].value)
                    key = value["ip_group"]
                    vpn_item_list.append(value["ip"])
                    vpn_item_list.append(value["proxy_ip"])
                    vpn_item_list.append(value["pptp_time"])
                    start_time = int(time.mktime(time.strptime(value["start_time"],'%Y-%m-%d %H:%M:%S')))
                    vpn_item_list.append(start_time)
                    status_flag = 0
                    pptp_status = value["pptp_status"]
                    if pptp_status == "True":
                        status_flag = 1
                    elif pptp_status == "False":
                        status_flag = 0
                    vpn_item_list.append(status_flag)
                    vpn_item_list = tuple(vpn_item_list)
                    if key == "ssy_100":
                        self.ssy_100_list.append(vpn_item_list)
                        self.data_dict[key] = self.ssy_100_list
                    elif key == "ziqie_yjxy":
                        self.ziqie_yjxy_list.append(vpn_item_list)
                        self.data_dict[key] = self.ziqie_yjxy_list
                    elif key == "vpn_106":
                        self.vpn_106_list.append(vpn_item_list)
                        self.data_dict[key] = self.vpn_106_list
                now_time = int(time.time())
                if now_time >= self.pre_time:
                    try:
                        consumer.close()
                    except:
                        pass
                    self.queue = self.data_dict.copy()
                    self.que.put(self.queue)
                    time.sleep(10)
                    break

class write_data(threading.Thread):
    def __init__(self, que):
        threading.Thread.__init__(self)
        self.q = que

    def conn_redis(self):
        r = redis.Redis(host='127.0.0.1',port=6379)
        return r

    def conn_mysql(self):
        try:
            client = mysql.connector.connect(host='',user='',passwd='',database='')
            return client
        except:
            return None

    def redis_pipline(self,sum_key,num_key,key,list):
        redis_client = self.conn_redis()
        list_num = len(list)
        redis_client.hincrby(num_key,key,list_num)
        with redis_client.pipeline(transaction=False) as p:
            for ip in list:
                p.sadd(key,ip)
                p.sadd(sum_key,ip)
            p.execute()
            time.sleep(1)

    def run(self):
        while True:
            mysql_client = self.conn_mysql()
            cur = mysql_client.cursor()
            day = datetime.date.today()
            month = day.strftime('%Y-%m')
            num_key = "vpn_num:" + str(month)
            sum_key = "vpn_sum:" + str(month)
            consumer_queue = self.q.get()
            if consumer_queue:
                for key,value in consumer_queue.items():
                    redis_list = []
                    redis_key = str(key) + ':' + str(month)
                    value = tuple(value)
                    insert_sql = "insert into " + str(key) + "(ip,proxy_ip,pptp_time,start_time,pptp_status) VALUES (%s,%s,%s,from_unixtime(%s),%s)"
                    try:
                        cur.executemany(insert_sql,value)
                        mysql_client.commit()
                    except:
                        print "插入失败"
                    # 插入redis
                    for item in value:
                        redis_list.append(item[1])
                    self.redis_pipline(sum_key,num_key,redis_key,redis_list)
                mysql_client.close()
            else:
                time.sleep(2)

if __name__=='__main__':
    que = Queue()
    consumer_thread = consumer_vpn_streaming(que,interval=60)
    consumer_thread.start()
    write_queue = write_data(que)
    write_queue.start()
