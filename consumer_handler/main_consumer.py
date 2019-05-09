#coding:utf-8
# 用于 kafka 第一次消费，合并存入kafka
# 这个消费的线程可以随意起多少个进程，最好是根据kafka 集群的分区来
from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading
import time
import ast
from Queue import Queue
import json

class Consumer_spider(threading.Thread):
    def __init__(self, que, topic="None", interval=60):
        threading.Thread.__init__(self)
        self.q = que
        self.topic = topic
        self.interval = interval
        self.data_queue = {}
        self.data_dict = {}
        self.pre_time = 0

    def consumer(self):
        try:
            consumer = KafkaConsumer(group_id="spider_test",bootstrap_servers=['10.10.183.246:9092', '10.10.61.29:9092', '10.10.40.200:9092'])
            consumer.subscribe(topics=(self.topic,))
            return consumer
        except:
            return None

    def run(self):
        while True:
            consumer = self.consumer()
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
                            value["finish_time"] = 0
                            self.data_dict[key] = value
                        else:
                            value["finish_time"] = 0
                            self.data_dict[key] = value
                    elif action == "spider_stats":
                        if key not in self.data_dict.keys():
                            value["finish_time"] = 0
                            self.data_dict[key] = value
                        else:
                            tmp = self.data_dict[key]
                            now_send = int(time.mktime(time.strptime(value["send_time"],'%Y-%m-%d %H:%M:%S')))
                            pre_send = int(time.mktime(time.strptime(tmp["send_time"],'%Y-%m-%d %H:%M:%S')))
                            if now_send >= pre_send:
                                tmp["data_stats"] = value["data_stats"]
                                tmp["request_stats"] = value["request_stats"]
                                tmp["spider_rate"] = value["spider_rate"]
                                tmp["finish_time"] = 0
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

class Producer_spider(threading.Thread):
    def __init__(self, que, produce_topic):
        threading.Thread.__init__(self)
        self.q = que
        self.produce_topic = produce_topic

    def producer(self):
        try:
            producer = KafkaProducer(bootstrap_servers=['10.10.183.246:9092', '10.10.61.29:9092', '10.10.40.200:9092'])
            return producer
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
            producer = self.producer()
            consumer_data = self.q.get()
            if consumer_data:
                for value in consumer_data.values():
                    value["request_stats"] = self.handle_request_status(value["request_stats"])
                    value = json.dumps(value)
                    try:
                        producer.send(self.produce_topic, value)
                    except:
                        print "写入失败"
            else:
                time.sleep(3)

if __name__=='__main__':
    que = Queue()
    print "start ....."
    consumer_thread = Consumer_spider(que,"mc-scrapy-stream",interval=60)
    consumer_thread.start()
    producer_thread = Producer_spider(que,"spider_stream_again")
    producer_thread.start()
