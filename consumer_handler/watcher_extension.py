# coding=utf-8
#新采集scrapy 的监控的数据， 嵌入到scrapy 的extention 模块中
from twisted.internet import task
from scrapy import signals
import urllib2
import urllib
import time
import datetime
from scrapy import __version__
from kafka import KafkaProducer
import json
import psutil

import sys

class SpiderWatcher(object):

    def __init__(self, stats, interval=30.0):
        self.stats = stats
        self.interval = interval
        self.uuid = ""
        self.multiplier = 60.0 / self.interval
        self.collection_stats_task = None
        self.avg_rate_task = None
        self.item_empty_rate = 0.2
        self.spider_items_len = {}
        self.spider_count = {}
        self.items_avg_count = {}
        self.interval_avg = 20
        self.request_dropped_count = {}
        self.ip = psutil.net_if_addrs()['eth0'][0][1]

    def send_message_to_monitor(self, message, action):
        message['action'] = action
        data = urllib.urlencode(message)
        try:
            req = urllib2.Request('http://10.10.77.136:8888/spider/watcher', data)
            urllib2.urlopen(req, timeout=3)
        except Exception as e:
            print e
    
    def conn_kafka(self):
        try:
            producer = KafkaProducer(bootstrap_servers=['10.10.183.246:9092', '10.10.61.29:9092', '10.10.40.200:9092'])
            return producer
        except:
            return None

    def send_message_to_kafka(self,message,action):
        message['action'] = action
        message["uuid"] = self.uuid
        send_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        message["send_time"] = send_time
        message["ip"] = self.ip
        message = json.dumps(message)
        try:
            self.kafka_client.send("mc-scrapy-stream",message)
        except Exception as e:
            print e 

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat('LOGSTATS_INTERVAL')
        ext = cls(crawler.stats, interval)
        ext.statsClt = crawler.stats
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        return ext

    def spider_opened(self, spider):
        try:
            self.uuid = spider.uuid
        except:
            self.uuid = ""
        self.spider_count[spider.name] = {'pagesprev': 0,
                                          'itemsprev': 0}

        self.items_avg_count[spider.name] = {'items': 0}
        self.item_abnormal_count = {spider.name: 0}
        # 连接 kafka
        self.kafka_client = self.conn_kafka()

        self.crawl_init(spider)
        self.collection_stats_task = task.LoopingCall(self.crawl_rate, spider)
        #self.avg_rate_task = task.LoopingCall(self.crawl_rate_avg, spider)

        #self.avg_rate_task.start(self.interval_avg)
        self.collection_stats_task.start(self.interval)
        print 'end of spider_opened\n' * 10
        sys.stdout.flush()

    def spider_closed(self, spider):
        spider_stats = self.statsClt.get_stats()
        start_time = spider_stats["start_time"] + datetime.timedelta(hours=8)
        finish_time = spider_stats["finish_time"] + datetime.timedelta(hours=8)

        # 别忘记关定时任务
        if self.collection_stats_task and self.collection_stats_task.running:
            self.collection_stats_task.stop()

        if self.avg_rate_task and self.avg_rate_task.running:
            self.avg_rate_task.stop()

        spider_stats.update({"download/request_dropped": self.request_dropped_count[spider.name]})
        spider_cls_stats = spider_stats.copy()
        spider_cls_stats.pop("start_time")
        spider_cls_stats.pop("finish_time")

        msg = {"spider_name": spider.name,
               "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
               "finish_time": finish_time.strftime('%Y-%m-%d %H:%M:%S'),
               "request_stats": spider_cls_stats,
               "data_stats": {"abnormal": self.item_abnormal_count[spider.name],
                              "total": self.stats.get_value('item_scraped_count', 0)},
               "spider_rate": 0}
        kafka_msg = msg.copy()
        kafka_msg["request_stats"] = spider_cls_stats
        self.send_message_to_kafka(kafka_msg, "spider_closed")
        #self.send_message_to_monitor(msg, "spider_closed")
        self.kafka_client.close()

    def crawl_init(self, spider):
        spider_stats = self.statsClt.get_stats()
        start_time = spider_stats["start_time"] + datetime.timedelta(hours=8)

        if spider.name not in self.request_dropped_count:
            self.request_dropped_count[spider.name] = 0

        spider_stats.update({"download/request_dropped": self.request_dropped_count[spider.name]})
        spider_cls_stats = spider_stats.copy()
        spider_cls_stats.pop("start_time")

        msg = {
            "spider_name": spider.name,
            "request_stats": spider_cls_stats,
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "data_stats": {"abnormal": self.item_abnormal_count[spider.name], "total": 0},
            "spider_rate": 0,
            "spider_version": __version__
        }
        kafka_msg = msg.copy()
        kafka_msg["request_stats"] = spider_cls_stats
        self.send_message_to_kafka(kafka_msg, "spider_open")
        #self.send_message_to_monitor(msg, "spider_open")

    def crawl_rate_avg(self, spider):
        items = self.items_avg_count[spider.name]['items']
        self.items_avg_count[spider.name]['items'] = 0
        spider_stats = self.statsClt.get_stats()
        start_time = spider_stats["start_time"] + datetime.timedelta(hours=8)

        msg = {
            "spider_name": spider.name,
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "spider_avg_rate": items
        }
        self.send_message_to_monitor(msg, "spider_avg_rate")

    def crawl_rate(self, spider):
        items = self.stats.get_value('item_scraped_count', 0)
        irate = (items - self.spider_count[spider.name]['itemsprev']) * self.multiplier
        self.spider_count[spider.name]['itemsprev'] = items
        spider_stats = self.statsClt.get_stats()
        start_time = spider_stats["start_time"] + datetime.timedelta(hours=8)
        spider_stats.update({"download/request_dropped": self.request_dropped_count[spider.name]})
        spider_cls_stats = spider_stats.copy()
        spider_cls_stats.pop("start_time")

        msg = {"spider_name": spider.name,
               "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
               "request_stats": spider_cls_stats,
               "data_stats": {"abnormal": self.item_abnormal_count[spider.name], "total": items},
               "spider_rate": irate,
               }
        kafka_msg = msg.copy()
        kafka_msg["request_stats"] = spider_cls_stats
        self.send_message_to_kafka(kafka_msg, "spider_stats")
        #self.send_message_to_monitor(msg, "spider_stats")

    def item_scraped(self, item, spider):

        if item:
            item_name = item.__class__.__name__
            if spider.name not in self.spider_items_len:
                self.spider_items_len[spider.name] = {}
            if item_name not in self.spider_items_len[spider.name]:
                self.spider_items_len[spider.name][item_name] = len(item)

            item_empty_count = 0
            item_count = 0
            for attr in item:
                if not item[attr]:
                    try:
                        if int(item[attr]) == 0:
                            pass
                    except:
                         item_empty_count += 1
                item_count += 1

            if item_empty_count > self.spider_items_len[spider.name][item_name] * self.item_empty_rate:
                self.item_abnormal_count[spider.name] += 1
            elif item_count > self.spider_items_len[spider.name][item_name]:
                self.item_abnormal_count[spider.name] += 1

            self.items_avg_count[spider.name]['items'] += 1

    def request_dropped(self, spider):
        if spider.name not in self.request_dropped_count:
            self.request_dropped_count[spider.name] = 1
        else:
            self.request_dropped_count[spider.name] += 1
