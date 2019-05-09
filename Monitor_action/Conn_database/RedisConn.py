#coding:utf-8
# 连接redis
import redis

class Redisaction(object):

    def __init__(self):
        self.redis_client = redis.Redis(host='10.10.73.23',port=6379)