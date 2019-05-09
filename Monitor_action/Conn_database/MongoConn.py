#coding:utf-8
#
from pymongo.mongo_client import MongoClient

class Mongo_client(object):

    def __init__(self):
        self.mongo_conn = MongoClient('mongodb://10.10.92.98:27017').streaming

