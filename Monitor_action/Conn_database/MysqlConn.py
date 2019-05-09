#coding:utf-8
import mysql.connector

class Mysqlaction(object):

    def __init__(self):
        self.conn = mysql.connector.connect(host='106.75.100.138',port=3307,user='',passwd='',database='')