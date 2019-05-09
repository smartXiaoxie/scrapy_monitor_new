#coding:utf-8

import tornado.web

class BeseHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("hello !")