# -*- coding: utf-8 -*-
#from SimpleXMLRPCServer import SimpleXMLRPCServer   
#import threading
#class Mhandler(object):
#    def add(x, y):
#        return x + y    
#
#class Monitor(threading.Thread):
#    def __init__(self):
#        threading.Thread.__init__(self)
#    def run(self):
#        #s = SimpleXMLRPCServer(('127.0.0.1', 8080))
#        s = rpc.server.RPCServer(('127.0.0.1', 8080))
#        s.register_function(add)
#        s.serve_forever()
#
#if __name__ == '__main__':
#    monitor=Monitor()
#    monitor.start()
#    monitor.join()
#是一个绑定了本地8080端口的服务器对象，register_function()方法将函数add注册到s中。serve_forever()启动服务器。 再给个客户端client.py：
from http.server import *
import urllib.request as urllib
#from checkUtility import check,getCurRunPosInfo
import socketserver
import io,shutil,time,re
import threading
import configparser
import os
import sys
from Log import Log
from commonUtility import cur_file_dir
from setting import urlpatterns


def runServer(server_class=HTTPServer, handler_class=BaseHTTPRequestHandler):
    port=int(queue.oConfig.get("agent","port"))
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print ("time:%s--------------------------START SERVICE PORT:%s--------------------------------------------" % (nowTime,port))
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

if __name__ == '__main__':
    try:
        workflow()
        runServer(MyThreadingHTTPServer,handler_class=DataHttpHandler)
    except Exception as e:
        print("exception:%s" % (format(str(e))))

