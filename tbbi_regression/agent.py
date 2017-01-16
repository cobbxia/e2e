# -*- coding:utf8 -*-
#from http.server import *
from TransferUtility import GlobalConf,getTableList,do_odps_cmd,do_odps_filecmd,getResouceList
import traceback
import BaseHTTPServer
#import urllib.request as urllib
import urllib

#from checkUtility import check,getCurRunPosInfo
#import socketserver
import SocketServer
import io,shutil,time,re
import threading
import os
import sys
from setting import processDict

#singleton model superclass
class Singleton(object):
    def __new__(cls,*args,**kw):
        if not hasattr(cls,'_instance'):
            orig=super(Singleton,cls)
            cls._instance=orig.__new__(cls,*args,**kw)
        return cls._instance

def splitArg(para):
    retDict={}
    argList=para.split("&")
    for item in argList:
        equalIndex=item.find("=")
        key=item[0:equalIndex]
        val=item[equalIndex+1:]
        retDict[key]=val
    return retDict

def parseContent(content):
    r_str="error!"
    print(content)
    if content.startswith("/"):
        func=content.split("?")[0].lstrip("/")
        param="".join(content.split("?")[1:])
        r_str=("param:%s\nfunc:%s\n" % (param,func))
    argDict=splitArg(param)
    request=Request(argDict)
    return (func,request)

class MyThreadingHTTPServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):  
    pass  

#http handle class get()/post() method
class DataHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        gc=GlobalConf()
        gc.log.info("URL:%s" % self.path)
        if self.path=='/':time.sleep(5)
        try:
            (func,request)=parseContent(self.path)
        except Exception as e:
            print("url:%s path parse error:%s" % (self.path,format(str(e))))
        
        #elif func=="check":
        #    r_str=checkWrapper(request)
        if func in processDict:
            try:
                r_str=processDict[func](request)
            except Exception as e:
                r_str="func:%s execute error:%s" % (func,format(str(e)))
                print(r_str)
        else:
            r_str="function:%s not defined,processDict:%s" % (func,repr(processDict))
        print(r_str)
        gc.log.info(r_str)
        self.httpResponse(r_str)
    def httpResponse(self,r_str,enc="UTF-8"):
        #encoded=''.join(r_str).encode(enc)
        encoded=r_str
        gc=GlobalConf()
        gc.log.info("encoded:%s enc=%s" % (encoded,enc))
        f=io.BytesIO()
        f.write(encoded)
        f.seek(0)
        self.send_response(200)
        self.send_header("Content-type","text/html;charset=%s" % enc)
        self.send_header("Content-Length",str(len(encoded)))
        self.end_headers()
        shutil.copyfileobj(f,self.wfile)
        
class Task:
#status:-1 busy; >=0 represents a thread number; -2 can be deleted
    def __init__(self,**kargs):
        self.taskId=0
        self.content=""
        self.func=""
        self.request=Request("")
        self.status=-1
        if "content" in kargs: 
            self.content=kargs["content"]
        if  "func" in kargs:  
            self.func=kargs["func"]
        if "request" in kargs:
            self.request=kargs["request"]
    def setStatus(self,status):
        self.status=status
    def getStatus(self):
        return self.status
    def getContent(self):
        return self.content

#class Request to handle request.GET.get('area')
class method:
    def __init__(vDict):
        self.__getDict=vDict
    def get(self,key):
        if key in self.__getDict:
            return self.__getDict[key]
        else:
            return None

class Request:
    def __init__(self,vDict):
       self.GET={}
       for k in vDict:
           self.GET[k]=urllib.unquote(vDict[k])

#start httpserver and httpRequestHandle
def runServer(server_class=BaseHTTPServer.HTTPServer, handler_class=BaseHTTPServer.BaseHTTPRequestHandler):
    port=8888
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print ("time:%s--------------------------START SERVICE PORT:%s--------------------------------------------" % (nowTime,port))
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    while not os.path.exists("./killfile"):
        httpd.handle_request()

if __name__ == '__main__':
    try:
        gc=GlobalConf()
        if gc.load("./config.py","test.log")!=0:
            print("can not load config file:%s" % (configFile))
            sys.exit(1) 
        runServer(MyThreadingHTTPServer,handler_class=DataHttpHandler)
    except Exception as e:
        print("exception:%s" % (format(str(e))))
