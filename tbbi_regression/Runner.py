#!/home/tops/bin/python2.7
# -*- coding: utf-8 -*-
import gevent
from gevent.queue import Queue
from gevent import Greenlet
from gevent import monkey; monkey.patch_all()
import traceback, ConfigParser, sys,json,time, logging, commonUtility, threading, random,os, string,getopt
from commonUtility import do_cmd,getTableList,do_odps_cmd,do_odps_filecmd,getResouceList
from TransferUtility import GlobalConf,Usage
from setting import processDict,processFlagDict,preproDict,postproDict
from Mysql import getDragList,execMysql


G_MUTEX = threading.Lock()
G_PINDEX=-1


class Monitor(Greenlet):
    def __init__(self,gc,threadList,processor=""):
        Greenlet.__init__(self)
        self.gc=gc
        self.opList=gc.opList
        self.threadList=threadList
        self.processor=processor
        self.sleeptimes=600
    def run(self):
        while self.gc.isrunning == True:
            try:
                processedNum=0
                i=0
                while i < len(self.threadList):
                   processedNum=self.threadList[i].processedNum+processedNum
                   i=i+1
                if self.processor == "render":
                   sql='''update e2e.regression_regression set processedNum="%s" where dirname="%s"; ''' % (processedNum,self.gc.timestamp)
                   cmd="%s -N -e '%s'" % (self.gc.mysql,sql)
                   execMysql(self.gc,sql)
                open(self.gc.statusfile,"w").write("%d\t%d\t%d" % (processedNum,len(self.opList)-processedNum,len(self.opList)))
            except Exception as e:
                self.gc.log.fatal("['error', ('Exception: %s')]" % (traceback.format_exc()))
            k=0
            while k < self.sleeptimes:
                if self.gc.isrunning != True:
                    break
                gevent.sleep(1)
                k=k+1

def threadinfo(threadList):
    thtotal=0
    i=0
    while i < len(threadList):
        if threadList[i].is_alive() == True: thtotal=thtotal + 1  
        i=i+1
    return thtotal

class MultiContainer(Greenlet):
    def __init__(self,threadid,processor,gc):
        self.gc=gc
        Greenlet.__init__(self)
        self.processedNum=0
        self.threadid=threadid
        self.isAlive=True
        self.gc.log.debug("start init MultiContainer :%d,processor:%s" % (self.threadid,processor))
        self.processor=processDict[processor]
        self.opList=gc.opList
        self.gc.log.info("init MultiContainer :%d" % (self.threadid))
    def __str__(self):
        return self.processor
    def run(self):
        global G_PINDEX,G_MUTEX
        localindex=-1
        totalnum=len(self.opList)
        while  localindex<totalnum and not os.path.exists(self.gc.killfile) :
            try:
                if os.path.exists(self.gc.killfile) :
                    self.gc.log.info("killfile exists,execution over. filename:%s" % (self.gc.killfile))
                    break
                if G_MUTEX.acquire():
                    G_PINDEX=G_PINDEX+1
                    localindex=G_PINDEX
                    G_MUTEX.release()
                if localindex == totalnum or localindex > totalnum: 
                    self.gc.log.info("localindex:%d G_PINDEX:%d exceeds totalnum:%s in threadid:%d" % (localindex,G_PINDEX,totalnum,self.threadid))
                    break
                self.gc.log.info("localindex:%d G_PINDEX:%d of totalnum:%s in threadid:%d" % (localindex,G_PINDEX,totalnum,self.threadid))
                self.gc.log.info("localindex:%d item to process:%s" % (localindex,self.opList[localindex]))
                r=self.processor(self.gc,self.opList[localindex])
                self.gc.log.info("localindex:%d item:%s processed over" % (localindex,self.opList[localindex]))
            except Exception as e:
                self.gc.log.fatal("localindex:%d G_PINDEX:%d of totalnum:%s in threadid:%d Exception: %s" % (localindex,G_PINDEX,totalnum,self.threadid,traceback.format_exc()))
            self.processedNum=self.processedNum+1
        self.isAlive=False

def MultiContainerRunner(processor,gc):
    log=gc.log
    opList=gc.opList
    threads=[]
    print("start MultiRunner cls:%s" % (processor))
    log.info("opList length:%d start MultiRunner cls:%s" % (len(opList),processor))
    if processor in preproDict:
        gc.log.info("processor:%s in preproDict starts" % (processor))
        preproDict[processor](gc)
        gc.log.info("processor:%s in preproDict over" % (processor))
    if gc.threadnum >  len(opList): 
        gc.threadnum=len(opList)
    gc.isrunning=True
    for index in range(gc.threadnum):
        ins=MultiContainer(index,processor,gc)
        threads.append(ins)
    log.info("init %s thread finished." % (processor))
    log.info("start %s thread." % (processor))
    for index in range(gc.threadnum):
        log.info("create thread id:%d threadnum:%d" % (index,gc.threadnum))
        threads[index].start()
    log.info("monitor thread inited.")
    monitor=Monitor(gc,threads,processor)
    log.info("monitor thread started. ")
    monitor.start()
    flag=0
    threadstatuslist=[]
    for index in range(gc.threadnum): 
        threadstatuslist.append(1)
    while flag == 0:
        for index in range(gc.threadnum): 
            if threadstatuslist[index]==1:
                threads[index].join(60)
                if not threads[index].isAlive:
                    threadstatuslist[index]=0
                    log.info("destroy thread id:%d" % (index))
        flag=1
        for index in range(gc.threadnum):
            if threadstatuslist[index] == 1:
                flag=0
    log.info("destroyed all running threads.")
    gc.isrunning=False
    log.info("destroy monitor thread,isrunning:%s" % (gc.isrunning))
    while monitor.isAlive == True:
        gevent.sleep(60)
    log.info("monitor thread finished.")
    print("%s finished." % (processor))
    if processor in postproDict:
        gc.log.info("processor:%s in postproDict starts" % (processor))
        postproDict[processor](gc)
        gc.log.info("processor:%s in postproDict over" % (processor))
    if len(gc.failList) >0:
        log.fatal("failitem:%s" % ("\t".join([failitem for failitem in gc.failList])))

def mod(gc,verifymailurl,status="error"):
    r=do_cmd(cmd)
    gc.log.info("cmd:%s r1:%s r2:%s" % (cmd,r[1],[2]))


def main():
    try:
        opList=[]
        print(sys.argv,len(sys.argv))
        if len(sys.argv) < 2:
            print("Usage")
            Usage()
        gc=GlobalConf()
        if gc.load(argv=sys.argv)!=0:
            print("can not load config file:%s,operator=%s" % (configFile,operator))
            sys.exit(1)
        log=gc.log
        operator=gc.operator
        print("start "+" ".join(sys.argv))
        log.info("start "+" ".join(sys.argv))
        if operator in ["sleep","tag","datacopy","result","copy","odpstable","table","part","column","test"]:
            getTableList(gc)
            opList=gc.tableList
        elif operator == "verify":
            opList=gc.verifyplanList
            mod(gc,"","info")
        elif operator == "render":
            opList=gc.renderList
        elif operator == "resource":
            getResouceList(gc)
            opList=gc.resourceList
        elif operator in processFlagDict:
            dragList=getDragList(gc,processFlagDict[operator])
            opList=dragList
        else:
            Usage()
        gc.opList=opList
        MultiContainerRunner(operator,gc)
        print("finish "+" ".join(sys.argv))
        log.info("finish "+" ".join(sys.argv))
    except Exception as e:
        print("['error', ('Exception: %s')]" % (traceback.format_exc()))
        gc.log.fatal("['error', ('Exception: %s')]" % (traceback.format_exc()))
        return 1

if __name__ == '__main__':
    main()
