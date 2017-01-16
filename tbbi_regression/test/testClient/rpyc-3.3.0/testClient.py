# coding=utf-8
# client.py
#-----------------------------------------------

import rpyc,logging
import sys
import time
import pickle
import os,subprocess,json,ConfigParser
class Singleton(object):
    def __new__(cls,*args,**kw):
        if not hasattr(cls,'_instance'):
            orig=super(Singleton,cls)
            cls._instance=orig.__new__(cls,*args,**kw)
        return cls._instance



class GlobalConf(Singleton):
    def __init__(self):
        pass  
    def set(self,key,val):
        self.__dict__[key]=val
    def setConf(self,key,section,option,default=""):
        if self.config.has_option(section,option):
            self.__dict__[key]=self.config.get(section,option)
        else:
            self.__dict__[key]=default
        if hasattr(self,"log"):
            self.log.info("%s:%s" % (key,self.__dict__[key]))
    def load(self,configFile,logFileName="testClient.log"):
        self.config= ConfigParser.ConfigParser()
        self.config.read(configFile)
        self.setConf("logname","log","logname","main")
        self.setConf("loglevel","log","level","debug")
        self.setConf("logdir","log","logdir","./log")
        self.setConf("logfile","log","logfile",self.logdir+"/"+logFileName)
        self.numeric_level = getattr(logging, self.loglevel.upper(),None)
        fms = '%(levelname)-12s %(asctime)s %(name)-8s [%(thread)d][%(threadName)s][%(module)s:%(funcName)s:%(lineno)d] %(message)s'
        logging.basicConfig(level=self.numeric_level,filename=self.logfile,format=fms)
        self.log=logging.getLogger('main')

        #################################################################################################
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter(fms)
        console.setFormatter(formatter)
        logging.getLogger('main').addHandler(console)
        #################################################################################################
        #################################################################################################
        self.stime=open(self.timestampfile).readlines()[0].strip("\n").replace(":","")
        self.setConf("baselogdir","odps","baselogdir",self.verifypath+"/logs/"+self.stime)
        if not os.path.exists(gc.baselogdir):
            os.makedirs(gc.baselogdir)
        self.etime=""
        self.succList=[]
        self.failList=[]
        for line in open(self.baseSuccfile,"r"):
            nodeid=line.strip("\n").split("\t")[0] 
            self.succList.append(nodeid)
        for line in open(self.baseFailfile,"r"):
            nodeid=line.strip("\n").split("\t")[0] 
            self.failList.append(nodeid)
        return 0
    def updateEtime(self):
        gc.log.info("updateEtime() begins")
        gc.etime=time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
        open(gc.endtimehistory,"a").write(gc.etime+"\n")
        gc.log.info("updateEtime() over")
    def getetime(self):
        return open(self.endtimehistory,"r").readlines()[-1].strip("\n")

def LogWrapper(function):
    def wrap_function(*args, **kwargs):
        GlobalConf().log.info("%s() begins" % (function.__name__))
        ret=function(*args, **kwargs)
        GlobalConf().log.info("%s() over" % (function.__name__))
        return ret
    return wrap_function

def do_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return p.returncode, out, err 


def getProject(gc,tableName):
    retProjectName=""
    tableDict={}
    projectURL=getProjectURL(tableName)
    gc.log.info(projectURL)
    cmd='''curl %s 2> /dev/null''' % (projectURL)
    r=do_cmd(cmd)
    if r[0] == 0:
        js=json.loads(r[1])
        i=0
        for item in js['result']:
            i=i+1
            guid=item['guid']
            if guid.split(".")[0].lower() != "odps":
                continue
            guidProjectName=guid.split(".")[1]
            guidTableName=guid.split(".")[2]
            if guidTableName == tableName and (retProjectName == "" or guidProjectName != "tbbi_isolation"):
                retProjectName=guidProjectName
    return retProjectName

@LogWrapper
def caller(remoteHost,**argdict):
    c=rpyc.connect(remoteHost,12233)
    infos=c.root.process(argdict)
    c.close()
    return infos    


def getNonDict(gc):
    retDict={}
    infile=gc.addtablePath+"/"+"nonExistTables.txt"
    return  pickle.load(open(gc.addtablePath+"/"+"nonExistTables.pk","rb"))
    for line in open(infile,"r"):
        if line == None or line == "":
            continue
        tablename=line.split()[0]
        projectname=line.split()[1]
        retDict[tablename]=projectname
    return retDict

@LogWrapper
def docount(gc,func,resfile,issucc):
    starttime=gc.stime
    endtime=gc.etime
    sum=0
    infofilename=gc.addtablePath+"/"+resfile
    gc.log.info("infofilename"+infofilename)
    infofile=open(infofilename,"w")
    for ip in ipList:
        gc.log.info(ip)
        argdict={}
        argdict["starttime"]=starttime
        key="Table not found"
        infos=""
    	infos=caller(ip,ip=ip,starttime=starttime,endtime=endtime,key=key,func=func,issucc=issucc)
        infofile.write(infos+"\t"+"\n")
        sum=sum+len(infos.split("\n"))
    infofile.close()
    gc.log.info(sum)

@LogWrapper
def dogetFailLogFile(gc,nodeid):
    starttime=gc.stime
    endtime=gc.etime
    ipList=[]
    func="getFailLogFile"
    for ip in ipList:
        infos=caller(ip,ip=ip,starttime=starttime,endtime=endtime,nodeid=nodeid,func=func)
        if infos == "": continue
        return infos

@LogWrapper
def getFailLogFile(gc,resfile):
    for nodeid in open(resfile,"r").readlines():
        nodeid=nodeid.strip("\n")
        dogetFailLogFile(gc,nodeid)

@LogWrapper
def domain(gc):
    starttime=gc.stime
    ipList=[]
    outList=[]
    filterDict=getNonDict()
    for ip in ipList:
        gc.log.info(ip)
        argdict={}
        argdict["starttime"]=starttime
        key="Table not found"
    	infos=caller(ip,ip=ip,starttime=starttime,key=key,func="helloworld")
        gc.log.info(infos)
        continue
        for info in infos.split("\n"):
            if info == None or info == "":
                continue
            gc.log.info(ip,info)
            tablename=info
            if tablename not in outList:
                outList.append(tablename)
    filerDict=getNonDict()
    tailfix=time.strftime('%Y%m%d',time.localtime(time.time()))
    outfile=gc.addtablePath+"/"+"out.txt."+tailfix 
    gc.log.info(outfile)
    fout=open(outfile,"w")
    for tableName in outList:
        key=tableName.split()[0]
        val=tableName.split()[1]
        if key in filterDict and val == filterDict[key]:
            continue
    fout.close()

@LogWrapper
def difffail(gc,failfile):
    d1={}
    d2={}
    retList=[]
    for line in open(gc.baseFailfile,"r"):
        line=line.strip("\n")
        nodeid=line.split("\t")[0]
        ip=line.split("\t")[1]
        d2[nodeid]=ip
    for line in open(failfile,"r"):
        line=line.strip("\n")
        nodeid=line.split("\t")[0]
        ip=line.split("\t")[1]
        if nodeid not in d2:
            d1[nodeid]=""
    for k in d1:
        retList.append(k)
    return retList

@LogWrapper
def download(gc,failList):
    for line in failList:
        line=line.strip("\n")
        nodeid=line
        content=dogetFailLogFile(gc,nodeid)
        if content is None: continue
        nodefile=open(gc.baselogdir+"/"+nodeid,"w")
        gc.log.info("nodeid:%s nodefile:%s" % (nodeid,gc.baselogdir+"/"+nodeid))
        nodefile.write(content)
        nodefile.close()

@LogWrapper
def killrunning(gc,UUList):
    op="KILL_BY_MANUAL"
    for line in UUList:
        taskid=line.strip("\n") 
        cmd=""
        gc.log.debug(cmd)
        r=do_cmd(cmd)
        if r[0]==0:
            gc.log.info("kill taskid:%s successfully.stderr:%s" % (taskid,r[2]))
        else:
            gc.log.info("kill taskid:%d failed.stdout:%s\nstderr:%s" % (taskid,r[1],r[2]))
    gc.log.info("%d killed totally" % (len(UUList)))

@LogWrapper
def runUU(gc,UUList):
    gc.log.info("start runUU number:%d" % (len(UUList)))
    taskids=",".join([line.strip("\n") for line in UUList])
    cmd=""
    gc.log.debug(cmd)
    r=do_cmd(cmd)
    if r[0]==0:
        gc.log.info("start up successfully,task total number:%d" % (len(UUList)))
        gc.log.info(r[2])
    else:
        gc.log.info("start up failed,task total number:%d" % (len(UUList)))
        gc.log.info("stdout:"+r[1])
        gc.log.info("stderr:"+r[2])
    gc.updateEtime()
    gc.log.info("over runUU number:%d" % (len(UUList)))

#持续等待十次发现都没有正在运行中的任务，就认为任务运行结束
@LogWrapper
def waitforStatus(gc):
    gc.log.info("waitforStatus starts")
    retrytimes=int(gc.retrytimes)
    runningCheckSpan=int(gc.runningCheckSpan)
    while 0 == 0:
        i=1
        while i < retrytimes and countRunning(gc)  == 0:
            gc.log.info("waitforStatus retry %d times,sleep %d" % (i,runningCheckSpan))
            time.sleep(runningCheckSpan)
            i=i+1
        if i == retrytimes:
            gc.log.info("after %d times retry,still not running ,exit" % (i))
            return 0
        gc.log.info("wait for running,sleep %d" % (runningCheckSpan*retrytimes))
        time.sleep(runningCheckSpan*retrytimes)
    gc.log.info("waitforStatus over")

@LogWrapper
def countRunning(gc):
    sql='''select status,count(*) from phoenix_task_inst where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0 group by status'''
    gc.log.info(sql)
    cmd="java -jar %s \"%s\"" % (gc.toracle,sql)
    r=do_cmd(cmd)
    statusdict={}
    if r[0] == 0:
        for line in r[1].split("\n"):
            if line == "":continue
            status=line.split("\t")[0].split(":")[1]
            cnt=line.split("\t")[1].split(":")[1]
            statusdict[status]=cnt
    gc.log.info("statusdict:%s" % (str(statusdict)))
    if '4' not in statusdict:
        statusdict['4']=0
    ret=statusdict['4']
    return ret


@LogWrapper
def update(gc):
    sql='''update  phoenix_task_inst set status=6  where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0 and status=5'''
    gc.log.info(sql)
    cmd="java -jar %s \"%s\"" % (gc.toracle,sql)
    r=do_cmd(cmd)
    return r[0]

@LogWrapper
def getids(gc,status=""):
    gc.log.info("getids() begins")
    if status !="" and int(status) >0 and int(status) < 10:
        sql="select task_inst_id,node_def_id from phoenix_task_inst  where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0  and status="+status
    else:
        sql="select task_inst_id,node_def_id from phoenix_task_inst  where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0"
        
    gc.log.info(sql)
    cmd="java -jar %s \"%s\"" % (gc.toracle,sql)
    r=do_cmd(cmd)
    taskidList=[]
    task2nodeDict={}
    if r[0] == 0:
        for line in r[1].split("\n"):
            line=line.strip("\n")
            if line == "": continue
            taskid=line.split('\t')[0].split(":")[1]
            nodeid=line.split('\t')[1].split(":")[1]
            if nodeid in gc.succList:
                gc.log.debug("line:%s  taskid:%s  nodeid:%s" % (line,taskid,nodeid))
                taskidList.append(taskid)
                task2nodeDict[taskid]=nodeid
    return task2nodeDict

@LogWrapper
def proceed(gc):
    update(gc)
    taskidList=getids(gc,"1").keys()
    gc.log.debug("taskidList:%s" % (taskidList))
    if len(taskidList)==0:
        gc.log.info("taskidList is empty,exit")
        return 1
    runUU(gc,taskidList)
    return 0

#done:make tiemstamp changeable
def refresh(gc):
    resfile="taskid/fail_id_"+gc.stime+".txt"
    respath=gc.addtablePath
    docount(gc,"getcount",resfile,"fail")
    failfilename=respath+"/"+resfile
    failList=difffail(gc,failfilename)
    download(gc,failList)
    statistics(gc)

@LogWrapper
def statistics(gc):
    message=""
    httpprefix=gc.httpprefix+"/"+gc.stime
    p=gc.baselogdir
    errorfile=p+"/error.html"
    gc.log.info("FAILED uniq")
    message += "FAILED uniq:<br>\n"
    cmd='''grep FAILE %s/*|grep -v "Table not foun" ''' % p
    gc.log.info("cmd:"+cmd)
    failedContent=do_cmd(cmd)[1]
    if failedContent == "":
        gc.log.info("failedContent is empty")
    else:
        failedQueries=failedContent.split("\n")
        failedDict={}
        countDict={}
        gc.log.info("httpprefix:%s gc.baselogdir:%s" % (httpprefix,gc.baselogdir))
        for line in failedQueries:
            if (line.strip()!= "" or line != "") and line.find("FAILED:") >= 0:
                key=line.split("FAILED:")[1]
                gc.log.info(key)
                val=httpprefix+"/"+line.split("FAILED:")[0].rstrip(":").split(gc.baselogdir)[1]
                if key in failedDict:
                    countDict[key]=countDict[key]+1
                else:
                    failedDict[key]=val
                    countDict[key]=1
        for key in failedDict:
            gc.log.info("%s,%s" % (key,failedDict[key]))
            message +="%d&nbsp&nbsp&nbsp&nbsp<a href=%s>%s</a><br>\n" % (countDict[key],failedDict[key],key)
    gc.log.info(message)
    open(errorfile,"w").write("<html>"+message+"</html>")
    gc.log.info(httpprefix+"/"+p.split(gc.baselogdir)[1]+"/error.html")

#(1)检测是否有任务运行;
#(2)如果没有任务运行就修改数据库状态为成功；
#(3)下载所有的失败节点并跟基准失败节点对比；
#(4)统计生产当前错误统计
@LogWrapper
def dorefresh(gc):
    while 1==1:
        gc.log.info("waitforStatus() bgeins")
        waitforStatus(gc)
        gc.log.info("waitforStatus() over,refresh() bigens")
        refresh(gc)
        gc.log.info("refresh() over")
        if proceed(gc)==1:
            gc.log.info("proceed exit,return")
            break

if __name__ == '__main__':
    gc=GlobalConf()
    configFile="./config.py"
#读取配置,运行之前必须手工清空pidfile并手工设置timestamp
    if gc.load(configFile,"test.log")!=0:
        gc.log.info("can not load config file:%s" % (configFile))
        sys.exit(1)
    if os.path.exists(gc.pidfile):
        gc.log.fatal("pidfile:%s already exists,the same process of testClient is running,abort." % (gc.pidfile))
        sys.exit(1)
    else:
        open(gc.pidfile,"w").write(str(os.getpid()))
    taskidList=getids(gc).keys()
    runUU(gc,taskidList)
#跑所有的基准成功节点
#如果配置中断开始标记，就继续执行任务，执行之前需要进行状态恢复等操作
    if gc.refresh.strip("\n") == "yes":
        dorefresh(gc)
