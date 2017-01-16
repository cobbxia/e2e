#!coding=utf8
from base64 import encodestring
from hmac import new as hmac_new
import subprocess,os,sys,socket,inspect, unittest, urllib, hashlib, time, random, re, sys
import traceback
from gevent.subprocess import Popen, PIPE
g_debug = False

def getcount(gc,sql):
    cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
    gc.log.info(cmd)
    r=do_cmd(cmd)
    if(r[0]!=0):
        gc.log.fatal("cmd:%s\nstdout:%s\nstderr:%s\n" % (cmd,r[1],r[2]))
        return 1
    cnt=int(r[1].strip()[0])
    if cnt>0:
        gc.log.info("sql:%s has record in database." % (sql))
        return cnt
    return 0

#返回1 是分区表
def isparted(gc,odps,projectname,tablename):
    ret=0
    hql='''desc %s.%s;''' % (projectname,tablename)
    r=do_odps_filecmd(gc,odps,hql)
    if r[0] == 0:
        for line in r[1].split("\n"):
            if line.find("Partition Columns") >= 0:
                ret=1
    return ret

def do_remote_cmd(gc,cmd):
    r=(1,"","")
    try:
        if gc.remote == "yes" and len(gc.executorList) != 0:
            executor=getExecutor(gc.executorList,gc)
            if executor is None or  executor == "":
                gc.log.info("no executor found,remote executed error!")
                return r
        cmd='''ssh %s "%s"'''% (executor,cmd)
        gc.log.info("cmd:%s" % (cmd))
        r=do_cmd(cmd)
        if r[0] == 0 :
            gc.log.debug("success:%s" % (cmd))
    except Exception as e:
        gc.log.fatal("error cmd:%s sql:%s r1:%s r2:%s Exception:%s" % (cmd,sql,r[1],r[2],traceback.format_exc()))
    return r

def LogWrapper(function):
    @functools.wraps(function)
    def wrap_function(*args, **kwargs):
        GlobalConf().log.info("%s() begins" % (function.__name__))
        ret=function(*args, **kwargs)
        GlobalConf().log.info("%s() over" % (function.__name__))
        return ret
    return wrap_function

class Singleton(object):
    def __new__(cls,*args,**kw):
        if not hasattr(cls,'_instance'):
            orig=super(Singleton,cls)
            cls._instance=orig.__new__(cls,*args,**kw)
        return cls._instance

def getNonDict(filterfile):
    if not os.path.exists(filterfile) or filterfile == "" or filterfile is None: return {}
    return  pickle.load(open(filterfile,"rb"))

def genNonDict(infile):
    outdict={}
    outfile=infile.split(".txt")[0]+".pk"
    for line in open(infile,"rb"):
        outdict[line]=""
    pickle.dump(outdict,open(outfile,"wb"))
    return outfile

def addpart(tablename,partname,gc,projectname,odps=""):
    if odps=="": odps=gc.kodps
    addpartSql="use %s;alter table %s add if not exists  partition(%s);" % (projectname,tablename,partname)
    gc.log.debug("addpartSql:%s" % (addpartSql))
    r=do_odps_filecmd(gc,odps,addpartSql)
    
#将不是gc.currentday的分区转换为currentday分区，以此实现分区的转换
# select * from partition where part not like '%ds=%' and part not like '%pt=%' and  part not like '%dt=%' and part !="" and  part is not NULL ;
#分区以ds/dt/pt等开头
#2014-10-01这样的分区数量很少，不超过5个
def partTransfer(gc,oldpart):
    r=getDayPart(gc,oldpart)
    partprefix=oldpart.split('=')[0]
    oldpart=oldpart.split('=')[1]
    gc.log.debug("oldpart:%s " % (oldpart))
    newpart=""
    if r[0] != 0 or  len(oldpart) < len(gc.currentday):
        newpart=oldpart
    elif oldpart.find('-') >=0:
        newpart="%s-%s-%s" % (gc.currentday[0:4],gc.currentday[4:6],gc.currentday[6:8])
    elif oldpart.endswith("'"):
        newpart="'"+gc.currentday+oldpart[9:]
    elif  oldpart.endswith("\""):
        newpart="\""+gc.currentday+oldpart[9:]
    else:
        newpart=gc.currentday+oldpart[8:]
    newpart=partprefix+"="+newpart
    gc.log.debug("newpart:%s " % (newpart))
    return newpart

#比较两个分区的时间，如果小于则返回0
def issmaller(curday,today):
    curday=curday.replace('-','')
    curlen=len(curday)
    tolen=len(today)
    if curlen >= tolen:
        for i in range(tolen):
            if curday[i] != today[i]:
                return 1
    return 0   

#日期是否相同，相同返回0,不同返回1
def equal(curday,today):
    curday=curday.replace('-','')
    curlen=len(curday)
    tolen=len(today)
    ret=0
    if curlen >= tolen:
        for i in range(tolen):
            if curday[i] != today[i]:
                ret=1
    else:
        ret=1
    return ret

#是否是以日期为单位，
#如果是天为单位返回0，
#如果有小时分区返回1
#不是日期格式的其他表返回2，
#周表返回3，
#月表返回4
#只有日表和小时表、其他表才进行处理，但是预处理过程中发现其实并不存在小时表，所以只处理2和0
def getDayPart(gc,mi):
    if mi=="" or mi is None:
        return (3,None,None)
    firstPart=mi.split("/")[0]
    gc.log.debug("firstPart:%s" % (firstPart))
    firstPart=mi.split("=")[1]
    gc.log.debug("firstPart:%s" % (firstPart))
    yList=re.split(r'[0-9]{4}',firstPart.replace("-","").strip())
    if len(yList)==2 and yList[0] == '' and yList[1] == '':
        gc.log.info("part:%s is year part,skip" % (mi))
        return (3,None,None)
    mList=re.split(r'[0-9]{6}',firstPart.replace("-","").strip())
    if len(mList)==2 and mList[0] == '' and mList[1] == '':
        gc.log.info("part:%s is month part,skip" % (mi))
        return (4,None,None)
    la=re.split(r'[0-9]{8}',firstPart.replace("-","").strip())
    gc.log.debug("la:%s" % (la))
    if len(la) <= 1:
        gc.log.info("part:%s never has a day part" % (la))
        return (2,None,None)
    bdate=firstPart[0:8]
    gc.log.debug("bdate:%s" % (bdate))
    val=la[1]
    if re.search(r'[1-9]{1,6}',val):
       gc.log.fatal("has time partition:%s" % (mi))
       return (1,bdate,mi)
    else:
       gc.log.info("day part:%s" % (mi))
       return (0,bdate,mi)

def getSize(gc,tablename,partname):
    r=(1,'','')
    size=0
    i=0
    gc.log.debug("tablename:%s,partname:%s" % (tablename,partname))
    while i < gc.retrytimes:
        i=i+1
        try:
            if os.path.exists(self.gc.killfile) :
                gc.log.info("killfile exists,execution over. filename:%s" % (gc.killfile))
                break
            if partname is not None and partname != "":
                cmd='''%s -z %s -q %s '''  % (gc.ddl,tablename,partname)
            else:
                cmd='''%s -z %s'''  % (gc.ddl,tablename)
            gc.log.debug("cmd:%s" % (cmd))
            r=do_cmd(cmd)
            if r[0] == 0:
                gc.log.debug("success:%s size:%s" % (cmd,r[1]))
                size=int(r[1])
                break
            elif os.path.exists(gc.killfile):
                gc.log.debug("%s exists,exit" %  (gc.killfile))
                size=-3
                break
            else:
                gc.log.error("error retytimes:%d cmd:%s ,r1:%s " % (i,cmd,r[1]))
        except Exception as e:
            gc.log.fatal("error cmd:%s r1:%s  Exception: %s" % (cmd,r[1],traceback.format_exc()))
    return size

def getExecutor(executorList,gc):
    if gc.remote == "no":
        return ""
    if executorList is None or len(executorList)==0:
        return ""
    rndindex=random.randint(0,len(executorList)-1)
    return executorList[rndindex]

def randomStr():
    seed = "1234567890"
    sa = []
    for i in range(8):
        sa.append(random.choice(seed))
    salt = ''.join(sa)
    return salt

def do_odps_cmd(gc,odps,sql,retrytimes=""):
    if retrytimes is None or retrytimes=="":
        retrytimes=gc.retrytimes
    r=(1,'','')
    cmd='''%s -e "%s" ''' % (odps,sql)
    gc.log.debug(cmd)
    i=0
    while i < retrytimes:
        if os.path.exists(gc.killfile):
            gc.log.debug("%s exists,exit" %  (gc.killfile))
            break
        i=i+1
        try:
            r=do_cmd(cmd)
            if r[0] == 0:
                gc.log.debug("success:%s" % (cmd))
                break
            elif i < retrytimes:
                gc.log.error("cmd retrytimes:%d error:%s r1:%s r2:%s" % (i,cmd,r[1],r[2]))
        except Exception as e:
            gc.log.fatal("['error', ('cmd:%s r1:%s r2:%s Exception: %s')]" % (cmd,r[1],r[2],traceback.format_exc()))
    return r

def do_o_filecmd(gc,odps,sql,outfp=subprocess.PIPE,errfp=subprocess.PIPE):
    executor = ""
    if gc.remote == "yes" and len(gc.executorList) != 0:
        executor=getExecutor(gc.executorList,gc)
        if executor is None or  executor == "":
            gc.log.info("no executor found,executed on local matchine")
    r=(1,'','')
    i=0
    gc.log.info("sql:%s,executor:%s" % (sql,executor))
    while i < gc.retrytimes:
        if os.path.exists(gc.killfile):
            gc.log.debug("%s exists,exit" %  (gc.killfile))
            break
        i=i+1
        cmdfilename="/tmp/"+randomStr()
        open(cmdfilename,"w").write(sql)
        cmd='''%s -f %s''' % (odps,cmdfilename)
        if executor != "":
            scpcmd="scp %s %s:%s" % (cmdfilename,executor,cmdfilename)
            gc.log.debug("scpcmd:%s" % (scpcmd))
            do_cmd(scpcmd)
            cmd='''ssh %s "%s"'''% (executor,cmd)
        gc.log.debug("cmd:%s" % (cmd))
        try:
            r=do_cmd(cmd,outfp,errfp)
            if r[0] == 0 :
                gc.log.debug("success:%s" % (cmd))
                break
            elif i<gc.retrytimes:
                gc.log.error("error retytimes:%d cmd:%s ,sql:%s r1:%s r2:%s" % (i,cmd,sql,r[1],r[2]))
        except Exception as e:
            gc.log.fatal("error cmd:%s sql:%s r1:%s r2:%s Exception:%s" % (cmd,sql,r[1],r[2],traceback.format_exc()))
        finally:
            rmcmd="rm -rf %s" % (cmdfilename)
            if executor != "":
                rmcmd='''ssh  %s "%s" ''' % (executor,rmcmd)
            gc.log.debug("rmcmd:%s" % (rmcmd))
            do_cmd(rmcmd)
    return r

def getTableList(gc,sql=""):
    filterdict=getNonDict(gc.ignorefile)
    if gc.tablefile != "" and gc.tablefile is not  None and gc.ismysql == "no":
        gc.log.info("generate table list from tablefile:%s" % (gc.tablefile))
        gc.tableList=[t.replace("\n","") for t in open(gc.tablefile,"r") if t not in filterdict ]
        return gc.tableList
    gc.log.info("generate table list from mysql")
    if sql == "":
        sql="select name from \`table\`;"
    cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
    gc.log.info(cmd)
    r=do_cmd(cmd)
    if(r[0]!=0):
        gc.log.fatal("getTableList error!cmd:%s" % (cmd))
        print("getTableList error!cmd:%s" % (cmd))
        sys.exit(0)
    for line in r[1].split("\n"):
        if line not in filterdict: 
            gc.tableList.append(line)
    gc.log.info("total table count from mysql :%d" % (len(gc.tableList)))

def getResouceList(gc):
    gc.log.info("generate resource list from file")
    for resource in open(gc.resourcefile,"r"):
        resource=resource.strip("\n")
        if resource == "":
            continue
        gc.resourceList.append(resource)
    gc.log.info("resource loaded")

def getExecutorList(hostFilename):
    executorList=[]
    if not os.path.exists(hostFilename):
        return executorList
    fp=open(hostFilename,"r")
    for executor in fp.readlines():
        if executor == "" or executor is None:
            continue
        executorList.append(executor.rstrip("\n"))
    return executorList


def isToday(curday):
    today=gettoday()
    curday=curday.replace('-','')
    curlen=len(curday)
    tolen=len(today)
    if curlen >= tolen:
        for i in range(tolen):
            if curday[i] != today[i]:
                return 1
    return 0   

def gettoday():
    return time.strftime('%Y%m%d',time.localtime(time.time()))

def nowstr():
    return time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))

def printTime():
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print nowTime

def now_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

def do_cmd(cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    p = Popen(cmd, shell=True,close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return p.returncode, out, err 

def runHiveCmd(cmd):
    res=""
    r=do_cmd(cmd)
    if re.search(r'ok|OK',r[2].decode('utf-8')):
        res=[1].decode("utf-8").strip("\n")
    return res

def d(karg):
    for i in karg:
        print(i,karg[i])



def list2Dict(argList):
    retDict={}
    for i in range(len(argList)):
        k=argList[i].split("=")[0]
        v="=".join(argList[i].split("=")[1:])
        retDict[k]=v
    return retDict

def isNum(a):
    if re.match(r"^\-?[\d\.]+$",a):
        return 1
    else:
        return 0              

def getComprKey(fieldCompr):
    tempComprFieldList = fieldCompr.split(':')
    if "key_t2" not in locals():
        key_t2=""
    if "key_t1" not in locals():
        key_t1=""
    for singleFieldCompr in tempComprFieldList:
        tempKeyT1=singleFieldCompr.split(';')[0]
        tempKeyT2=singleFieldCompr.split(';')[1]
        if key_t1 == "":
            key_t1=tempKeyT1
        else:
            key_t1=key_t1+";"+tempKeyT1
        if key_t2 == "":
            key_t2=tempKeyT2
        else:
            key_t2=key_t2+";"+tempKeyT2
    return (key_t1,key_t2)

    
def cur_file_dir():
    path = os.path.realpath(sys.path[0])        # interpreter starter's path
    if os.path.isfile(path):                    # starter is excutable file
        path = os.path.dirname(path)
        return os.path.abspath(path)            # return excutable file's directory
    else:                                       # starter is python script
        caller_file = inspect.stack()[1][1]     # function caller's filename
        return os.path.abspath(os.path.dirname(caller_file))# return function caller's file's directory

def getCurRunPosInfo():
    try:
        raise Exception
    except:
        exc_info = sys.exc_info()
        traceObj = exc_info[2]      
        frameObj = traceObj.tb_frame 
        Upframe = frameObj.f_back                        
        return (Upframe.f_code.co_filename, Upframe.f_code.co_name, Upframe.f_lineno)

def uploadLog(filename,gChecklistURL=""):
    try:
        filename=urllib.request.quote(filename)
        requestUrl='''curl -F "filename=@%s;type=text/plain"  "%s/uploadLog"''' % (filename,gChecklistURL)                        
        r=do_check_cmd(requestUrl)
        return r
    except Exception as e:
        print("uploadLog error:%s" % format(str(e)))

def filterList(fieldList,negFieldList):
    retList=[]
    for item in fieldList:
        if item not in negFieldList:
            retList.append(item)
    return retList

def alarmWrapper():
    ip=do_cmd("hostname -i")[1].decode('utf-8').strip("\n")
    retStr="agent ip:%s" % (ip)
    wangwang("",retStr,subject="\"insert webserver failed!\"")

def do_check_cmd(cmd):
    r=(1,'','')
    try:
        r=do_cmd(cmd)
    except Exception as e:
        print("cmd execute error:%s" % (format(str(e))))
        alarmWrapper()
        return r
    return r


def wangwang(nick,retStr,subject="\"table comparation failed\""):
    context="\""+retStr+"\""
    if nick == "" :
        nick="慕宗"
    baseURL="http://kelude/api/admin/notice/wangwang?auth=155537aa6e5c65a42e89f3a8c10a6892"
    requestURL="curl '%s&%s'" % (baseURL,urllib.parse.urlencode({"nick":nick,"subject":subject,"context":context}))
    do_cmd(requestURL)

if __name__ == '__main__':
    stdout=open("stdout.txt","w")
    stderr=open("stdout.txt","w")
    cmd = ''' ls ./'''
    do_cmd(cmd,stdout=stdout, stderr=stderr)
