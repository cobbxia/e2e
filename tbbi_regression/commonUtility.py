#!coding=utf8
from gevent.subprocess import Popen, PIPE
from base64 import encodestring
from hmac import new as hmac_new
import subprocess,os,sys,socket,inspect, unittest, urllib, hashlib, time, random, re, sys,traceback
g_debug = False

def do_mysql_cmd(gc,sql):
    cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
    gc.log.info(cmd)
    r=do_cmd(cmd)
    if(r[0]!=0):
        gc.log.fatal("cmd:%s\nstdout:%s\nstderr:%s\n" % (cmd,r[1],r[2]))
        return 1
    try:
        cnt=int(r[1].strip()[0])
    except IndexError as e:
        cnt=0
    if cnt>0:
        gc.log.info("sql:%s has record in database." % (sql))
        return cnt
    return 0

#返回1 是分区表
def isparted(gc,odps,projectname,tablename):
    ret=0
    hql='''desc %s.%s;''' % (projectname,tablename)
    r=do_odps_filecmd(gc,odps,hql)
    #print(r[1])
    #print(r[2])
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
        #print(line)
        #lines=line.strip("\n").split()
        #lineLength=len(lines)
        #k=lines[0]
        #v=lines[1]
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
            #executor=getExecutor(gc.executorList)
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

def do_odps_filecmd(gc,odps,sql,outfp=subprocess.PIPE,errfp=subprocess.PIPE):
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
            #do_cmd(cmd)
            r=do_cmd(cmd,outfp,errfp)
            if r[0] == 0 :
                gc.log.debug("success:%s" % (cmd))
                break
            #elif not (r[1].find("There is not row need to copy")>=0 or r[2].find(" There is not row need to copy")>=0):
            #    gc.log.error("sql:%s r1:%s r2:%s" % (sql,r[1],r[2]))
            #    break
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
        #sql="select name from \`table\` t ,partition p where t.id=p.\`table\` and p.flag=100;"
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

def printTime():
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print nowTime

def now_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

def do_cmd(cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    #p = subprocess.Popen(cmd, shell=True,close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = Popen(cmd, shell=True,close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if stdout != subprocess.PIPE:
        print(out)
        stdout.write(out)
        stderr.write(err)
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

def parseResult(column_diff,colorSrc="red",colorDst="green"):
    ret_diff='''<!DOCUMENT><html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>Koala-log</title>
</head>'''
    ret_diff=ret_diff+'<table border="1">'
    lineNum=0
    itemTotal= 0
    for line in column_diff.split("\n"):
        itemNum=0
        itemFlag=0
        if line.strip() == "":
            continue
        if lineNum==0:
            ret_diff=ret_diff+"<tr>"+"".join(['''<th>%s</th>''' % (item) for item in line.split("\t")])+"</tr>"
            lineNum=lineNum+1
            itemTotal=len(line.split("\t"))
            continue 
        if len(line.split("\t")) != itemTotal:
            #ret_diff=ret_diff+"<tr>"+line+"</tr>"
            lineNum=lineNum+1
            continue
        ret_diff=ret_diff+"<tr>"
        for item in line.split("\t"):
            color="black"
            if itemNum == 0:
                itemFlag=int(item+"0",2)
            elif (1<<(int)(itemNum/2))&itemFlag >0   and  itemNum % 2 == 0:
                #print(itemFlag,itemNum,(int)(itemNum/2),1<<(int)(itemNum/2),(1<<(int)(itemNum/2))&itemFlag,item)
                color=colorSrc
            elif (1<<(int)(itemNum/2))&itemFlag >0  and  itemNum % 2 != 0:
                #print(itemFlag,itemNum,(int)(itemNum/2),1<<(int)(itemNum/2),(1<<(int)(itemNum/2))&itemFlag,item)
                color=colorDst
            ret_diff=ret_diff+'''<th><p style="color:%s">%s</p></th>''' % (color,item)
            itemNum=itemNum+1
        ret_diff=ret_diff+"</tr>"
        lineNum=lineNum+1
    ret_diff=ret_diff+"</table></html>"
    return ret_diff


def list2Dict(argList):
    retDict={}
    for i in range(len(argList)):
        k=argList[i].split("=")[0]
        v="=".join(argList[i].split("=")[1:])
        #print("k:%s\tv:%s" % (k,v))
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



#def cur_file_dir():
    #path=sys.path[0]
    #if os.path.isdir(path):
    #    return path
    #elif os.path.isfile(path):
    #    return os.path.dirname(path)
    
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

class REQUEST():
    def __init__(self,accessid,accesskey,host):
        #self.host   = G_ODPS_HOST
        self.host   = host
        self.method = ''
        self.path   = ''
        self.body = ''
        self.accessid = accessid
        self.accesskey= accesskey
        self.headers= {}
        self.memo = ''       #case的描述
    def output(self):
        if g_debug:        
            print ("\n========REQUEST========\nhost:%s\nmethod:%s\npath:%s\nheader:%s\nbody:\n%s"\
        % (self.host,self.method,self.path,self.headers,self.body) )
    def addhead(self,stra):
        key,value = stra.split(':')
        self.headers[key.lower()] = value.lower()
    def genauth(self):
        #import hashlib
        #import cgi
        #import urlparse
        xodps = ""
        xkey_list = []
        xsort_list = []
        for key in self.headers:
            if key.find("x-odps-") >= 0:
                xkey_list.append(key.strip())
        xsort_key = sorted(xkey_list)
        for xkey in xsort_key:
            if self.headers.get(xkey) != None:
                xodps = xodps + xkey+":"+self.headers[xkey] +'\n'
        if self.path.find("?") > 0:
            dict_query = {}
            str_query = self.path[self.path.find("?")+1:]
            list_query = str_query.split("&")
            sort_query = ""
            key_list = []
            for item in list_query:
                key_value = item.split("=")
                key_list.append(key_value[0])
                if len(key_value) == 2:
                    dict_query[key_value[0]] = key_value[1]
                else:
                    dict_query[key_value[0]] = ""
            sort_key = sorted(key_list)
            #print sort_key
            for key in sort_key:
                if dict_query[key] == "":
                    sort_query = sort_query + key +"&"
                else:
                    sort_query = sort_query + key +"=" + dict_query[key] +"&"
            list_path = self.path[0:self.path.find("?")]
            self.path = self.path[0:self.path.find("?")] + "?"+sort_query[:-1] 
        else:
            pass
        if len(self.body.strip()) > 0:
            #content_md5 = hashlib.md5(self.body).hexdigest()
            content_md5 = ""
        else:
            content_md5 = ""
        
        try:
            content_type = self.headers["content-type"].strip()
        except:
            content_type = ''
        #print self.headers

        date = self.headers['Date'].strip()
        #self.headers['x-odps-date']  = date
        #print self.path
        path = self.path[self.path.find("/projects"):]
        #print "\npath:"+path
        string = self.method.strip() + '\n' \
            + content_md5 + '\n'    \
            + content_type + '\n' \
            + date + '\n'           \
            + xodps \
            + path
        #   + "x-odps-date:%s" % date + "\n" \
        #print ("\nstring:\n[",string,"]\n")
        h = hmac_new(
            self.accesskey.encode(),
            string.encode(),
            hashlib.sha1          
        )
        #print string 
        signature = encodestring(h.digest()).strip()
        #print signature
        return signature
class RESPONSE():
    def __init__(self):
        self.headers = {}
        self.status  = 0
        self.reason  = ""
        self.version = ""
        self.body    = ""
    def output(self):
        if g_debug:
            print ("\n========RESPONSE========\nstatus:%s\nheaders:%s\nbody:\n%s"\
        % (self.status,self.headers,self.body))

class util():
    def __init__(self,abc):
        self.td = abc
    def run(self):
        res = RESPONSE()
        res.headers.clear()
        try:
            #print ('host:%s' % (self.td.host))
            conn = httplib.HTTPConnection(self.td.host,timeout=10)
            #conn.set_debuglevel(1)
            #print(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time())))
            #self.td.headers["Date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time()))#time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(time.time()))
            self.td.headers["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(time.time()))
            if self.td.path.find(G_ODPS_EVENT_PATH) < 0:
                signature = self.td.genauth()
                self.td.headers["Authorization"] = "ODPS " + self.td.accessid + ":" + signature.decode()
                #return self.td.headers
            #self.td.output()
            #print ("com:139 %s" % self.td.path)
            conn.request(self.td.method,
                            self.td.path,
                            self.td.body,
                            self.td.headers)
            ret = conn.getresponse()
        except socket.error as v:
            #print v
            return None
        for head in ret.getheaders():
            res.headers[head[0]] = head[1]
        res.reason  = ret.reason
        res.status  = ret.status
        res.version = ret.version
        res.body = ret.read()
        #res.output()
        conn.close()
        return res

class Signer(object):
    def __init__(self,access_id,access_key):
        self.access_id = access_id
        self.access_key= access_key
    def gen(self,host):
        access_id=self.access_id
        access_key=self.access_key

        td = REQUEST(access_id,access_key,host)
        #td.host     = "%s" % G_ODPS_HOST
        td.host     = "%s" % host
        td.method = "GET"
        td.path     = "%s/projects/" % G_ODPS_PREFIX
        td.headers  = {}
        #print(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time())))
        td.headers["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        test1 = util(td)
        ret = test1.run()
        return ret.headers

def uploadLog(filename,gChecklistURL="http://172.24.102.214:8080"):
    try:
        # curl  -F "filename=@cao;type=text/plain"  "http://172.24.102.214:8888/uploadLoga"
        #print(filename)
        filename=urllib.request.quote(filename)
        requestUrl='''curl -F "filename=@%s;type=text/plain"  "%s/uploadLog"''' % (filename,gChecklistURL)                        
        #print(requestUrl)
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
    wangwang("慕宗",retStr,subject="\"insert webserver failed!\"")

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
#curl "http://kelude.taobao.net/api/admin/notice/wangwang?auth=155537aa6e5c65a42e89f3a8c10a6892&nick=慕宗&subject=旺旺通知&context=内容"      
    #log=olog.getLog("check","debug")
    context="\""+retStr+"\""
    #log("nick=%s\tsubject=%s\tcontext=%s" % (nick,subject,context))
    if nick == "" :
        #log("wangwang nick is empty,send it to 慕宗")
        nick="慕宗"
    baseURL="http://kelude.taobao.net/api/admin/notice/wangwang?auth=155537aa6e5c65a42e89f3a8c10a6892"
    requestURL="curl '%s&%s'" % (baseURL,urllib.parse.urlencode({"nick":nick,"subject":subject,"context":context}))
    #log("requestURL:%s" % requestURL)
    do_cmd(requestURL)

def test():
    curday="2014-01-01-11-22-33"
    print(isToday(curday))
    curday="2014-12-22-00-00-00"
    print(isToday(curday))
    curday="20141212"
    print(isToday(curday))
    curday="2014-12-22-01-00-00"
    print(isToday(curday))
    curday="201412120101"
    print(isToday(curday))
    sys.exit(0)

##   access_id="3ARMp0GSruSLnMwI"
##   access_key="bAdvSomgQfxJWcULf7w2AJo2PJ6WRA"
##   sign = Signer(access_id,access_key)
#    #print ('result: %s' % sign.gen("10.206.120.19")["Authorization"])
#    print ('result: %s' % sign.gen("10.206.120.19"))
    #filename="/20130601/132cbac1e22a0db2587ddf11802cb4f2a3f238b185ca54fe0df485da.log"
    logDirName="/home/taobao/httpd/htdocs"
    filename=logDirName+"/20131106/757f8ee6db78008700ebd59bf49e75aebbe4acee4749ba27b025d9c3.log"
    uploadLog(filename)
    
if __name__ == '__main__':
    stdout=open("stdout.txt","w")
    stderr=open("stdout.txt","w")
    cmd = ''' ls ./'''
    do_cmd(cmd,stdout=stdout, stderr=stderr)
