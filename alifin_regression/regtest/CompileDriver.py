#!/home/tops/bin/python2.7
import os
import sys
import re
import subprocess
import shutil
import logging
import string
import glob
import random
import threading,multiprocessing
import Queue
import shlex
import tempfile,time
import cPickle as pickle
from datetime import datetime,tzinfo,timedelta
import common
import commonUtility
import traceback
import socket
import gevent
from gevent.queue import Queue
from gevent import Greenlet
from gevent import monkey; monkey.patch_all()


ST_FINISHED = 0
ST_NOTRUN = 1
ST_FAILED = 2

FUXI_EMPTY = 0
FUXI_USED = 1
FUXI_FULL = 2

G_ROUNDNUM=1
G_RETRY_TIMES=3
G_THREADS=[]
G_MUTEX = threading.Lock()
G_PINDEX=-1
G_FUXI_QUOTA=FUXI_EMPTY
G_VCMDS={}
G_JSDICT={}
G_VSDICT={}
global_conf = None
G_QUEUE = None 


commonUtility.do_cmd("mkdir -p ./log")
QUERY_SUFFIX = '.hql'
fmtstr = '%(levelname)-12s %(asctime)s %(name)-8s [%(thread)d][%(threadName)s][%(module)s:%(funcName)s:%(lineno)d] %(message)s'
numeric_level = getattr(logging,"DEBUG",None)
logging.basicConfig(level=numeric_level,filename="./log/alifin_"+commonUtility.nowstr()+".log",format=fmtstr)
LOG = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter(fmtstr)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class ConfigError(Exception):
    pass


class GMT8(tzinfo):
    delta=timedelta(hours=8)
    def utcoffset(self,dt):
        return self.delta
    def tzname(self,dt):
        return "GMT+8"
    def dst(self,dt):
        return self.delta

class GMT(tzinfo):
    delta=timedelta(0)
    def utcoffset(self,dt):
        return self.delta
    def tzname(self,dt):
        return "GMT+0"
    def dst(self,dt):
        return self.delta

def BeijingTime():
    from_tzinfo=GMT()
    local_tzinfo=GMT8()
    return datetime.utcnow().replace(tzinfo=from_tzinfo).astimezone(local_tzinfo)

#min 500,max 1000 threads,determined by fuxi quota
def worker_thread(queue):
    while True:
        cmd = queue.get()
        LOG.info('In threading: ' + cmd)
        #os.system(cmd)
        queue.task_done()


def run_parallel(items, para):
    queue = Queue.Queue()
    for i in items:
        queue.put(i)
    for i in range(para):
        t = threading.Thread(target=worker_thread, args=(queue,))
        t.daemon = True
        t.setDaemon(True)
        t.start()
    queue.join()


def count_equal(base, run):
    if base is None:
        return False
    try:
        base = int(base)
        return base == run
    except ValueError:
        _range = eval(base)
        return run in _range


class Command(object):
    
    def __init__(self, key):
        self._key = key
        self._pobj = None
        self.status = ST_NOTRUN
        self.retry_cnt = 0
        self.start_time = 0
        self.end_time = 0
        self.priority = 0

    def compose(self):
        return self._key
    
    def pid(self):
        if self._pobj is None:
            return None
        return self._pobj.pid

    def key(self):
        return self._key

    def status(self):
        return self._status

    def __repr__(self):
        return 'Command("%s")' % self._key

    def is_success(self):
        if self._pobj.returncode == 0:
            return True
        return False

    def log_rotate(self, file_name, retry_cnt):
        for i in range(retry_cnt)[::-1]:
            src = file_name if i == 0 else file_name + '.%s' % i
            dst = file_name + '.%s' % (i+1)
            shutil.move(src, dst)


def log_rotate(file_name, retry_cnt):
    for i in range(retry_cnt)[::-1]:
        src = file_name if i == 0 else file_name + '.%s' % i
        dst = file_name + '.%s' % (i+1)
        shutil.move(src, dst)

def is_fuxi_quota_full(quotaname,computeag):
    is_quota_full=0
    try:
        cmd = ""
        r=commonUtility.do_cmd(cmd)
        if r[0] == 0:
            for line in r[1].split("\n"):
                if line.find("is_quota_full") >= 0:
                    is_quota_full=int(line.split(":")[1].strip())
        LOG.info("stdout:%s is_quota_full:%d" % (r[1],is_quota_full))
    except Exception as e:
        LOG.warn(traceback.format_exc())
    return is_quota_full

def MultiContainerRunner(opList,executor_hosts,conf):
    if len(opList) == 0 :return
    global G_THREADS
    LOG.info("opList length:%d start MultiRunner" % (len(opList)))
    threadnum=conf.max_jobs
    if threadnum >  len(opList): 
        threadnum=len(opList)
    LOG.info("MultiContainer threads inited")
    for index in range(threadnum):
        ins=TestRunner(opList,executor_hosts,conf,index,threadnum)
        G_THREADS.append(ins)
    LOG.info("MultiContainer threads begin starting.")
    for index in range(threadnum):
        LOG.debug("create thread id:%d threadnum:%d" % (index,threadnum))
        G_THREADS[index].start()
    LOG.info("MultiContainer started.")
    LOG.info("Monitor thread inited.")
    LOG.info("Monitor thread started. ")
    monitor()
    LOG.info("waiting for monitor thread destroyed and exit." )
    LOG.info("monitor thread finished")
    LOG.info("start destroying  all running threads.")
    for index in range(threadnum): 
        G_THREADS[index].join()
        LOG.debug("destroy thread id:%d" % (index))
    LOG.info("all running threads destroyed.")

def worker(threadIndex):
    try:
        while True:
            task = G_QUEUE.get(timeout=1) # decrements queue size by 1
            gevent.sleep(0)
            
    except Empty:
        LOG.info('thread:%d destroyed!' % (threadIndex))

def boss(conf,opList):
    if len(opList) == 0 :return
    roundnum=initRound(conf)
    roundIndex =0 
    G_QUEUE=Queue(len(opList))
    while roundIndex < roundnum:
        for task in opList:
            G_QUEUE.put_nowait(task)

#coroutine mode
def CoRunner(opList,conf):
    bosstask=genvet.spawn(boss,conf,opList)
    gevent.sleep(0)

def executeCmd(cmd,executor_hosts,executor_index):
    global G_FUXI_QUOTA,G_VCMDS,global_conf,G_RETRY_TIMES
    if G_FUXI_QUOTA == FUXI_FULL :
        LOG.debug("fuxi full:%d,sleep 1 seconds" % (FUXI_FULL))
        gevent.sleep(1)
        #time.sleep(1)
    retry_cnt=0
    while retry_cnt < G_RETRY_TIMES:
        executor_index=executor_index + 1
        executor_host=executor_hosts[executor_index % len(executor_hosts)]
        cmd.copy(executor_host)
        outfp = open(cmd.log_file, 'a+')
        errfp = open(cmd.err, 'a+')
        try:
            sshcmd="ssh %s '%s'" % (executor_host, cmd.compose())
            LOG.info("%s" % (sshcmd))
            stime=time.time()
            r=(1,"","")
            r=commonUtility.do_cmd(sshcmd)
            etime=time.time()
            durtime=etime-stime
            outfp.write("starttime:%s endtime:%s durtime:%s" % (str(stime),str(etime),str(durtime)))
            outfp.write(r[1])
            errfp.write(r[2])
        finally:
            outfp.close()
            errfp.close()
        if r[0] == 0 : 
            break
        else:
            retry_cnt = retry_cnt + 1
            log_rotate(cmd.log_file, retry_cnt)
            log_rotate(cmd.err, retry_cnt)
    if retry_cnt == G_RETRY_TIMES:
         cmd.status = ST_FAILED 
    else:
         cmd.status = ST_FINISHED
    return retry_cnt

def monitor():
    global G_THREADS,G_FUXI_QUOTA
    jsdict={}
    vsdict={}
    threadnum = len(G_THREADS)
    alive_thread_num = threadnum
    while alive_thread_num > 0 :
        alive_thread_num = 0
        processedNum = 0
        for index in range(threadnum):
            processedNum = processedNum + G_THREADS[index].processedNum
            jsdict = dict(jsdict.items()+G_THREADS[index].jsdict.items())
            vsdict = dict(vsdict.items()+G_THREADS[index].vsdict.items())
            if G_THREADS[index].isAlive != False :
                alive_thread_num = alive_thread_num + 1
        G_FUXI_QUOTA=is_fuxi_quota_full(global_conf.quotaname,global_conf.computeag)
        LOG.info('alive_thread_num:%d fuxi_quota:%d processedNum:%d' % (alive_thread_num,G_FUXI_QUOTA,processedNum))
        dump_status(global_conf.work_dir,"odps",  jsdict)
        dump_status(global_conf.work_dir,"verify",vsdict)
        gevent.sleep(5)
        #time.sleep(60)
       
def dump_status(work_dir,subdir,statusDict):
    succnum = 0
    failnum = 0
    succfp = open(os.path.join(work_dir,subdir, 'finished.log'),"w")
    failfp = open(os.path.join(work_dir,subdir, 'failed.log'),"w")
    for qfile in statusDict:
        if statusDict[qfile] == ST_FINISHED:
            succfp.write("%s\n" % (qfile))
            succnum = succnum + 1
        elif statusDict[qfile] == ST_FAILED:
            failfp.write("%s\n" % (qfile))
            failnum = failnum + 1
    succfp.close()
    failfp.close()
    LOG.info("%s succ job:%d fail job:%d" % (subdir,succnum,failnum))

def initRound(conf):
    G_ROUNDNUM = conf.roundnum
    if G_ROUNDNUM == "" :
        G_ROUNDNUM = 1
    else:
        G_ROUNDNUM = int(G_ROUNDNUM)
    return G_ROUNDNUM

#class TestRunner(threading.Thread):
class TestRunner(Greenlet):
    def __init__(self,cmds,executor_hosts,conf,threadid,threadnum):
        #threading.Thread.__init__(self)
        Greenlet.__init__(self)
        self.processedNum=0
        self.opList=cmds
        self.executor_hosts=executor_hosts
        self.conf=conf
        self.threadid=threadid
        self.threadnum=threadnum
        self.executor_index = threadid
        self.isAlive = True
        self.jsdict = {}
        self.vsdict = {}
        initRound(conf)
    def render(self,cmd):
        self.executor_index += 1
        executeCmd(cmd,self.executor_hosts,self.executor_index)
        self.jsdict[cmd._key] = cmd.status
        if cmd._key in G_VCMDS:
            if global_conf.is_verify != None and global_conf.is_verify == "no":
                return 1
            verify_cmds=G_VCMDS[cmd._key]
            for verify_cmd in verify_cmds:
                executeCmd(verify_cmd,self.executor_hosts,self.executor_index)
                self.vsdict[verify_cmd._key] = verify_cmd.status
    def run(self):
        localindex=self.threadid
        totalnum=len(self.opList)
        self.executor_index += 1
        roundIndex = 0
        while roundIndex < G_ROUNDNUM:
            while  localindex < totalnum :
                try:
                    if localindex == totalnum or localindex > totalnum: 
                        LOG.info("localindex:%d exceeds totalnum:%s in threadid:%d" % (localindex,totalnum,self.threadid))
                        break
                    r=self.render(self.opList[localindex])
                except Exception as e:
                    LOG.fatal("localindex:%d of totalnum:%s in threadid:%d Exception: %s" % (localindex,totalnum,self.threadid,traceback.format_exc()))
                self.processedNum = self.processedNum + 1 
                localindex=localindex + self.threadnum
            roundIndex = roundIndex + 1
            localindex=self.threadid
            LOG.info("roundIndex:%s G_ROUNDNUM:%d" % (roundIndex,G_ROUNDNUM))
        self.isAlive = False
        LOG.info("localindex:%d finished thread-alive:%s,exceeds totalnum:%s in threadid:%d" % (localindex,self.isAlive,totalnum,self.threadid))

class DE(Command):
    #i query file name, log_dir, declient, conf.query_dir
    def __init__(self, key, log_dir, declient, query_dir):
        super(DE, self).__init__(key)
        self.log_dir = log_dir
        self.declient = declient
        self.query_dir = query_dir
        self.log_file = os.path.join(log_dir, '%s.log' % self._key)
        self.err = os.path.join(log_dir, '%s.err' % self._key)
        p_log_dir = os.path.dirname(self.log_dir)
        self.finished_log = os.path.join(p_log_dir, 'finished.log')
        self.failed_log = os.path.join(p_log_dir, 'failed.log')
        self.notrun_log = os.path.join(p_log_dir, 'notrun.log')

    def update_retry_log(self):
        self.log_rotate(self.log_file, self.retry_cnt)
        self.log_rotate(self.err, self.retry_cnt)
    def copy(self,executor):
        qcontent=""
        sessionflag = global_conf.sessionflag 
        qfp = open(os.path.join(self.query_dir,self.key()),"r")
        for line in qfp:
            if line.startswith("SET") and sessionflag != None and sessionflag != "":
                line = line + sessionflag
                sessionflag=""
            qcontent = qcontent + line
        qfp.close()
        open(os.path.join(self.query_dir,self.key()),"w").write(qcontent)
        self.executor=executor
        qfile = os.path.join(self.query_dir,self.key())
        scpcmd="scp %s %s:%s" % (qfile,executor,qfile)
        r=commonUtility.do_cmd(scpcmd)
        if r[0] != 0:
            LOG.info("scpcmd:%s error!stdout:%s stderr:%s" % (scpcmd,r[1],r[2]))
    
    def compose(self):
        return self.declient + ' --project ' + global_conf.result_project + ' -f ' + os.path.join(self.query_dir, self.key())

    def __repr__(self):
        return 'DE(%s, %s)' % (self.declient, self.key())


class VERIFY(Command):

    def __init__(self, key, log_dir, verify_cmd, ignore_cols=[]):
        super(VERIFY, self).__init__(key)
        self.__cmd = key
        self.log_file_name = key.replace(string.punctuation, '_')
        self.log_dir = log_dir
        self.log_file = os.path.join(log_dir, self.log_file_name + '.log')
        self.err = os.path.join(log_dir, self.log_file_name + '.txt')
        self.verify_cmd = verify_cmd
        self.ignore_cols = ignore_cols
        p_log_dir = os.path.dirname(self.log_dir)
        self.finished_log = os.path.join(p_log_dir, 'finished.log')
        self.failed_log = os.path.join(p_log_dir, 'failed.log')
        self.notrun_log = os.path.join(p_log_dir, 'notrun.log')

    def update_retry_log(self):
        self.log_rotate(self.log_file, self.retry_cnt)
        self.log_rotate(self.err, self.retry_cnt)

    def compose(self):
        #sessionflag = global_conf.sessionflag 
        cmd = self.verify_cmd % (self.__cmd,global_conf.result_project + '.' + self.__cmd)
        return cmd

    def copy(self,executor):
        pass


def find_with_prefix(d, key, default=None):
    for k in d.keys():
        if key.startswith(k):
            return d[k]
    return default

def parse_verify_log_error(entry, error):
    entry['ay42_rowcount'] = 'n/a'
    entry['ay39_rowcount'] = 'n/a'
    entry['unmatched'] = 'n/a'
    entry['result'] = 'FAIL'
    entry['comment'] = None
    return entry

class Suit(object):
    ST_FILE = 'run_status.pickle'

    def __init__(self, suit_dir, work_dir):
        self.suit_dir = os.path.abspath(suit_dir)
        self.work_dir = os.path.abspath(work_dir)
        #self.stamp = datetime.now().strftime('%Y_%m_%d_%H_%M')
        self.stamp = BeijingTime().strftime("%Y_%m_%d_%H_%M")
        if not os.path.lexists(self.work_dir):
            os.mkdir(self.work_dir)
        self.filterList=[]
        self.cmds = {}
        self.vcmds= {}
        self.key_to_cmds = {}
        self.prunner = None
        self._random_set = []
        self.executor_hosts = None
        self.load_config()
        initRound(self.conf)
        global global_conf
        global_conf = self.conf

    def load_filter_query(self):
        self.longquery=[line.strip("\n").split(":")[0] for line in open(os.path.sep.join([self.suit_dir,"longquery.txt"])).readlines()]
        self.filterList.extend(self.longquery)
        LOG.info("add long query:%d to filter query list:%d" % (len(self.longquery),len(self.filterList)))
        if self.conf.is_middle == "no": 
             self.middlequery=[line.strip("\n").split(":")[0] for line in open(os.path.sep.join([self.suit_dir,"middlequery.txt"])).readlines()]
             self.filterList.extend([q for q in self.middlequery if q not in self.filterList])
             LOG.info("add middle query:%d to filter query list:%d"  % (len(self.middlequery),len(self.filterList)))
        if self.conf.is_knownissue == "no":
             self.known_issue_query=["q"+line.strip("\n").split(":")[0].strip().split("_q")[1]+".sql" for line in open(os.path.sep.join([self.suit_dir,"known_issue"])).readlines()]
             self.filterList.extend([q for q in self.known_issue_query if q not in self.filterList])
             LOG.info("add known issue query:%d to filter query list:%d"  % (len(self.known_issue_query),len(self.filterList)))
        self.row_number_query=[line.strip("\n") for line in open(os.path.sep.join([self.suit_dir,"row_number.txt"])).readlines()]
        self.filterList.extend([q for q in self.row_number_query if q not in self.filterList])
        LOG.info("add row number query:%d to filter query list:%d"  % (len(self.row_number_query),len(self.filterList)))

    def load_config(self):
        #config file in work_dir
        config_file = os.path.join(self.suit_dir, 'config.py')
        LOG.info("config_file:%s" % (config_file))
        self.conf = common.import_from_path(config_file)
        conf = self.conf
        self.conf.declient=self.conf.declient % (self.work_dir)
        conf.query_dir = "%s/%s" % (os.path.dirname(os.path.dirname(conf.CLT)), conf.query_dir)
        conf.query_list = self._get_workpath(conf.query_list)
        conf.work_dir = self.work_dir
        self.st_file_path = os.path.join(conf.work_dir, self.ST_FILE)
        conf.verify_work = os.path.join(conf.work_dir, 'verify')
        log_level = getattr(conf, 'log_level', 'INFO')
        conf.log_level = log_level
        LOG.info("executor_hosts file:%s" % self._get_workpath(conf.executor_hosts))
        self.executor_hosts = filter(len, [line.strip() for line in open(self._get_workpath(conf.executor_hosts)).readlines()])
        LOG.info("executor_hosts:%d" % (len(self.executor_hosts)))
        conf.verify_list = self._get_workpath(conf.verify_list)
        priority_list = os.path.join(self.suit_dir, 'priority.txt')
        self.priority_table = {}
        for line in common.lineiter(priority_list):
            k, v = line.split()
            self.priority_table[k] = int(v)
        self.load_filter_query()
        self.load_verify_tables(os.path.join(self.suit_dir, 'verify_tables'))
        self.conf.getfield = self.conf.getfield % (self.work_dir,conf.declient)
        self.left_odps = common.ODPSConsole(conf.declient)
        self.right_odps = common.ODPSConsole(conf.odps_declient)
        conf.meta_items = list(common.lineiter(os.path.join(self.suit_dir, 'meta.sql')))
        self.setup_loggin()
        #store cmd status in ST_FILE
        if os.path.exists(self.st_file_path):
            self.prunner = ParallelRunner.load_status(self.st_file_path)
        
    def bublesort(self):
        self.newcmds=[]
        cmdlen=len(self.cmds)
        for i in range(len(cmdlen)-1,0,-1):
            for j in reange(i):
                if self.cmds[j] > self.cmds[j+1] :
                    temp = self.cmds[i]
                    self.cmds[i] = self.cmds[i+1]
                    self.cmds[i+1] = temp
                
    
    def setup_loggin(self):
        log_level = getattr(logging, self.conf.log_level)
        if self.conf.verbose:
            logging.basicConfig(format='[%(levelname)s][%(asctime)s][%(module)s:%(lineno)s]: %(message)s',
                            level=log_level)

        else:
            logging.basicConfig(format='[%(levelname)s][%(asctime)s][%(module)s:%(lineno)s]: %(message)s',
                            level=log_level,
                            filename=os.path.join(self.work_dir, 'driver.log'))

    def load_line_list(self, conf):
        return [i for i in common.lineiter(conf) if not i.startswith('#')]

    def _get_workpath(self, path):
        if not path:
            return path
        if not path.startswith(os.path.sep):
            # relative path
            path = os.path.join(self.suit_dir, path)
            path = os.path.abspath(path)
        return path

    def _rotate_log(self, log):
        if os.path.lexists(log):
            new_log = log + '_' + self.stamp
            os.rename(log, new_log)
            log = new_log
        return log

    def _mkdirs(self, work_dir):
        # create dirs
        log_dir = 'logs'
        self.log_dir = os.path.join(work_dir, log_dir)
        try:
            os.mkdir(work_dir)
        except (IOError, OSError):
            pass
        try:
            os.mkdir(self.log_dir)
        except:
            pass
    
    #set queries to be run and load expected results
    def load_modeling(self, which):
        if self.prunner:
            return
        work_dir = os.path.join(self.work_dir, which)
        self._mkdirs(work_dir)
        log_dir = os.path.join(work_dir, 'logs')
        declient = self.conf.declient
        if which == 'odps':
            declient = self.conf.odps_declient
        conf = self.conf
        if conf.query_list:
            total_queries = set(self.load_line_list(conf.query_list))
        else:
            queries = glob.glob(os.path.join(conf.query_dir, '*.sql'))
            #get all query file
            LOG.info("len of queries:%d in conf.query_dir:%s" % (len(queries),conf.query_dir))
            total_queries = []
            LOG.info("filterquery::%s"  %  self.filterList)
            for q in queries:
                q = os.path.basename(q)
                if q in self.filterList:
                    LOG.debug("qfile:%s in filterList,filter."  % q)
                else:
                    total_queries.append(q)
            total_queries = set(total_queries)
            LOG.info("len of total_queries:%d,len of queries:%d,len of filterList:%d" % (len(total_queries),len(queries),len(self.filterList)))
            if conf.random > 0:
                if self._random_set:
                    total_queries = self._random_set
                else:
                    total_queries = set(random.sample(total_queries, conf.random))
                    self._random_set = total_queries
                LOG.info('Selected %s random queries: %s' % \
                             (conf.random, total_queries))
        to_run_queries = total_queries
        self.total_queries = total_queries
        #get priority for each query,key_to_cmds ==>{queryfilename,queryobjaect}
        LOG.info("declient:%s" % (declient))
        for i in to_run_queries:
            c = DE(i, log_dir, declient, conf.query_dir)
            c.priority = self.priority_table.get(i, 0)
            #key DE object ,value is set
            self.cmds[c] = set()
            self.key_to_cmds.setdefault(i, set()).add(c)

    def attach_verify_cmd(self, qfile_keys, alreadyrun, log_dir, need_parent=False):
        allocated = set()
        LOG.info("len of self.qfile_to_tables:%d self.qfile_keys:%d" % (len(self.qfile_to_tables),len(qfile_keys)) )
        for key in qfile_keys:
            #find destination tbale from key(sql filename)
            if key in self.filterList:
                LOG.debug("key:%s in filterList" % (key))
                continue
            if key not in self.qfile_to_tables:
                continue
            tabs = self.qfile_to_tables[key]
            for target in tabs:
                if target not in alreadyrun \
                        and target.split(';')[0] not in allocated:
                    # No need to verify a partition if a full table
                    # has been verified.
                    # ignore_cols = self.ingore_conf.get(target)
                    verify_cmd = VERIFY(target, log_dir, 
                                        self.conf.verify_cmd)
                    global G_VCMDS
                    G_VCMDS.setdefault(key,set()).add(verify_cmd)
                    # ignore_cols)
                    if need_parent:
                        model_cmds = self.key_to_cmds[key]
                        for model_cmd in model_cmds:
                            self.vcmds.setdefault(verify_cmd, set()).add(model_cmd)
                    else:
                        self.vcmds.setdefault(verify_cmd, set())
                    if target not in allocated:
                        allocated.add(target)
                    s = set()
                    s.add(verify_cmd)
                    #qiflename --> verify_cmd
                    self.key_to_cmds[target] = s
        return allocated

    def load_verify(self):
        if self.prunner:
            print(self.prunner)
            return
        conf = self.conf
        work_dir = os.path.join(self.work_dir, 'verify')
        self._mkdirs(work_dir)
        log_dir = os.path.join(work_dir, 'logs')
        allocated = set()
        verify_plan = open(os.path.join(work_dir, 'verify_plan'), 'w')
        if global_conf.is_verify != None and global_conf.is_verify == "no":
            return 1
        
        if self.conf.verify_list:
            LOG.info("self.conf.verify_list:%s" % (self.conf.verify_list))
            total_verifys = set(self.load_line_list(conf.verify_list))
            total_verifys -= alreadyrun
            for v in total_verifys:
                verify_cmd = VERIFY(v, log_dir, self.conf.verify_cmd)
                self.vcmds[verify_cmd] = set()
                global G_VCMDS
                G_VCMDS.setdefault(v,set()).add(verify_cmd)
            allocated.update(total_verifys)
        else:
            #keys actually get fienames
            keys = [k._key for k in self.cmds.keys()]
            allocated = self.attach_verify_cmd(keys, set(), log_dir, need_parent=True)
        LOG.info("len of allocated:%s len of G_VCMDS:%s" % (len(allocated),len(G_VCMDS)))
        self.allocated = allocated
        cmds = []
        for entry in allocated:
            qfiles = self.table_to_qfiles[entry]
            verify_plan.write('%s\t%s\n' % (entry, ','.join(qfiles)))
        verify_plan.close()

    def load_verify_tables(self, verify_tables_file):
        self.qfile_to_tables = {}
        self.table_to_qfiles = {}
        self.verify_meta = {}

        # load verify table mappings
        vcnt=0
        for line in common.lineiter(verify_tables_file):
            if line.startswith('#'):
                continue
            toks = line.split('\t')
            assert len(toks) >= 2
            qfile = toks[0]
            if qfile in self.filterList:
                LOG.debug("verify filter %s" %  (qfile))
                continue
            tabs = toks[1:]
            self.qfile_to_tables[qfile] = tabs
            for t in tabs:
                self.table_to_qfiles.setdefault(t, set()).add(qfile)
            vcnt = vcnt +1
        LOG.info("cnt of verify_table:%d,len of self.qfile_to_tables:%d" % (vcnt,len(self.qfile_to_tables)))
        # load ignore configuration
        self.ignore_conf = {}
        ignore_conf_file = os.path.join(self.suit_dir, 'verify_meta')
        for line in common.lineiter(ignore_conf_file):
            k, v = line.split('\t')
            self.ignore_conf[k.strip()] = v.split(',')

        # load known issues
        self.known_issues = {}
        known_issue_file = os.path.join(self.suit_dir, 'known_issue')
        for line in common.lineiter(known_issue_file):
            k, v = line.rsplit(':', 1)
            self.known_issues[k.strip()] = v.strip()
        LOG.info("len of self.qfile_to_tables:%d" % (len(self.qfile_to_tables)))

    def start(self):
        opList=[]
        i=0
        for k in self.cmds:
            opList.append(k)
        LOG.info("cmd len:%d executor_hosts len:%d" % (len(opList),len(self.executor_hosts))) 
        MultiContainerRunner(opList,self.executor_hosts,self.conf) 

    def generate_rerun_list(self):
        verify_work = os.path.join(self.work_dir, 'verify')        
        verify_faileds = glob.glob(os.path.join(verify_work, 'fail.list'))
        verify_failed_items = common.linefiles(verify_faileds)
        rerun = set()
        for line in verify_failed_items:
            for i in self.table_to_qfiles[line]:
                rerun.add(i)
        rerun_f = open(os.path.join(self.work_dir, 'rerun.list'), 'w')
        for i in rerun:
            rerun_f.write(i+'\n')
        rerun_f.close()

    def _drop_tables(self, odps, pattern=''):
        tables = [ t.split(':')[1] for t in odps.show_tables(pattern) if 'ip_region_dict' not in t]
        LOG.debug('Dropping tables: ' + str(tables))
        group_num = 200
        groups = [[]] * group_num
        i = -1
        while tables:
            i = (i+1) % group_num
            tab = tables.pop()
            groups[i].append(tab)
        statements = []
        for tabs in groups:
            lines = []
            for i in tabs:
                s = 'drop table if exists %s;\n' % i
                lines.append(s)
            statements.append(''.join(lines))
        temp_files = []
        for i in statements:
            tmpf = tempfile.NamedTemporaryFile(delete=False)
            temp_files.append(tmpf.name)
            tmpf.write(i)
            tmpf.close()
        cmds = ['%s -f %s' % (odps.cmd, t) for t in temp_files]
        run_parallel(cmds, 5)

    def _create_tables(self, odps, items):
        items = ['%s -e "%s"' % (odps.cmd, i) for i in items]
        run_parallel(items, 50)
        

    def drop_dirty(self):
        # Verify runs at production service, only in which dirty tables
        # exist.
        self._drop_tables(self.left_odps, "'dirty_*'")


    def report(self):
        # Step 1: Generate report data
        from refine_verify_log import log_summary2
        try:
            import json
        except ImportError:
            import simplejson as json
        conf = self.conf

        # Get known fail count
        known_rowcount_file = os.path.join(self.suit_dir, 'known_rowcount')
        known_rowcounts = {}
        for i in common.lineiter(known_rowcount_file):
            k, v = i.split()
            known_rowcounts[k] = v
        verify_work = os.path.join(self.work_dir, 'verify')
        pre_work = os.path.join(self.work_dir, 'odps')
        verify_plan = set(common.lineiter(os.path.join(verify_work, 'verify_plan')))
        verify_plan = list(verify_plan)
        summary = {}

        pre_succ = common.lineiter(os.path.join(pre_work, 'finished.log'))
        pre_fail = common.lineiter(os.path.join(pre_work, 'failed.log'))
        pre_mail = os.path.join(pre_work, 'mail.html')
        pre_total_num = len(pre_succ) + len(pre_fail)
        print("mail_html:%s pre_work:%s pre_mail:%s" % (pre_work,os.path.join(pre_work, 'mail.html'),pre_mail))

        pre = {}
        jobs = {'pre': pre}
        report = {}
        report['title'] = ''
        env = {}
        env['Client Machine'] = os.getenv("USER") +"@"+ socket.gethostname() 
        env['Local Log Directory'] = conf.work_dir
        env['Result Project'] = global_conf.result_project
        report['info'] = env

        report['jobs'] = jobs

        pre['Total Queries'] = pre_total_num
        pre['Success'] = len(pre_succ)
        pre['Fail'] = len(pre_fail)

        report['title'] = ''
        report['date'] = conf.biz_date
        report['summary'] = summary
        report['description'] = ''
        summary['Table Count'] = len(verify_plan)
        table_list = []
        report['table_list'] = table_list
        fail_list = open(os.path.join(verify_work, 'fail.list'), 'w')
        succ = 0
        fail = 0
        not_run = 0
        not_run_fp = open(os.path.join(pre_work, 'not_run.log'),"w")
        for vtask in verify_plan:
            tablename  = vtask.split('\t')[0]
            qfile = vtask.split('\t')[1]
            entry = {}
            log_file = os.path.join(verify_work, 'logs', tablename+'.txt')
            fulltablename = tablename+".txt"
            if not os.path.lexists(log_file):
                fulltablename = tablename + ".txt.3"
            log_file = os.path.join(verify_work, 'logs', fulltablename)
            page = os.path.join('view/ODPS-5K/job/ODPS-DailyTest-5Kprd-FinanceCorrectnessValidation-AT5K/ws/',
                                os.path.basename(conf.work_dir),
                                'verify',
                                'logs',
                                tablename+'.txt')
            entry['table'] = tablename
            if not os.path.lexists(log_file):
                not_run_fp.write("%s\n" % (tablename))
                not_run += 1
                if self.conf.is_verify != None and self.conf.is_verify == "no":
                    continue
                LOG.info("qfile:%s Not Run." % (qfile))
                parse_verify_log_error(entry, 'Not Run')
            else:
                try:
                    log = log_summary2(log_file)
                    entry['ay42_rowcount'] = log['left_count']
                    entry['ay39_rowcount'] = log['right_count']
                    unmatched = log['unmatched_count']
                    jenkins_job_url = os.environ.get('JOB_URL')
                    if jenkins_job_url:
                        url=os.path.sep.join([jenkins_job_url,'ws',os.path.basename(conf.work_dir),'verify','logs',fulltablename,"/*view*/"])
                        unmatched = '<a href="%s"> %s </a>' % (url, unmatched)
                        caseurl=os.path.sep.join([jenkins_job_url,'ws','alifin_regression',self.conf.regression_data,os.path.basename(self.conf.query_dir),qfile,'*view*'])  
                        entry['table']='<a href="%s"> %s </a>' % (caseurl,tablename)
                        entry['caseurl'] = caseurl
                    else:
                        unmatched = '<a href="http://%s/%s"> %s </a>' % (conf.host, page, unmatched)
                    entry['unmatched'] = unmatched
                    entry['result'] = log['status']
                    comment = find_with_prefix(self.known_issues, tablename , 'None')
                    entry['comment'] = comment
                    if log['status'] == 'PASS':
                        succ += 1
                    else:
                        pre_count = known_rowcounts.get(tablename)
                        if count_equal(pre_count, log['right_count']):
                            entry['result'] = 'PASS'
                            succ += 1
                        else:
                            LOG.info("%s not equal pre_count:%s log['right_count']:%s" % (tablename,pre_count,log['right_count']))
                            fail += 1
                            table_list.append(entry)
                            print >>fail_list, tablename
                except:
                    print ''.join(traceback.format_exception(*sys.exc_info()))
                    fail += 1
                    parse_verify_log_error(entry, 'Parse Error')
                    table_list.insert(0, entry)
                    print >>fail_list, tablename
        not_run_fp.close()
        summary['Verify Passed'] = succ
        summary['Verify Failed'] = fail
        summary['Verify Not Run'] = not_run

        if fail != 0 or not_run != 0:
            open(os.path.join(self.work_dir, 'status.json'),"w").write("fail")
        json_path = os.path.join(self.work_dir, 'report.json')
        json_file = open(json_path, 'w')
        json.dump(report, json_file)
        json_file.close()

        # Step2: send mail
        from report import mail
        format_tool = os.path.join(os.path.dirname(__file__), 'report', 
                                   'format_report.py')
        
        content = os.popen('python %s < %s' % (format_tool, json_path)).read()
        title = ''
        mailfile=open(pre_mail,"w")
        mailfile.write("%s\n" % (title))
        mailfile.write("%s\n" % (content))

def getPartList(projectname,tablename):
    partList=[]
    try:
        hqlcmd='''%s -e 'show partitions %s.%s' ''' % (global_conf.declient,projectname,tablename)
        r=commonUtility.do_cmd(hqlcmd)
    except :
        return partList
    if r[0] == 0:
        partList=[i.strip("\n") for i in r[1].split("\n") if i != "" ]
    return partList

def overwriteWrapper():
    tbfile=global_conf.owlist % (global_conf.declient)
    try:
        for line in open(tbfile,"r"):
            tablename = line.strip("\n")
            overwrite(tablename)
    except IOError:
        print("owerwrite-table-file:%s not exists." % (tbfile))
      
def overwrite(tablename):
    dstproject=global_conf.base_project
    srcproject=global_conf.result_project
    confirm=raw_input("Warning:you will overwrite the content:%s in project:%s,confrim: Yes/no" % (tablename,dstproject))
    if confirm != "Yes":
        print("confirm:%s not Yes,exit!" % (confirm))
        return 1
    partList=getPartList(srcproject,tablename)
    if len(partList) == 0:
        LOG.info("tablename:%s.%s has no partitions." % (srcproject,tablename))
        doOverwrite(".".join([dstproject,tablename]),".".join([srcproject,tablename]),"*")
        return 0
    cols=getNonFields(srcproject,tablename)
    for part in parts:
        print("tablename:%s part:%s" % (tablename,part))
        try:
            part=part.replace("/",",").replace(",","\",").replace("=","=\"")+"\""
            part=part.replace("\"\"","\"")
            partswhere=" and ".join([singlepart for singlepart in part.split(",")])
            partswhere=partswhere.replace("\"\"","\"")
            dsttp = "%s partition(%s)" % (dstproject,tablename)
            srctp = "%s where %s"  % (srcproject,partswhere)
            doOverwrite(dsttp,srctp,cols)
        except Exception as e:
            LOG.fatal("['error', ('Exception: %s' tablename:%s)]" % (traceback.format_exc(),tablename))


def getNonFields(projectname,tablename):
    cmd='''%s -a %s -t %s ''' % (global_conf.getfield,projectname,tablename)
    LOG.info("cmd:%s" % (cmd))
    r=commonUtility.do_cmd(cmd)
    if r[0] == 0:
        return r[1]
    else:
        return ""

def doOverwrite(dsttp,srctp,cols):
    r=(1,"","")
    fname=dsttp.split()[0]
    hql='''INSERT OVERWRITE TABLE %s SELECT %s FROM  %s''' % (dsttp,cols,srctp)
    print(hql)
    open(fname,"w").write(hql)
    cmd='''%s/bin/odpscmd -f %s ''' % (global_conf.declient,fname)
    print(cmd)
    r=commonUtility.do_cmd(cmd)
    

def main():
    steps = sys.argv[3:]
    suit_dir = sys.argv[1]
    WorkLogDir = sys.argv[2]
    #suit_dir for sqls in testsuite,and WorkLogDir where the working logs exist
    #/apsara/ganjiang/Python-2.7/bin/python2.7 alifin_regression/regtest/CompileDriver.py alifin_regression/case_0801 log_$stamp run verify report 
    suit = Suit(suit_dir, WorkLogDir)
    remain_args = sys.argv[1:]
    invoke = False
    LOG.info('load odps config')
    suit.load_modeling('odps')
    LOG.info('load verify config')
    suit.load_verify()
    if 'drop_dirty' in steps:
        suit.drop_dirty()
    if 'meta' in steps:
        suit.run_meta()
    if 'base' in steps:
        suit.load_modeling('de')
        invoke = True
    if 'run' in steps:
        invoke = True
    if 'verify' in steps:
        invoke = True
    if invoke:
        suit.start()
    if 'report' in steps:
        suit.report()
    if 'overwrite' in steps:
        overwriteWrapper()
    
            
if __name__ == '__main__':
    main()


