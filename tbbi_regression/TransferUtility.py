#coding: utf-8
import ConfigParser,subprocess
import re,os,sys,traceback,getopt, logging, commonUtility, threading, pickle, string,functools,subprocess
from commonUtility import do_cmd,Singleton,getExecutorList
import getopt

def Usage():
    print(sys.argv)
    print("%s -o,--operator" % (sys.argv[0]))
    print("-c,--config: configFile")
    print('-h,--help: print help message.')
    print('-v, --version: print script version')
    print('-f, --sessionflag: session level flags')
    print('--isverify: wheather to do verify')
    sys.exit(1)

def Version():
    print 'Runner.py 1.0.0.0.1'

class GlobalConf(Singleton):
    def __init__(self):
        self.loaded=False
    def parse(self,argv):
        try:
            opts, args = getopt.getopt(argv[1:], 'o:c:hvf:', ['operator=','config=','isverify=', 'sessionflag='])
        except getopt.GetoptError, err:
            Usage()
            gc.log.fatal("error:%s Exception:%s" % (err,traceback.format_exc()))
            sys.exit(2)
        self.configFile="config.py"
        print("opts:%s,args:%s" % (opts,args))
        for o, val in opts:
            if o in ('-h', '--help'):
                Usage()
                sys.exit(1)
            elif o in ('-v','--version'):
                Version()
                sys.exit(0)
            elif o in ('-c','--config'):
                self.configFile=val
                print("--configFile:%s" % (self.configFile))
            elif o in ('-o', '--operator'):
                self.operator=val
                print("--operator:%s" % (self.operator))
            elif o in ('-f','--sessionflag'):
                self.sessionflag=val
                if val == '""':
                    self.sessionflag=""
                print("--sessionflag:%s" % (self.sessionflag))
            elif o in ('--isverify',):
                self.isverify="yes"
                if val is not None and val.lower()== "no":
                    self.isverify="no"
                print("--sessionflag:%s" % (self.isverify))
            else:
                print('unhandled option')
        if "operator" not in self.__dict__ :
            print("operator not set")
            Usage()
            sys.exit(2)
        else:
            print("--operator:%s" % (self.operator))
    def set(self,key,val):
        self.__dict__[key]=val
    def setConf(self,key,section,option,default=""):
        if self.config.has_option(section,option):
            self.__dict__[key]=self.config.get(section,option)
        else:
            self.__dict__[key]=default
        self.log.debug("%s:%s" % (key,self.__dict__[key]))
    def getVerifyPlan(self):
        for line in open(self.verifyplan,"r"):
            line=line.strip("\n")
            if line != "":
                if len(line.split()) == 1:
                    if line not in self.res2nodeDict :
                        continue
                    nodeid = self.res2nodeDict[line]
                    if nodeid in self.blackList:
                        continue
                    self.verifyplanList.append(line)
                else:
                    self.verifyplanList.append(line.split()[0])
    def getDict(self,res2node):
        self.res2nodeDict={}
        self.node2resDict={}
        for line in open(res2node,"r"):
            line=line.strip("\n")
            tablename=line.split()[0]
            nodeid=line.split()[1]
            if os.path.sep.join([self.renderdir,nodeid]) not in self.renderList:
                self.log.debug("table:%s not in renderList skip." % (tablename))
                continue
            if nodeid not in self.node2resDict:
                self.node2resDict[nodeid]=tablename
            else:
                self.node2resDict[nodeid]=self.node2resDict[nodeid]+","+tablename
            self.res2nodeDict[tablename]=nodeid
        self.log.info("self.res2nodeDict:%s" % (self.res2nodeDict))
    def load(self,**argDict):
        if self.loaded==True:
            return 0
        if "argv" in argDict:
            self.parse(argDict["argv"])
        elif "configFile" in argDict and "operator" in argDict:
            self.operator=argDict["operator"]
            self.configFile=argDict["configFile"]
        logFileName=self.operator+".log"
        self.statusDict={"done-success":0,"done-error":1,"running":2,"running-error":3,"verify":4,"verify-error":5}
        self.retrytimes=3
        self.opList=[]
        self.config= ConfigParser.ConfigParser()
        self.config.read(self.configFile)
        if self.config.has_option("log","logname"):
            self.logname=config.get("log","logname")
        else:
            self.logname="main"
        if self.config.has_option("log","level"):
            self.loglevel = self.config.get("log","level")
        else:
            self.loglevel ="info"
        if self.config.has_option("log","logdir"):
            self.logdir= self.config.get("log","logdir")
        else:
            self.logdir="./log"
        print("logdir:%s" % (self.logdir))
        if self.config.has_option("log","logfile"):
            self.logfile= self.logdir+"/"+self.config.get("log","logfile")
        else:
            self.logfile= self.logdir+"/"+logFileName
        print("logFileName:%s" % (self.logfile))
        self.numeric_level = getattr(logging, self.loglevel.upper(),None)
        s = '%(levelname)-12s %(asctime)s %(name)-8s [%(thread)d][%(threadName)s][%(module)s:%(funcName)s:%(lineno)d] %(message)s'
        logging.basicConfig(level=self.numeric_level,filename=self.logfile,format=s)
        self.log=logging.getLogger("main")

        #################################################################################################
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter(s)
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        #################################################################################################

    def loaddata(self):
        self.blackList=[line.strip("\n") for line in open(self.blackfile,"r")]
        self.renderList=[self.renderdir+"/"+dirname.strip("\n") for dirname in os.listdir(self.renderdir) if dirname.strip("\n") not in self.blackList]
        self.tableList=[]
        self.resourceList=[]
        self.failList=[]
        self.verifyplanList=[]
        self.getDict(self.res2node)
        self.getVerifyPlan()
        self.executorList=getExecutorList(self.executor_hosts)
        self.nodeWeightList=[ line.strip("\n").split()[0] for line in open(self.node2weight)]
        self.timestamp=open(self.timestampfile,"r").readlines()[0].strip("\n")
        self.log.info("executorList:%s" % (self.executorList))
        self.log.info("config laod over")
        self.loaded=True
        self.isrunning=False
        return 0

if __name__ == '__main__':
    test()
