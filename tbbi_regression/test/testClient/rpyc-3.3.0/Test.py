import subprocess,os,time,sys

class Test():
    def __init__(self):
        self.defaultproject="tbbi_isolation"
    def do_cmd(self,cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        return p.returncode, out, err
    def setkey(self,key,argdict,defaultval=""):
        if key not in argdict:
            setattr(self,key,defaultval)
        else:
            setattr(self,key,argdict[key])
        print(key,getattr(self,key))
    def getFailure(self,argdict):
        print(self.__dict__)
        self.setkey("key",argdict,"Table not found")
        self.setkey("starttime",argdict,"")
        self.setkey("endtime",argdict,"")
        self.setkey("func",argdict,"")
        self.setkey("ip",argdict,"")
        self.setkey("issucc",argdict,"")
        self.setkey("nodeid",argdict,"")
        print(self.func)
        return getattr(self,self.func)()
    def helloworld(self):
        return "hello\tworld\n1\t2\n"

    def getFailLogFile(self):
        if self.starttime =="":
            return "starttime not set"
        timeArray = time.strptime(self.starttime,"%Y%m%d%H%M%S")
        timeStamp = int(time.mktime(timeArray))
        endtimeArray = time.strptime(self.endtime,"%Y%m%d%H%M%S")
        endtimeStamp = int(time.mktime(endtimeArray))
        nodefile="/home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/%s" % (self.nodeid)
        cmd="ls %s/*/*/*.log" % (nodefile)
        print("cmd:%s" % (cmd))
        r=self.do_cmd(cmd)
        content=""
        if r[0] == 0:
            filelist=r[1].split("\n")
            for info in filelist:
                filename=info.split(":")[0]
                if not  os.path.exists(filename):
                    continue
                filestamp=os.path.getmtime(filename)
                if filestamp >= timeStamp and filestamp <= endtimeStamp:
                    content=content+"".join(open(filename,"r").readlines())
        return content
        
    def getcount(self):
        print(self.starttime)
        if self.starttime =="":
            return "starttime not set"
        timeArray = time.strptime(self.starttime,"%Y%m%d%H%M%S")
        timeStamp = int(time.mktime(timeArray))
        endtimeArray = time.strptime(self.endtime,"%Y%m%d%H%M%S")
        endtimeStamp = int(time.mktime(endtimeArray))
        print("timeStamp:%s" % (timeStamp))
        if self.issucc == "succ":
            cmd="grep -v 'ERROR' /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/*/*/*/*.log"
        else:
            cmd="grep 'ERROR' /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/*/*/*/*.log"
        print("cmd:%s" % (cmd))
        r=self.do_cmd(cmd)
        #print(r[0])
        infos=""
        taskidList=[]
        infolist=[]
        count=0
        if r[0] == 0:
            filelist=r[1].split("\n")
            for info in filelist:
                filename=info.split(":")[0]
                if not  os.path.exists(filename):
                    continue
                filestamp=os.path.getmtime(filename)
                if filestamp >= timeStamp and filestamp <= endtimeStamp:
#                     /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/4549351/21-58-59/ib2fqnve4tza5nrik70gq82r/T3_0002042215.log
                    taskid=filename.split('/')[8]
                   # print(filename)
                    if taskid not in taskidList:
                        #print(taskid)
                        taskidList.append(taskid)
                        infolist.append(taskid+"\t"+self.ip)
        #print(infolist)
        return "\n".join(infolist)
    def getkeyword(self):
        print(self.starttime)
        if self.starttime =="":
            return "starttime not set"
        keyfile= self.key.replace(" ","_")+".txt"
        print(keyfile)
        timeArray = time.strptime(self.starttime,"%Y%%d%H%M%S")
        timeStamp = int(time.mktime(timeArray))
        print("timeStamp:%s" % (timeStamp))
        #/home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/3450356/10-55-04/x39ibcpgasa059j2j3j9fp9h/T3_0002159004.log
        cmd="grep '%s' /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/*/*/*/*.log" % (self.key)
        print("cmd:%s" % (cmd))
        r=self.do_cmd(cmd)
        print(r[0])
        infos=""
        infolist=[]
        if r[0] == 0:
            filelist=r[1].split("\n")
            for info in filelist:
                filename=info.split(":")[0]
                #print("filename:%s" % (filename))
                if filename.find("taskinfo") < 0:
                    continue
                filestamp=os.path.getmtime(filename)
                if filestamp >= timeStamp:
                    #print("timeStamp:%s\tfilestamp:%s\ninfo:%s" % (timeStamp,filestamp,info))
                    if info.find(self.key):
    	                tablename=info.split()[-1].replace("'","")
                        projectname=self.getProjectname(filename,tablename)
                        if tablename not in infolist:
                            infolist.append(tablename)
                            infos=infos+"\n"+tablename+"\t"+projectname
                            print(tablename,projectname,filename)
        return infos
    def getProjectname(self,filename,tablename):
        print(filename)
        projectname=""
        for line in open(filename,"r"):
            line=line.strip("\n")
            if line == "": 
                continue
            if line.find(tablename)<0:
                continue
            for word in line.split():
                if word.find("."+tablename)>=0 and word.find("\"")<0 and word.find("'")<0 and word.find("=")<0:  
                    projectname=word.split(".")[0]
                    if not word.startswith(projectname):
                        projectname=""
                #if word.find(tablename) < 0:
                #    continue
                #if word.replace("'","").replace("\"","") == tablename:
                #    continue
        if projectname == "": 
            projectname=self.defaultproject
        return projectname
