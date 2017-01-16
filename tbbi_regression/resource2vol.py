import subprocess,os,sys,socket,time, random, re, sys ,traceback
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

def do_cmd(cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    print("%s\n" % (cmd))
    p = subprocess.Popen(cmd, shell=True,close_fds=True, stdout=stdout, stderr=stderr)
    out, err = p.communicate()
    if p.returncode !=0:
        print("stdout:%s\n" % (out))
        print("stderr:%s\n" % (err))
    return p.returncode, out, err 

def do_odps_cmd(odps,sql,retrytimes=""):
    if retrytimes is None or retrytimes=="":
        retrytimes=3
    r=(1,'','')
    cmd='''%s -e "%s" ''' % (odps,sql)
    i=0
    while i < retrytimes:
        i=i+1
        try:
            r=do_cmd(cmd)
            if r[0] == 0:
                break
            elif i <= retrytimes:
                print("cmd retrytimes:%d error:%s r1:%s r2:%s" % (i,cmd,r[1],r[2]))
        except Exception as e:
            print("['error', ('cmd:%s r1:%s r2:%s Exception: %s')]" % (cmd,r[1],r[2],traceback.format_exc()))
    return r

class RES:
    def __init__(self,panguroot,projectname,temproot,odps,paranum):
        self.odps=odps
        self.panguroot=panguroot
        self.projectname=projectname
        self.pangudir="%s/%s/resources" % (self.panguroot,self.projectname)    
        self.resourceList=[]
        self.temproot=temproot
        self.paranum=paranum
    def getreList(self):
        do_cmd("mkdir -p %s/%s" % (self.temproot,self.projectname))
        cmd='''pu ls %s ''' % (self.pangudir)
        r=do_cmd(cmd)
        if r[0]== 0:
            self.resourceList=r[1].split("\n")
        return self.resourceList
    def move2vol(self,resname):
        print("move %s" % (resname))
        self.download(resname)
        sql='''use tbbi_out;fs -put %s/%s/%s /vol/%s/%s; ''' % (self.temproot,self.projectname,resname,self.projectname,resname) 
        r=do_odps_cmd(self.odps,sql,retrytimes=1)
        if r[0] == 0:
            print("move resname:%s success." % (resname))
        else:
            print("move resname:%s failed." % (resname))
    def domove(self):
        pool = ThreadPool(self.paranum)
        resultList = []
        self.getreList()
        for resname in self.resourceList:
            if resname == "": continue
            if resname.find("datax")>=0: continue
            resultList.append(pool.apply_async(self.download,(resname,)))
        for result in resultList:
            r=result.get()
            if r[0] != 0:
                print("download resname:%s failed." % (r[1]))
        sql='''use tbbi_out;fs -put %s/%s/ /vol/%s; ''' % (self.temproot,self.projectname,self.projectname) 
        r=do_odps_cmd(self.odps,sql,retrytimes=1)
        if r[0] == 0:
            print("move project %s success." % (self.projectname))
        else:
            print("move project %s failed. " % (self.projectname))
    def download(self,resname):
        cmd= '''pu cp %s/%s %s/%s/''' % (self.pangudir,resname,self.temproot,self.projectname)
        r=do_cmd(cmd)
        return (r[0],resname)

def main():
    try:
        if len(sys.argv) < 2:
            print("projectname not set")
            sys.exit(0)
        projectname=sys.argv[1]
        panguroot=""
        localtemp=""
        odps=""
        res=RES(panguroot,projectname,localtemp,odps,8)
        res.domove()
    except Exception as e:
        print("['error', ('Exception: %s')]" % (traceback.format_exc()))

if __name__ == '__main__':
    main()
