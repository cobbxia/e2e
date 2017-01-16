# coding=utf-8
#server.py
#-----------------------------------------------
import time,subprocess
from rpyc import Service
from rpyc.utils.server import ThreadedServer
import sys
g_module=None

class TimeService(Service):
    def exposed_sum(self,a,b):
        return a+b
    def exposed_show(self,cmd):
        #cmd='''grep "FAIL"  /home/admin/alisatasknode/taskinfo/*/*/*/*/*/*/*.log   '''
        return self.do_cmd(cmd)[1]
    def do_cmd(self,cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        return p.returncode, out, err    
    def exposed_process(self,argv):
        self.modulePath="/home/admin/rpyc-3.3.0/server.py"
        global g_module
        if g_module is None:
            print("none")
            g_module=__import__('Test')
        else:
            print("reload")
            g_module=reload(g_module)
        Test=getattr(g_module,"Test")
        t=Test()
        return t.getFailure(argv)
    def exposed_update(self,content):
        outfile=open("Test.py","w")
        outfile.write(content)
        return "OK"
        
s=ThreadedServer(TimeService,port=12233,auto_register=False)
s.start()
