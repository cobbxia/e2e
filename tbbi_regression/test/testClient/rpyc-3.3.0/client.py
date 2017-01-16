# coding=utf-8
# client.py
#-----------------------------------------------

import rpyc
import sys
#remoteHost="10.101.92.211"
#10.101.90.203
#10.101.91.239
remoteHost=sys.argv[1]
cmd=sys.argv[2]
c=rpyc.connect(remoteHost,12233)
#print c.root.sum(1,2)
#cmd='''grep "FAIL"  /home/admin/alisatasknode/taskinfo/*/*/*/*/*/*/*.log'''
#cmd='''grep "FAIL"  /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/ '''
print c.root.show(cmd)
c.close()

