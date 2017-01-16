import rpyc
def Table_not_found(remoteHost,argdict):
	c=rpyc.connect(remoteHost,12233)
	infos=c.root.process(argdict)
	c.close()
	return infos    
#test=Test()
ipList=["10.101.92.211","10.101.90.203","10.101.91.239"]
ip="10.101.92.211"

argdict={"key":"Table not found","starttime":"2015:03:02:00:00:00"}
print(Table_not_found(ip,argdict))

