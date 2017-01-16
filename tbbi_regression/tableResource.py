from commonUtility import do_cmd
import sys
def process(linefile):
    projectname="bi_udf"
    line="".join(open(linefile,"r").readlines())
    jarcmd=line
    print("jarcmd:%s" % (jarcmd))
    cmdlist=jarcmd.split()
    tablename=cmdlist[2].split(".")[1]
    srcprojectname=cmdlist[2].split(".")[0]
    print("tablename:%s" % (tablename))
    cmd=""
    print(cmd)
    r=do_cmd(cmd)
    if r[0] != 0:
        print("ERROR:%s\ncmd:%s\nr1:%s\nr2:%s" % (line,cmd,r[1],r[2]))
        return
    ddl=r[1]
    tmpfilename="%s_hql.tmp" % (linefile)
    open(tmpfilename,"w").write('''use %s;%s''' % (projectname,ddl))
    print(open(tmpfilename,"r").readlines())
    cmd=""
    print(cmd)
    r=do_cmd(cmd)
    if r[0] != 0:
        print("ERROR:%s\ncmd:%s\nr1:%s\nr2:%s" % (line,cmd,r[1],r[2]))
        return

    partname=cmdlist[3]
    if partname.find("(") >=0 :
        partname=partname.split('(')[1].strip(')')
        partname=partname.replace('\'',"").replace('"',"")
        print(partname)
        cmd=""
        print(cmd)
        r=do_cmd(cmd)
        if r[0] != 0:
            print("ERROR:%s\ncmd:%s\nr1:%s\nr2:%s" % (line,cmd,r[1],r[2]))
            return
        print(r[1])
        print(r[2])
    else:
        cmd=""
        print(cmd)
        r=do_cmd(cmd)
        if r[0] != 0:
            print("ERROR:%s\ncmd:%s\nr1:%s\nr2:%s" % (line,cmd,r[1],r[2]))
            return
        print(r[1])
        print(r[2])
        
process(sys.argv[1])
