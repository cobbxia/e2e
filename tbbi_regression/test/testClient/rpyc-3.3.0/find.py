def f():
    d1={}
    d2={}
    for line in open("/home/mingchao.xiamc/addtable/taskid_base/fail_id.txt","r"):
        line=line.strip("\n")
        nodeid=line.split("\t")[0]
        ip=line.split("\t")[1]
        d2[nodeid]=ip
    #for line in open("/home/mingchao.xiamc/addtable/taskid/fail_id_uniq.txt","r"):
    for line in open("/home/mingchao.xiamc/addtable/taskid/fail_id_20150325.txt","r"):
    #for line in open("./20150325.txt","r"):
        line=line.strip("\n")
        #nodeid=line.split("\t")[0]
        #ip=line.split("\t")[1]
        if line not in d2:
            d1[line]=""
    for k in d1:
        print(k)


def flog():
    d1={}
    d2={}
    for line in open("/home/mingchao.xiamc/addtable/taskid_base/fail_id.txt","r"):
        line=line.strip("\n")
        nodeid=line.split("\t")[0]
        ip=line.split("\t")[1]
        d2[nodeid]=ip
    #for line in open("/home/mingchao.xiamc/addtable/taskid/fail_id_uniq.txt","r"):
    for line in open("/home/mingchao.xiamc/addtable/taskid/fail_id_20150325.txt","r"):
    #for line in open("./20150325.txt","r"):
        line=line.strip("\n")
        nodeid=line.split("\t")[0]
        ip=line.split("\t")[1]
        if nodeid not in d2:
            d1[nodeid]=""
    for k in d1:
        print(k)
flog()
