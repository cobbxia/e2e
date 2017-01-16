headerList=[300.0,600.0,1200.0,1800.0,3600.0]

def runtime(runtimefile):
    runtimedict={}
    headcntDict={300.0:0,600.0:0,1200.0:0,1800.0:0,3600:0}
    for line in open(runtimefile,"r"):
        line=line.strip("\n")
        if line == "": continue
        fn=line.split(":")[0].split("/")[-1].split(".")[0]
        ts=float(line.split(":")[2])
        headerIndex=0
        for headerIndex in range(len(headerList)):
            header = headerList[headerIndex]
            if ts > header:
                headcntDict[header] = headcntDict[header] +1
        runtimedict[fn]=ts
    sorted(runtimedict.items(), key=lambda d:d[1], reverse=False)
    filenum=len(runtimedict)
    print(headcntDict)
    #print("len:%d" % (filenum))


def durtime(runtimefile,verify=""):
    runtimedict={}
    headcntDict={300.0:0,600.0:0,1200.0:0,1800.0:0,3600:0}
    cnt=0
    maxts=0
    maxfn=""
    for line in open(runtimefile,"r"):
        line=line.strip("\n")
        if line == "": continue
        cnt = cnt +1
        fn=line.split(":")[0].split("/")[-1].split(".log")[0]
        #t_rkadm_yjmol_spam_bothside_ms_4_q346165_6:2780.8968091
        if verify != "":
            fn="q"+line.split(":")[0].split("_q")[1].split(".log")[0]+".sql"
        ts=float(line.split("durtime:")[1])
        if ts > maxts:
            maxts=ts
            maxfn=line
        headerIndex=0
        for headerIndex in range(len(headerList)):
            header = headerList[headerIndex]
            if ts <  header:
                headcntDict[header] = headcntDict[header] +1
                break
        runtimedict[fn]=ts
    sorted(runtimedict.items(), key=lambda d:d[1], reverse=False)
    filenum=len(runtimedict)
    headerIndex=0
    print("Total number:%d" % (cnt))
    print("maxts:%f maxfn:%s" % (maxts,maxfn))
    for headerIndex in range(len(headerList)):
        header = headerList[headerIndex]
        print("lighter than %s seconds: %d" % (header,headcntDict[header]))
    #print("len:%d" % (filenum))

print("sql jobs")
durtime("./case_0801/runtime.txt")
print("verify jobs")
durtime("./case_0801/verify_runtime.txt","v")
