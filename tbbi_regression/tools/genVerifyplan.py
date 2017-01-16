res2nodeDict={}
for line in open("./res2node.txt","r"):
    line=line.strip("\n")
    res2nodeDict[line.split()[0]]=line.split()[1]
outfp=open("./verifyplan.txt","w")
for line in open("./tableList.txt","r"):
    line=line.strip("\n")
    tablename=line.split()[0]
    if tablename in res2nodeDict:
        outfp.write('%s\ttbbi_isolation\ttbbi_isolation_prd5_dst_0\n' % (tablename))
