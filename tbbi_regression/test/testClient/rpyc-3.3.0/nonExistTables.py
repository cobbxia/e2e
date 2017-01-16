def genPk():
    infile="/home/mingchao.xiamc/addtable/nonExistTables.txt"
    outfile=open("/home/mingchao.xiamc/addtable/nonExistTables.pk","w")
    outdict={}
    for line in open(infile,"wb"):
        line=line.strip("\n")
        k=line.split()[0]
        v=line.split()[1]
        outdict[k]=v
    import pickle
    pickle.dump(outdict,outfile)

genPk()
