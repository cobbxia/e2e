from commonUtility import do_cmd
succdir="./succ"
sqldir="./sql"
renderdir="./renderSql"
renderlogdir="/render/198001010000"
templatedir="./template"
errorid="./errorid"
basedir="/home/admin/mingchao.xiamc/pydata/verify/logs/"
errordict={}
#for line in open("./errorid.txt"):
#for line in open("./FAILid.txt"):
for line in open("fail_id_sort.txt"):
    id=line.strip("\n")
    errordict[id]=""
    cmd="rm -f %s" % (basedir+"/"+renderlogdir+"/"+id+"_0_*" )
    print(cmd)
    r=do_cmd(cmd)

    cmd="rm -f %s" % (basedir+"/"+renderdir+"/"+id+"_0" )
    print(cmd)
    r=do_cmd(cmd)

    cmd="rm -f %s" % (basedir+"/"+sqldir+"/"+id)
    print(cmd)
    r=do_cmd(cmd)
   
    cmd="rm -rf %s" % (basedir+"/"+templatedir+"/"+id)
    print(cmd)
    r=do_cmd(cmd)

    cmd="rm -f %s" % (basedir+"/"+succdir+"/"+id)
    print(cmd)
    r=do_cmd(cmd)


outfp=open(basedir+"new_succList.txt","w")
for line in open(basedir+"/succList.txt"):
    id=line.strip("\n")
    if id not in errordict:
        outfp.write(id+"\n")
outfp.close()
