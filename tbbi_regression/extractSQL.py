import os
def addtag(token,tag):
    projectname=token.split(".")[0]
    tablename=".".join(token.split(".")[1:])
    #print("projectname:%s tablename:%s" % (projectname,tablename))
    return projectname+tag+"."+tablename

succdir="./succ"
sqldir="./sql"
renderdir="./renderSql"
templatedir="./template"
defaultDstProject="tbbi_isolation"
tailtag="$$index$$"
maxnum=9

def render(nodeid,paralellnum):
    if paralellnum > maxnum :
        paralellnum=maxnum
    for i in range(0,paralellnum):
        doRender(os.sep.join([templatedir,nodeid]),os.sep.join([renderdir,nodeid+"_"+str(i)]),str(i))

def doRender(infilename,outfilename,index="1"):
    #open(outfilename,"w").write("".join(line.replace("$$index$$",index) for line in open(infilename,"r")))
    open(outfilename,"w").write("".join(line.replace(tailtag,index) for line in open(infilename,"r")))

def makeSQLTemplate(nodeid):
    outtablefp=open("./outtablelist.txt","a")
    outfilename=os.sep.join([templatedir,nodeid])
    infilename=os.sep.join([sqldir,nodeid])
    outfp=open(outfilename,"w")
    pretoken=""
    prepretoken=""
    token=""
    #dsttag="_prd5_dst_$$index$$"
    dsttag="_prd5_dst_"+tailtag
    srctag="_prd5_src"
    for line in open(infilename,"r"):
        for token in line.split():
            if pretoken.lower() == "table" and (prepretoken.lower()=="overwrite" or prepretoken.lower()=="into"):
                outtablefp.write(token+"\t"+nodeid+"\n")
                if token.find(".") > 0:
                    token=addtag(token,dsttag)
                else: 
                    token=defaultDstProject+dsttag+"."+token
            #elif pretoken.lower() == "from" and token.find(".") >0:
            #    token=addtag(token,srctag)
            outfp.write(token+" ")
            prepretoken=pretoken
            pretoken=token
        outfp.write("\n")
    outfp.close()
    outtablefp.close()

def extractSQL(nodeid):
    flag=0
    outfp=open(os.sep.join([sqldir,nodeid]),"w")
    clen=0
    preline=""
    content=""
    precontent=""
    for line in open(succdir+"/"+nodeid,"r"):
        #print("flag:%d line:%s clen:%d" % (flag,line,clen))
        line = line.strip("\n")
        if line == "" : continue
        if line.find(" sql:") > 0 and flag == 0:
            flag=flag |  2  
            continue
#        if line.startswith("set "):
#            flag = flag | 4
#        if line.find(";") >=0 and flag &4 == 4:
#            flag = flag - 4
#            continue
          
        if flag & 2 ==2 and flag &4 != 4:
            if (line.strip().startswith("OK") or line.strip().startswith("FAILED")) and (preline.find(';')>=0 or line.find(";")>=0) and clen > 10:
            #if line.find(";") >= 0 and clen > 10:
                if precontent != content:
                    outfp.write(content)
                flag = 0
                clen=0
                precontent=content
                content=""
                continue
            clen=clen+len(line) 
            content=content+line+"\n"
        preline=line
        #print("flag:%d line:%s clen:%d" % (flag,line,clen))


def processNode(nodeid):
    extractSQL(nodeid)
    makeSQLTemplate(nodeid)
    render(nodeid,1)

def main():
    if not os.path.exists(succdir):
        os.mkdir(succdir)
    if not os.path.exists(sqldir):
        os.mkdir(sqldir)
    if not os.path.exists(templatedir):
        os.mkdir(templatedir)
    if not os.path.exists(renderdir):
        os.mkdir(renderdir)
    for nodeid in open("./succList.txt","r"):
        nodeid=nodeid.strip("\n")
        processNode(nodeid)

main()
#processNode("3474095")

#makeSQLTemplate("./sample.sql","./template/sample.sql")
#render("sample.sql",9)
#extractSQL("3450356")
