# -*- coding: utf-8 -*-
#拼接脱敏用到的SQL，从线上的project tbbi_isolation脱敏到tbbi_out
import re,ConfigParser,sys,logging,commonUtility,threading,random,string,time,traceback
from commonUtility import do_cmd,gettoday,getTableList,do_odps_filecmd,do_odps_cmd,getDayPart,issmaller,partTransfer,addpart
from Mysql import MysqlData,OdpsTableMeta,updateMysql,partIsExist,deletetable,getPartList
def updatePart(gc,partid):
    updatePartSql="update partition set flag=0 where `id` = %s" % (partid)
    updateMysql(gc,updatePartSql,"-N")

def setTransferPart(gc,partid):
    #updatePartSql="update partition set flag=CASE WHEN flag>1000 THEN flag ELSE flag+1000 where `id` = %s" % (partid)
    updatePartSql="update partition set flag=CASE WHEN flag>1000 THEN flag ELSE flag+1000 END where `id` = %s;" % (partid)
    updateMysql(gc,updatePartSql,"-N")
#将分区partday替换为gc.currentpart
def replacePartday(gc,partday):
    ret=0
    quote=0
    retday=""
    seperator=","
    gc.log.debug("partlist:%s" % (partday.split(seperator)))
    firstPart=partday.split(seperator)[0]
    rightpart=seperator.join(partday.split(seperator)[1:])
    if len(firstPart.split("=")) !=2:
        return (ret,partday)
    partfix=firstPart.split("=")[0]
    bdate=firstPart.split("=")[1]
    if bdate.startswith("'"):
        bdate=bdate.replace("'","")
        quote=1
    elif bdate.startswith("\""):
        bdate=bdate.replace("\"","")
        quote=2
    if not (len(bdate)>=8 and re.search(r'[0-9]{8}',bdate)):
        return (ret,partday)
    retday=""
    curday=gc.currentday
    if bdate.find('-') >= 0:
        return (ret,partday)
    partlen=len(bdate)
    curlen=len(curday)
    if partlen < curlen:
        return (ret,partday)
    elif curday == bdate[0:8]:
        return (ret,partday)
    gc.log.debug("curday:%s bdate:%s partday:%s rightpart:%s"  % (curday,bdate,partday,rightpart))
    for i in range(curlen):
        gc.log.debug("i:%d curday:%s"  % (i,curday[i]))
        retday=retday+curday[i]
    gc.log.debug("curlen:%d bdate tailfix:%s" % (curlen,bdate[curlen:]))
    if quote == 1:
        retday=partfix+"='"+retday+bdate[curlen:]+"'"
    elif quote == 2:
        retday=partfix+"=\""+retday+bdate[curlen:]+"\""
    else:
        retday=partfix+"="+retday+bdate[curlen:]
    if rightpart != "":
        retday=retday+seperator+rightpart
    ret=1
    return (ret,retday)   
#    if partday=="" or partday is None:
#        return partday
#    firstPart=partday.split("/")[0]
#    gc.log.debug("firstPart:%s" % (firstPart))
#    firstPart=firstday.split("=")[1]
#    gc.log.debug("firstPart:%s" % (firstPart))
#    yList=re.split(r'[0-9]{4}',firstPart.replace("-","").strip())
#    if len(yList)==2 and yList[0] == '' and yList[1] == '':
#        gc.log.info("part:%s is year part,skip" % (partday))
#        return partday
#    mList=re.split(r'[0-9]{6}',firstPart.replace("-","").strip())
#    if len(mList)==2 and mList[0] == '' and mList[1] == '':
#        gc.log.info("part:%s is month part,skip" % (partday))
#        return partday
#    la=re.split(r'[0-9]{8}',firstPart.replace("-","").strip())
#    gc.log.debug("la:%s" % (la))
#    if len(la) <= 1:
#        gc.log.info("part:%s never has a day part" % (la))
#        return parday
#    bdate=firstPart[0:8]
#    gc.log.debug("bdate:%s" % (bdate))
#    val=la[1]
#    if re.search(r'[1-9]{1,6}',val):
#       gc.log.fatal("has time partition:%s" % (partday))
#       return partday
#    else:
#       gc.log.info("day part:%s" % (partday))
#       return (0,bdate,partday)

def tagWholeTable(gc,tablename):
    tablemeta=OdpsTableMeta(gc)
    tableid=tablemeta.getTableId(tablename,"")
    if tablemeta.isPartedTable()!=0:
        pass

def testgetNonPartedColList():
    gc=GlobalConf()
    if gc.load("config.py","test.log")!=0:
        print("can not load config file:%s" % (configFile))
        sys.exit(1)
    tableid="3386"
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    print(tablemeta.getNonPartedColList())

def TransferPart(gc,tpinfo):
    #preflag='69'
    preflag='3'
    partid=tpinfo['id']
    flag=tpinfo['flag']
    tableid=tpinfo['table']
    partitionspec=tpinfo['part']
    gc.log.debug("partid=%s flag=%s tableid=%s partitionspec=%s" % (partid,flag,tableid,partitionspec))
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    curday=gc.partday
    r={}
    if int(flag)&int(preflag) != int(preflag):
        gc.log.info("flag:%s not include preflag:%s" % (flag,preflag))
        return 
    if tablemeta.isPartedTable()!=0:
        gc.log.info("flag:%s tablename:%s not parted no need to transfer" % (flag,tablename))
        setTransferPart(gc,partid)
        return
    else:
        cols=",".join([col for col in tablemeta.getNonPartedColList("original")])
        gc.log.debug("cols:%s" % (cols))
        partname=partitionspec.replace("/",",")
        partname=partname.replace("=","=\"").replace(",","\",")+"\""
        gc.log.info("partname:%s" % (partname))
        partswhere=" and ".join([partwhere for partwhere in partname.split(",")])
        partswhere=partswhere.replace("\"\"","\"")
        gc.log.debug("partwhere:%s" % (partswhere))
        gc.log.debug("tablename:%s partitionspec:%s before replacePartday\n" % (tablename,partitionspec))
        repret=replacePartday(gc,partname)
        gc.log.debug("after replacePartday tablename:%s repret:%s\n" % (tablename,repret))
        if repret[0] == 0:
            gc.log.info("tablename:%s partitionspec:%s already on curday partition,no need to transfer" % (tablename,partitionspec))
            setTransferPart(gc,partid)
            return
        partname=repret[1]
        if partIsExist(gc,tableid,partname) != 0:
            gc.log.info("partid:%s tablename:%s partname:%s already exists,no need to transfer" % (partid,tablename,partname))
            setTransferPart(gc,partid)
            return
        gc.log.debug("partname:%s\tpartitionspec:%s\tpartid:%s" % (partname,partitionspec,partid))
        stime=int(time.time())
        try:
            #partswhere=" and ".join([partTransfer(gc,partwhere) for partwhere in partname.split(",")])
            # pt=1234 to pt="1234"
            hql='''use %s; insert overwrite table %s.%s partition(%s)  select %s from %s.%s where %s;'''  %  (gc.kproject,gc.kproject,tablename,partname,cols,gc.kproject,tablename,partswhere)
            gc.log.debug("hsql:%s" % (hql))
            r=do_odps_filecmd(gc,gc.kodps,hql)
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (traceback.format_exc()))
            r[0]=1
            r[2]=format(str(e))
            return
        etime=int(time.time())
        cost=etime-stime
        setTransferPart(gc,partid)

#对分区数据进行打标签，flag=0才进行分区迁移，其他的进行整个表的迁移
def TagPart(gc,line):
    tablename=""
    projectname=""
    r=(1,'','')
    LineLength=len(line.split())
    if LineLength == 1:
        tablename=line.split()[0]
        return
    elif LineLength == 2: 
        tablename  =line.split()[0]
        projectname=line.split()[1]
    gc.log.debug("line:%s\tLineLength:%d\ttablename:%s\tprojectname:%s" % (line,LineLength,tablename,projectname))
    tablemeta=OdpsTableMeta(gc)
    tableid=tablemeta.getTableId(tablename,projectname)
    if tableid == "":
        gc.log.info("table:%s not exist" % (tablename))
        return ""
    partSql="select * from partition where `table` = %s" % (tableid)
    gc.log.debug("partSql:%s" % (partSql))
    mydata=MysqlData(gc,partSql)
    if mydata.RowNum()==0:
        return ""
    partList=[]
    partIdList=[]
    latestday=""
    curday=gc.currentday
    for row in mydata.Rows():
        #对分区特征进行判断和提取
        r=getDayPart(gc,row['part'])       
        #只有日表和小时表、其他表才进行处理，但是预处理过程中发现其实并不存在小时表，所以只处理2和0
        if r[0]==2:
            updatePart(gc,row['id'])   
        elif r[0]==0:
            #今天分区的数据不ok，要使用昨天分区的数据
            partday=r[1]
            gc.log.debug("len(partList):%d\tcurday:%s\tlatestday:%s\n" % (len(partList),curday,latestday))
            ret=issmaller(partday,curday)
            if ret==0:
                continue
            gc.log.debug("partday:%s curday:%s ret:%d" % (partday,curday,ret))
            if len(partList) == 0 or (partday==latestday  and partday!=None):
                latestday=partday
                partList.append(row['part'])
                partIdList.append(row['id'])
            elif partday>latestday:
                latestday=partday
                partList=[]
                partIdList=[]
                partList.append(row['part'])
                partIdList.append(row['id'])
        gc.log.debug("partList:%s\npartIdList:%s\n" % (partList,partIdList))
    for i in range(len(partIdList)):
        gc.log.info("Tag Part partid:%s part:%s" % (partIdList[i],partList[i]))
        partid=partIdList[i]
        updatePart(gc,partid)
        
def reduceQuota(quota):
    pass
def backQuota(quota):
    pass
             
def getUDFDict(gc):
    udfList=[]
    sql='''select * from udf; '''
    mydata=MysqlData(gc,sql)
    for i in range(mydata.RowNum):
        udfList.append(UDFOdpsColumn(mydata[i]))    

def isReady(gc,tableName,partition=""):
    size=0
    return size

#增加信息，脱敏用掉了多少的quota,设置状态为desec_done
def updatePartSize(gc):
    pass

#脱敏函数从数据库中取到需要脱敏的字段和函数，拼接成最后的hql，然后执行
#对于是分区表，但是却没有对应的分区的信息，就是show part为空的情况直接忽略
#还需要判断是否是分区表,如果是分区表则全部获取
#不处理对quato的修改，脱敏和托数据的程序都不依赖于quota，只是去修改各自分区中的状态，并且更新size字段
#由独立的线程/进程 每隔几秒钟，对quota统一修改一次;
#只对flag=0的分区和非分区表才处理，错误设置flag=2，完成设置flag=1
#def desen(gc,tablename):
def desen(gc,tpinfo):
    #resultDict={0:'69',1:'70'} 
    #preflag='67'
    resultDict={0:'1',1:'2'} 
    preflag='0'
    partid=tpinfo['id']
    flag=tpinfo['flag']
    tableid=tpinfo['table']
    partitionspec=tpinfo['part']
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    tablemeta.initTable(tableid)
    projectname=tablemeta.getProjectName()
    gc.log.debug("start desen projectname:%s tablename:%s" % (projectname,tablename))
    r={}
    if int(flag) & int(preflag) != int(preflag):
        gc.log.info("flag:%s not include preflag:%s" % (flag,preflag))
        return 1
    parts=getPartList(gc,gc.srcodps,tablename,projectname)
    if tablemeta.isPartedTable()!=0:
        #(partid,flag)=tablemeta.getPartId("")
        gc.log.info("flag:%s partid:%s tablename:%s " % (flag,partid,tablename))
        stime=int(time.time())
        if projectname is None or projectname == "":
            projectname=gc.kproject
        try:
            allCols=",".join([col for col in tablemeta.getAllColList()])
            if allCols == "" :
                allCols="*"
            hql='''use %s; insert overwrite table %s.%s select %s from %s.%s '''  %  (gc.outproject,gc.outproject,tablename,allCols,projectname,tablename)
            gc.log.debug("hsql:%s" % (hql))
            r=do_odps_filecmd(gc,gc.dstodps,hql)
        except Exception as e:
            gc.log.fatal("['error', (Exception: %s tablename:%s)]" % (traceback.format_exc(),tablename))
            r[0]=1
            r[2]=format(str(e))
        etime=int(time.time())
        cost=etime-stime
        updateMetaInfo(r,partid,gc,resultDict,cost=cost)
    elif tablemeta.isPartedTable()==0 and len(parts) == 0:
        r[0]=1
        r[2]="no data"
        cost=0
        updateMetaInfo(r,partid,gc,resultDict,cost=cost)
    else:
        gc.log.info("parted table tablename:%s" % (tablename))
        cols=",".join([col for col in tablemeta.getNonPartedColList()])
        gc.log.debug("cols:%s" % (cols))
        partname=partitionspec.replace("/",",")
        gc.log.debug("partname:%s\tpartitionspec:%s\tpartid:%s" % (partname,partitionspec,partid))
        stime=int(time.time())
        try:
            partname=partname.replace("=","=\"").replace(",","\",")+"\""
            gc.log.info("partname:%s" % (partname))
            partswhere=" and ".join([singlepart for singlepart in partname.split(",")])
            partswhere=partswhere.replace("\"\"","\"")
            gc.log.debug("partwhere:%s" % (partswhere))
            newpartname=" , ".join([partTransfer(gc,singlepart) for singlepart in partname.split(",")])
            #newpartname=newpartname.replace("\"\"","\"")
            addpart(tablename,newpartname,gc,projectname)
            hql='''use %s; insert overwrite table %s.%s partition(%s)  select %s from %s.%s where %s;'''  %  (gc.outproject,gc.outproject,tablename,newpartname,cols,projectname,tablename,partswhere)
            gc.log.debug("hsql:%s" % (hql))
            r=do_odps_filecmd(gc,gc.dstodps,hql)
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s tablename:%s')]" % (traceback.format_exc(),tablename))
            r[0]=1
            r[2]=format(str(e))
        etime=int(time.time())
        cost=etime-stime
        updateMetaInfo(r,partid,gc,resultDict,cost=cost)
    gc.log.info("finish desen tablename:%s" % (tablename))
    return 0

def updateMetaInfo(*arglist,**argdict):
    r=arglist[0]
    partid=arglist[1]
    gc=arglist[2]
    resultDict=arglist[3]
    partinfo={}
    setclause=""
    if len(argdict) !=0:
        setclause=",".join(["%s=%s" % (key,argdict[key]) for key in argdict.keys()])
        gc.log.debug("setclause:%s" % (setclause))
    if resultDict!=None and resultDict !="":
        if r[0]==0:
           partinfo['flag']=resultDict[0]# 3表示顺利执行拷贝
        else:
           partinfo['flag']=resultDict[1] # 4表示拷贝出错
           partinfo['errmsg']=r[2]
        if setclause=="":
            setclause=" flag=flag+"+partinfo['flag']
        else:
            setclause=setclause+",flag=flag+"+partinfo['flag']
    gc.log.debug("setclause:%s" % (setclause))
    updatePartSql="update partition set %s where id=%s;"  % (setclause,partid)
    gc.log.debug("updatePartSql:%s" % (updatePartSql))
    updateMysql(gc,updatePartSql,"-N")

def usage():
    print("%s command[|tag]" % (sys.argv[0]))
    sys.exit(1)


#能够通过表名称获取到对应的导入和导出的sql，多线程放入到MultiRunner中即可,关键的流控，在执行getsize以后需要判断是否可以执行
def main():
    gc=GlobalConf()
    if gc.load("config.py",sys.argv[0].split(".")[0]+".log")!=0:
        print("can not load config file:%s" % (configFile))
        sys.exit(1)
    getTableList(gc)
    #tableName="t_pg_ipdata_detail"
    print(sys.argv,len(sys.argv))
    if len(sys.argv) <2:
        print("usage lt 3")
        usage()
    operator=sys.argv[1].lower()
    if operator=="desen":
        desen(gc,tableName)
    elif  operator=="tag":
        TagPart(gc,tablename) 
        for t in gc.tableList:
            TagPart(gc,t)
def partTransferWrapper(gc,partname):
    newpartname=" , ".join([partTransfer(gc,singlepart).replace(" ","") for singlepart in partname.split(",")])
    return newpartname

def test():
    gc=GlobalConf()
    if gc.load("config.py","test.log")!=0:
        print("can not load config file:%s" % (configFile))
        sys.exit(1)
    print(getDayPart(gc,"ds=201401"))
    print(getDayPart(gc,"ds=2015"))
    print(getDayPart(gc,"ds=2015-01"))
    print(getDayPart(gc,"ds=2015-01-01"))
    print(getDayPart(gc,"ds=201501011010"))
    print(getDayPart(gc,"is_dirty=1"))
    print(partTransfer(gc,"ds=2015"))
    print(partTransfer(gc,"ds=2015-01"))
    print(partTransfer(gc,"ds=2015-01-01"))
    print(partTransfer(gc,"ds=201501290000"))
    print(partTransfer(gc,"dt=201501290000"))
    print(partTransfer(gc,"pt=201501290000"))
    print(partTransfer(gc,"ds=\"20141001\""))


def testFunc(func):
    partList=[
"ds=201401","ds=2015","ds=2015-01","ds=2015-01-01","ds=201501011010","is_dirty=1","ds=2015",
    "ds=2015-01",
    "ds=2015-01-01",
    "ds=201501290000",
    "dt=201501290000",
    "pt=20150129000000",
    "ds=\"20141001\"",
    "ds='20141001',hr=1",
    "ds=20140101/hr=01",
    "ds=20140101,hr=01",
    "ds=hr/pt=1",
    "ds=hr,pt=1",
    "pt='20141027000000'",
    "pt=\"20141027000000\""
]
    partList=["ds=hr,pt=1"]
    gc=GlobalConf()
    if gc.load("config.py","test.log")!=0:
        print("can not load config file:%s" % (configFile))
        sys.exit(1)
    for part in partList:
        print("part:%s %s:%s" % (part,func,func(gc,part)))

if __name__ == '__main__':
    #main()
    #testFunc(replacePartday)
    testFunc(partTransferWrapper)
    #testgetNonPartedColList()
