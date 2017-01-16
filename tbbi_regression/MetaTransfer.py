#coding=utf-8
import ConfigParser,traceback
import sys,json
import logging
import commonUtility
import threading
import random
import string
from commonUtility import do_cmd,do_odps_cmd,do_odps_filecmd,getDayPart,issmaller,equal,LogWrapper,isparted,getExecutor,getSize,partTransfer,addpart,do_odps_cmd,do_remote_cmd,isparted
from Mysql import updateMysql,OdpsTableMeta,MysqlData,getUDF,tableIsExist,partIsExist,ifColExist,deletetable,getPartList

def usage():
    print("%s command[table|resource|column|part|updateUDF]" % (sys.argv[0]))
    sys.exit(1)

def getMeta(gc,tablename,projectname=""):
    if projectname == "":
        projectname=gc.defaultproject
    cmd='''curl %s.%s.%s ''' % (gc.metaURL,projectname,tablename)
    gc.log.info("cmd:%s" % (cmd)) 
    r=do_cmd(cmd)
    if r[0] !=0:
        gc.log.error("execute error!cmd:%s" % (cmd))
        return (1,'','')
    else:
        return r

def updateColMeta(gc,colinfo):
    tablename=colinfo["guid"].split(".")[2]
    projectname=colinfo["guid"].split(".")[1]
    tableid=OdpsTableMeta(gc).getTableId(tablename,projectname)
    gc.log.debug("Get tableid:%s tablename:%s" % (tableid,tablename))
    if tableid == -1 or tableid == "":
        gc.log.error("Get tableid error:tableid:%s tablename:%s" % (tableid,tablename))
        return 1
    if  colinfo["isPtColumn"] is not None and colinfo["isPtColumn"].lower() == "y":
        colinfo["isPtColumn"]=0
    else:
        colinfo["isPtColumn"]=1
    if colinfo["isForeignKey"] is not None and colinfo["isForeignKey"].lower() == "y":
        colinfo["isForeignKey"]=0
    else:
        colinfo["isForeignKey"]=1
    if colinfo["securityLevel"] is not None:
        colinfo["securityLevel"]=0
    else:
        colinfo["securityLevel"]=1
    
    if int(ifColExist(gc,tableid,colinfo["name"])) !=0 or colinfo["name"] is None: 
        gc.log.info("already exist col tableid:%s tablename:%s colname:%s" % (tableid,tablename,colinfo["name"]))
        return 1
    if  colinfo["type"] is None:
        colinfo["type"]="NULL" 
    cmd='''%s -N -e 'insert into `column` set `table`=%s,name="%s",type="%s",ispart=%s,isSecurity=%s,isFK=%s; ' ''' % (gc.mysql,tableid,colinfo["name"],colinfo["type"],colinfo["isPtColumn"],colinfo["securityLevel"],colinfo["isForeignKey"])
    gc.log.debug("cmd:%s" % (cmd))
    r=do_cmd(cmd)
    gc.log.debug("updateColMeta over,tableid:%s tablename:%s" % (tableid,tablename))
    if r[0] !=0 :
        gc.log.error("cmd error:%s" % (cmd))
        return 1
    else:
        return 0

def CreateColumn(gc,line):
    try:
        tablename=""
        projectname=""
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.debug("LineNum:%s\tLineLength:%d" % (line,LineLength))
        if LineLength == 1:
            tablename=line.split()[0]
            projectname=gc.defaultproject
        elif LineLength == 2: 
            tablename=line.split()[0]
            projectname=line.split()[1]
        gc.log.debug("tablename:%s projectname:%s" % (tablename,projectname))
        r=getMeta(gc,tablename,projectname)
        gc.log.debug("getMeta tablename:%s over" % (tablename))
        if r[0] == 0:
            colinfos=json.loads(r[1])
            for colinfo in colinfos:
                updateColMeta(gc,colinfo)
        else:
            gc.log.error("getMeta error tablename:%s" % (tablename))
    except Exception as e:
        gc.log.fatal("error table name:%s Exception: %s" % (tablename,traceback.format_exc()))

#如果是分区表，获取所有的分区信息，将第一个日趋分区为curday留下；
#如果没有curday，则获取到最近的一个日期分区；
def CreateCurrentPart(gc,line):
    gc.log.debug("line:%s CreateCurrentPart starts" % (line))
    try:
        upflag=0
        tablename=""
        projectname=""
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.debug("Line:%s LineLength:%d" % (line,LineLength))
        curday=gc.currentday
        if LineLength == 1:
            tablename=line.split()[0]
            projectname=gc.defaultproject
        elif LineLength == 2: 
            tablename=line.split()[0]
            projectname=line.split()[1]
        tablemeta=OdpsTableMeta(gc)
        tableid=tablemeta.getTableId(tablename,projectname)
        tablemeta.initTable(tableid)
        projectname=tablemeta.getProjectName()
        gc.log.debug("tableid:%s\tprojectname:%s" % (tableid,projectname))
        #非分区表不进行分区是否进行转换的判断
        if tablemeta.isPartedTable()!=0:
            gc.log.debug("tableid:%s tablename:%s not  parted" % (tableid,tablename))
            sql='''insert into partition values(NULL,%s,"",%d,NULL,NULL,NULL,NULL,NULL,NULL);''' % (tableid,upflag)
            updateMysql(gc,sql)
            gc.log.debug("line:%s CreateCurrentPart over" % (line))
            return 0
        #get parts from odps
        parts=getPartList(gc,gc.srcodps,tablename,projectname)
        if len(parts) == 0:
            gc.log.debug("tableid:%s tablename:%s not  parted" % (tableid,tablename))
            sql='''insert into partition values(NULL,%s,"",%d,NULL,NULL,NULL,NULL,NULL,NULL);''' % (tableid,upflag)
            updateMysql(gc,sql)
            gc.log.debug("line:%s CreateCurrentPart over" % (line))
            return 0
        replaceflag=0
        for part in parts:
            #对分区特征进行判断和提取
            #只有日表和小时表、其他表才进行处理，但是预处理过程中发现其实并不存在小时表，所以只处理2和0
            #2表示非日期表，全部获取
            #0为正常的分区日期
            r=getDayPart(gc,part)       
            if r[0]==2:
                gc.log.debug("tableid:%s tablename:%s not datetime formatted" % (tableid,tablename))
                sql='''insert into partition values(NULL,%s,"%s",%d,NULL,NULL,NULL,NULL,NULL,NULL);''' % (tableid,part,upflag)
                updateMysql(gc,sql)
                replaceflag=1
            elif r[0]==0:
                partday=r[1]
                gc.log.debug("curday:%s" % (curday))
                #含有业务日期的分区,replacefalg=0时候 不含有业务日期 的分区
                ret=equal(partday,curday)
                if ret==0:
                    replaceflag=1
                    sql='''insert into partition values(NULL,%s,"%s",%d,NULL,NULL,NULL,NULL,NULL,NULL);;''' % (tableid,part,upflag)
                    updateMysql(gc,sql)
                    replaceflag=1
                gc.log.debug("partday:%s curday:%s ret:%d" % (partday,curday,ret))
        if replaceflag==0:    
            partList=[]
            latestday=""
            for part in parts:
                gc.log.debug("len(partList):%d\tcurday:%s\tlatestday:%s\n" % (len(partList),curday,latestday))
                r=getDayPart(gc,part)       
                if r[0] != 0:
                    continue
                partday=r[1]
                if len(partList) == 0 or (partday==latestday  and partday!=None):
                    latestday=partday
                    partList.append(part)
                elif partday>latestday:
                    latestday=partday
                    partList=[]
                    partList.append(part)
                elif equal(latestday,partday)==0:
                    partList.append(part)
            for i in range(len(partList)):
                gc.log.info("latest part:%s" % (partList[i]))
                partid=partList[i]
                # need to transfer ,add 128
                sql='''insert into partition values(NULL,%s,"%s",%d,NULL,NULL,NULL,NULL,NULL,NULL);;''' % (tableid,part,upflag+128)
                updateMysql(gc,sql)
    except Exception as e:
        gc.log.fatal("error table name:%s Exception: %s" % (tablename,traceback.format_exc()))
    gc.log.debug("line:%s CreateCurrentPart over" % (line))
    return 0


def CreatePart(gc,line):
    try:
        tablename=""
        projectname=""
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.debug("LineNum:%s LineLength:%d" % (line,LineLength))
        if LineLength == 1:
            tablename=line.split()[0]
            projectname=gc.defaultproject
        elif LineLength == 2: 
            tablename=line.split()[0]
            projectname=line.split()[1]
        tablemeta=OdpsTableMeta(gc)
        tableid=tablemeta.getTableId(tablename,projectname)
        gc.log.debug("tableid:"+tableid)
        tablemeta.initTable(tableid)
        projectname=tablemeta.getProjectName()
        parts=getPartList(gc,gc.srcodps,tablename,projectname)
        if len(parts) == 0:
            gc.log.debug("tableid:%s tablename:%s not  parted" % (tableid,tablename))
            sql='''insert into partition values(NULL,%s,"",NULL,NULL,NULL,NULL,NULL,NULL,NULL);;''' % (tableid)
            updateMysql(gc,sql)
        for part in parts:
            #partExistSql='''select * from partition where `table`=%s and part="%s"; ''' % (tableid,part)
            #isPartExist=MysqlData(gc,partExistSql)
            #if isPartExist.RowNum() == 0:
            if partIsExist(gc,tableid,part) > 0:
                gc.log.debug("tablename:%s part:%s already exists" % (tablename,part))
                return
            sql='''insert into partition values(NULL,%s,"%s",NULL,NULL,NULL,NULL,NULL,NULL,NULL);;''' % (tableid,part)
            updateMysql(gc,sql)
    except Exception as e:
        gc.log.fatal("error table name:%s Exception: %s" % (tablename,traceback.format_exc()))

def getJar(gc,jarname,projectname=""):
    if projectname != "":
        cmd='''%s  -j %s -a %s ''' % (gc.ddl,jarname,projectname)
    else:
        cmd='''%s  -j %s ''' % (gc.ddl,jarname)
    gc.log.info("cmd:%s" % (cmd)) 
    r=do_cmd(cmd)
    if r[0] !=0:
        gc.log.error("execute error!cmd:%s" % (cmd))
        return (1,'','')
    else:
        return r

def updateJarMeta(gc,jarname,md5,addjarcmd,projectname):
    addjarcmd=addjarcmd.replace("\n\r","").replace("\n","")
    sql='''insert into resource values(NULL,"%s","%s","%s",NULL);''' % (jarname,md5,addjarcmd)
    return updateMysql(gc,sql)[0]

def deleteJar(gc,jarfile):
    cmd='''/bin/rm -rf %s '''  % (jarfile)
    r=do_cmd(cmd)

def genMd5(gc,jarfile):
    cmd='''md5sum %s ''' % (jarfile)
    r=do_cmd(cmd)
    if r[0] == 0:
        return r[1].split()[0]
    else:
        return ""

def CreateResource(gc,line):
    try:
        #projectname=gc.jarprojectname
        projectname="bi_udf"
        r=(1,'','')
        jarname=line
        r=getJar(gc,jarname,projectname)
        if r[0] == 0:
            addjarcmd=r[1]
            if addjarcmd.find("add") <0:
                gc.log.info("addjarcmd:%s is empty" % (line))
                return
            gc.log.info("addjarcmd:%s" % (addjarcmd))
            if projectname !="":
                addjarcmd="use %s;%s" % (projectname,addjarcmd)
            gc.log.info("addjarcmd[2]:%s" % (addjarcmd.split(";")[2]))
            jarfile=(addjarcmd.split(";")[2]).split()[2].replace(";","")
            gc.log.info("jarfile:%s" % (jarfile))
            #add jar
            #r=do_odps_cmd(gc,gc.dstodps,addjarcmd)
            r=do_odps_cmd(gc,gc.kodps,addjarcmd)
            if r[0]!=0:
                gc.log.error("addjarcmd error:%s" % (addjarcmd))
                gc.failList.append(jarname)
                return
            md5=genMd5(gc,jarfile)
            if md5 == "":
                gc.log.error("md5 generatoed error jarfile:%s" % (jarfile))
                gc.failList.append(jarname)
                return
            gc.log.info("md5:%s" % (md5))
            if updateJarMeta(gc,jarname,md5,addjarcmd,projectname) !=0:
                gc.log.error("update mysql error jarfile:%s" % (jarname))
                gc.failList.append(jarname)
                return
            deleteJar(gc,jarfile)
        else:
            gc.failList.append(jarname)
            gc.log.error("resource name:"+line)
    except Exception as e:
        gc.log.fatal("error resource  name:%s Exception: %s" % (line,traceback.format_exc()))

def getDdlSql(gc,tablename,projectname=""):
    if projectname == "":
        cmd='''%s  -t %s ''' % (gc.ddl,tablename)
    else:
        cmd='''%s -a %s -t %s ''' % (gc.ddl,projectname,tablename)
    gc.log.info("cmd:%s" % (cmd)) 
    r=do_cmd(cmd)
    if r[0] !=0:
        gc.log.error("execute error!cmd:%s" % (cmd))
        return (1,'','')
    return r
#    else:
#        if gc.lifecycle != ""  and  gc.lifecycle  is not None:
#            lcstr=" lifecycle %s ;" % (gc.lifecycle)
#            gc.log.debug("lcstr:%s" % (lcstr))
#            lcsql=r[1].rstrip(";")
#            lcsql=lcsql+lcstr
#            gc.log.debug("sql:%s" % (r[1]))
#        return (r[0],lcsql,r[2])

def save(gc,tablename,sql):
    open("%s/%s" % (gc.savedir,tablename),"w").write(sql)

def addlife(gc,oldhql):
    lcstr=" lifecycle 3650;"
    hql=""
    seminum=len(oldhql.split(";"))
    semiindex=0
    gc.log.info("semiindex:%d seminum:%d" % (semiindex,seminum))
    before=""
    beforebefore=""
    for i in oldhql:
        if i < chr(31) and i!= chr(9) and i != chr(10) and i!=chr(13):
            continue
        elif i == ";":
            semiindex=semiindex+1
            gc.log.debug("semiindex:%d seminum:%d" % (semiindex,seminum))
            if semiindex == seminum -1:
               hql =hql +lcstr
               continue
        #else:
            #if before==before and i!=' ' and i != beofre and (before == '\"' or beofre =="'" ):
        hql=hql+i
    hql=hql.rstrip('\n')
    return hql


def CreateOdpsTable(gc,line):
    try:
        lcstr=""
        if gc.lifecycle != ""  and  gc.lifecycle  is not None:
            lcstr=" lifecycle %s ;" % (gc.lifecycle)
        tablename=""
        srcprojectname=""
        dstprojectname=""
        partList=[]
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.info("line:%s LineLength:%d CreateTable begins" % (line,LineLength))
        if LineLength != 3:
            gc.log.error("line:%s LineLength:%d not enough argumentor." % (line,LineLength))
            return 1
        else: 
            tablename=line.split()[0]
            srcprojectname=line.split()[1]
            dstprojectname=line.split()[2]
        r=getDdlSql(gc,tablename,srcprojectname)
        if r[0] != 0:
            gc.log.error("tablename:%s\tsrcprojectname:%s\tgetDdlSql error." % (tablename,srcprojectname))
            return 1
        elif r[0] == 0:
            oldhql=r[1]
            hql=addlife(gc,oldhql)
            if len(hql)==0 or hql == "" or hql is None:
                gc.log.info("table:%s not exists,exit" % (line))
                return 1
            gc.log.info("ddl hql:%s" % (hql))
            actualHql="use %s;%s" % (dstprojectname,hql)
            r1=do_odps_filecmd(gc,gc.kodps,actualHql)
            if r1[0] == 0:
                gc.log.info("srcprojectname:%s table:%s created on dstprojectname:%s" % (srcprojectname,tablename,dstprojectname))
            else:
                gc.log.info("line:%s create errror." % (line))
            partedStatus=isparted(gc,gc.srcodps,srcprojectname,tablename)
            if partedStatus != 0:
                parts=getPartList(gc,gc.srcodps,tablename,srcprojectname)
                if len(parts) == 0:
                    gc.log.info("parted tablename:%s.%s has no partitions." % (srcprojectname,tablename))
                    return 0
                replaceflag=0
                for part in parts:
                    #对分区特征进行判断和提取
                    #只有日表和小时表、其他表才进行处理，但是预处理过程中发现其实并不存在小时表，所以只处理2和0
                    #2表示非日期表，全部获取
                    #0为正常的分区日期
                    r=getDayPart(gc,part)       
                    if r[0]==2:
                        gc.log.debug("tableid:%s tablename:%s not datetime formatted" % (tableid,tablename))
                        replaceflag=2
                        partList.append(part)
                    elif r[0]==0:
                        partday=r[1]
                        gc.log.debug("curday:%s" % (gc.currentday))
                        ret=equal(partday,gc.currentday)
                        if ret==0:
                            replaceflag=1
                            partList.append(part)
                        gc.log.debug("partday:%s curday:%s ret:%d" % (partday,gc.currentday,ret))
                #处理日期分区，但是没有今天的情况 
                if replaceflag==0:    
                    latestday=""
                    for part in parts:
                        gc.log.debug("len(partList):%d\tcurday:%s\tlatestday:%s\n" % (len(partList),gc.currentday,latestday))
                        r=getDayPart(gc,part)       
                        if r[0] != 0:
                            continue
                        partday=r[1]
                        if len(partList) == 0 or (partday==latestday  and partday!=None):
                            latestday=partday
                            partList.append(part)
                        elif partday>latestday:
                            latestday=partday
                            partList=[]
                            partList.append(part)
                        elif equal(latestday,partday)==0:
                            partList.append(part)
                for i in range(len(partList)):
                    part = partList[i]
                    gc.log.info("tablename:%s part:%s" % (tablename,part))
                    part=part.replace("/",",").replace(",","\",").replace("=","=\"")+"\""
                    part=part.replace("\"\"","\"")
                    gc.log.debug("tablename:%s  part:%s" % (tablename,part))
                    addpart(tablename,part,gc,dstprojectname,gc.dstodps)
    except Exception as e:
        gc.log.error("error tablename:%s Exception: %s" % (line,traceback.format_exc()))
    gc.log.info("line:%s LineLength:%d CreateTable over" % (line,LineLength))
    return 0

def CreateHiveTable(gc,line):
    try:
        tablename=""
        projectname=""
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.info("line:%s LineLength:%d CreateHiveTable begins" % (line,LineLength))
        if LineLength == 1:
            gc.log.fatal("line:%s LineLength:%d " % (line,LineLength))
            tablename=line.split()[0]
        elif LineLength == 2: 
            tablename=line.split()[0]
            projectname=line.split()[1]
        r=getDdlSql(gc,tablename,projectname)
        if r[0] != 0:
            gc.log.error("tablename:%s\tprojectname:%s" % (tablename,projectname))
            return 1
        elif r[0] == 0:
            hql=r[1]
            gc.log.info("ddl hql:%s" % (hql))
            #if not (gc.savedir == "" or gc.savedir is None):
            actualHql="use %s;%s" % (gc.outproject,hql)
            r1=do_odps_filecmd(gc,gc.dstodps,actualHql)
            hql=hql.replace("`","\`").replace("'","\'").replace("\"","\\\"")
            save(gc,tablename,hql)
            if r1[0] == 0 and r2[0] == 0:
                gc.log.info("table:%s create sucess both 5k and online" % (line))
                ispart=1
                if hql.find("partitioned") >=0:
                    ispart=0
                sql='''insert into `table`(id,name,jobid,`schema`,info,ispart,gmt_modified,projectname) values(NULL,"%s",NULL,"%s","metaOn5k",%d,NULL,"%s");''' % (tablename,hql,ispart,projectname)
                r=updateMysql(gc,sql)
                if r[0] != 0:
                    sql='''insert into `table`(id,name,jobid,`schema`,info,ispart,gmt_modified,projectname) values(NULL,"%s",NULL,"%s","metaOn5k",%d,NULL,"%s");''' % (tablename,"",ispart,projectname)
                    r=updateMysql(gc,sql)
            else:
                gc.log.info("table:%s create errror on 5k or online" % (line))
                    
            #else:
            #    save(gc.tableList[i],r[1])
    except Exception as e:
        gc.log.error("error tablename:%s Exception: %s" % (line,traceback.format_exc()))
    gc.log.info("line:%s LineLength:%d CreateTable over" % (line,LineLength))
    return 0

def do_hive_cmd(gc,hive,hiveHql):
    pass

def CreateTable(gc,line):
    try:
        lcstr=""
        if gc.lifecycle != ""  and  gc.lifecycle  is not None:
            lcstr=" lifecycle %s ;" % (gc.lifecycle)
        tablename=""
        projectname=""
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.info("line:%s LineLength:%d CreateTable begins" % (line,LineLength))
        if LineLength == 1:
            tablename=line.split()[0]
            projectname=gc.defaultproject
        elif LineLength == 2: 
            tablename=line.split()[0]
            projectname=line.split()[1]
        if gc.ismysql != "no":
            if tableIsExist(gc,tablename,projectname) > 0:
                return 2
            deletetable(gc,tablename,"")
        r=getDdlSql(gc,tablename,projectname)
        if r[0] != 0:
            gc.log.error("tablename:%s\tprojectname:%s" % (tablename,projectname))
            return 1
        elif r[0] == 0:
            oldhql=r[1]
            hql=addlife(gc,oldhql)
            if len(hql)==0 or hql == "" or hql is None:
                gc.log.info("table:%s not exists,exit" % (line))
                return 1
            gc.log.info("ddl hql:%s" % (hql))
            #if not (gc.savedir == "" or gc.savedir is None):
            actualHql="use %s;%s" % (gc.outproject,hql)
            r1=do_odps_filecmd(gc,gc.dstodps,actualHql)
            kactualHql="use %s;%s" % (projectname,hql)
            r2=do_odps_filecmd(gc,gc.kodps,kactualHql)
            hql=hql.replace("`","\`").replace("'","\'").replace("\"","\\\"")
            save(gc,tablename,hql)
            if r1[0] == 0 and r2[0] == 0:
                gc.log.info("table:%s create sucess both 5k and online" % (line))
                ispart=1
                if hql.find("partitioned") >=0:
                    ispart=0
                sql='''insert into `table`(id,name,jobid,`schema`,info,ispart,gmt_modified,projectname) values(NULL,"%s",NULL,"%s","metaOn5k",%d,NULL,"%s");''' % (tablename,hql,ispart,projectname)
                r=updateMysql(gc,sql)
                if r[0] != 0:
                    sql='''insert into `table`(id,name,jobid,`schema`,info,ispart,gmt_modified,projectname) values(NULL,"%s",NULL,"%s","metaOn5k",%d,NULL,"%s");''' % (tablename,"",ispart,projectname)
                    r=updateMysql(gc,sql)
            else:
                gc.log.info("table:%s create errror on 5k or online" % (line))
                    
            #else:
            #    save(gc.tableList[i],r[1])
    except Exception as e:
        gc.log.fatal("error tablename:%s Exception: %s" % (line,traceback.format_exc()))
    gc.log.info("line:%s LineLength:%d CreateTable over" % (line,LineLength))
    return 0

def updateUDF(gc,colinfo):
    tablename=OdpsTableMeta(gc).getTableName(colinfo['table'])
    gc.log.debug("tablename:%s" % (tablename))
    udf=getUDF("tbbi_isolation",tablename,colinfo['name'],colinfo['type'],"")
    gc.log.debug("udf:%s" % (udf))
    updateSql='''update `column` set desen="%s"  where id=%s''' % (udf,colinfo['id']) 
    gc.log.info("updateUDFSql:%s" % (updateSql))
    updateMysql(gc,updateSql)
    gc.log.info("tablename:%s done desen in mysql" % (tablename))

