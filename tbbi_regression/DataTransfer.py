# -*- coding: utf-8 -*-
import ConfigParser,sys,os, logging, commonUtility, threading, random, string,time,traceback
from commonUtility import do_cmd,do_odps_filecmd,getExecutor,getSize,partTransfer,addpart,do_odps_cmd,do_remote_cmd,isparted
from Mysql import MysqlData,OdpsTableMeta,updateMysql,getDragList,tableIsExist,mysqltableDelete,deletetable,getPartList,execMysql
from TransferUtility import GlobalConf,Usage
import MetaTransfer
import DesenTransfer
import DataTransfer
import os

#到partition库中寻找flag=1的字段，然后：
#（1） 组合为add partition 的SQL到5k执行；
#（2） 组合copytask任务，运行导出其中的数据；
# (3) todo: 给出一个executorList，通过ssh -f 方式到多个机器上面执行
# 只管拖数据，删除分区和标记size等动作都交给独立程序进行
def dragwrapper(gc):
    dragList=getDragList()
    for tpinfo in dragList:
        datacopy(tpinfo,gc)

def modend(gc):
    endtime=time.strftime("%Y%m%d%H%M%S")
    cmd=""
    r=do_cmd(cmd)
    gc.log.info("cmd:%s r1:%s r2:%s" % (cmd,r[1],[2]))


def insert(gc,status="warning"):
    totalNum=len(gc.opList)
    jenkins=''
    if os.getenv("BUILD_URL") is not None:
        jenkins=os.getenv("BUILD_URL")+"/console"
    jobstatus=2
    cmd=""
    r=do_cmd(cmd)
    gc.log.info("cmd:%s r1:%s r2:%s" % (cmd,r[1],[2]))

def datacopy(gc,line):
    tablename=line.split()[0]
    projectname=line.split()[1]
    mysqltableDelete(gc,tablename,projectname)
    if tableIsExist(gc,tablename,projectname) != 0:
        gc.log.info("table %s already processed,exit." % (tablename))
        return 1
    gc.log.info("---- start CreateTable ----")
    if MetaTransfer.CreateTable(gc,line) != 0:
        gc.log.info("table :%s created failed." % (line))
        return 1
    gc.log.info("---- finish CreateTable ----")
    gc.log.info("---- start CreateColumn ----")
    MetaTransfer.CreateColumn(gc,line)
    gc.log.info("---- finish CreateColumn ----")
    gc.log.info("---- start CreateCurrentPart ----")
    MetaTransfer.CreateCurrentPart(gc,line)
    gc.log.info("---- finish CreateCurrentPart ----")
    funcnameList=["desen","drag"]
    i=0
    for funcname in funcnameList:
        gc.log.info("---- start function %s ----" % (funcname))
        for tpinfo in tpinfoList:
            if ret !=0:
                gc.log.fatal("table:%s funcname:%s not processed well." % (line,funcname))
                return 1
        gc.log.info("---- finish  function %s ----" % (funcname))
        i=i+1
    return 0


def getNonFields(gc,projectname,tablename):
    cmd='''%s -a %s -t %s ''' % (gc.getfield,projectname,tablename)
    gc.log.info("cmd:%s" % (cmd))
    r=do_remote_cmd(gc,cmd)
    if r[0] == 0:
        return r[1]
    else:
        return ""

def CopyToResult(gc,line):
    LineLength=len(line.split())
    srcprojectname=""
    dstprojectname=""
    gc.log.info("line:%s LineLength:%d CopyToResult begins" % (line,LineLength))
    if LineLength == 3: 
        tablename=line.split()[0]
        srcprojectname=line.split()[1]
        dstprojectname=line.split()[2]
    else:
        gc.log.fatal("line:%s not has project." % (line))
        return 2
    gc.log.debug("start CopyToResult srcprojectname:%s dstprojectname:%s tablename:%s" % (srcprojectname,dstprojectname,tablename))
    r={}
    hql='''use %s;create table if not exists %s like %s.%s;''' % (dstprojectname,tablename,srcprojectname,tablename)
    r=do_odps_filecmd(gc,gc.kodps,hql)
    if r[0] != 0:
        gc.log.info("hql:%s executed error!" % (hql))
    partedStatus=isparted(gc,gc.kodps,srcprojectname,tablename)
    if partedStatus == 0:
        gc.log.info("tablename:%s.%s is not parted." % (srcprojectname,tablename))
        try:
            allCols=getNonFields(gc,srcprojectname,tablename)
            hql='''use %s; insert overwrite table %s select %s from %s.%s '''  %  (dstprojectname,tablename,allCols,srcprojectname,tablename)
            gc.log.info("hsql:%s" % (hql))
            r=do_odps_filecmd(gc,gc.kodps,hql)
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s' tablename:%s)]" % (traceback.format_exc(),tablename))
            r[0]=1
            r[2]=format(str(e))
        return 0
    parts=getPartList(gc,gc.kodps,tablename,srcprojectname)
    gc.log.info("tablename:%s parts:%s" % (tablename,parts))
    #分区表中的分区却为空，则为空表，不进行拷贝
    if len(parts) == 0:
        gc.log.info("parted tablename:%s.%s has no partitions." % (srcprojectname,tablename))
        return 0
    gc.log.info("parted table tablename:%s.%s" % (srcprojectname,tablename))
    cols=getNonFields(gc,srcprojectname,tablename)
    if cols == "":
        gc.log.info("cols is empty in %s.%s" % (srcprojectname,tablename))
        return 3
    gc.log.debug("cols:%s" % (cols))
    for part in parts:
        gc.log.info("tablename:%s part:%s" % (tablename,part))
        try:
            part=part.replace("/",",").replace(",","\",").replace("=","=\"")+"\""
            part=part.replace("\"\"","\"")
            gc.log.debug("tablename:%s  part:%s" % (tablename,part))
            partswhere=" and ".join([singlepart for singlepart in part.split(",")])
            partswhere=partswhere.replace("\"\"","\"")
            gc.log.debug("tablename:%s partwhere:%s" % (tablename,partswhere))
            addpart(tablename,part,gc,dstprojectname)
            hql='''use %s; insert overwrite table %s partition(%s) select %s from %s.%s where %s;'''  %  (dstprojectname,tablename,part,cols,srcprojectname,tablename,partswhere)
            gc.log.info("hsql:%s" % (hql))
            outfp=open(gc.result+os.sep+tablename+"_"+part.replace(" ","_").replace("\"","").replace("'","")+"_stdout","w")
            errfp=open(gc.result+os.sep+tablename+"_"+part.replace(" ","_").replace("\"","").replace("'","")+"_stderr","w")
            r=do_odps_filecmd(gc,gc.kodps,hql,outfp,errfp)
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s' tablename:%s)]" % (traceback.format_exc(),tablename))
    gc.log.info("finish CopyToResult tablename:%s" % (tablename))
    return 0

#可以复制任意的表，如果设置为result这个project，那么就可以复制结果表
def CreateResultTable(gc,line):
    try:
        tablename=""
        projectname=""
        r=(1,'','')
        LineLength=len(line.split())
        gc.log.info("line:%s LineLength:%d CreateTable begins" % (line,LineLength))
        if LineLength == 3: 
            tablename=line.split()[0]
            srcprojectname=line.split()[1]
            dstprojectname=line.split()[2]
        else:
            gc.log.fatal("line:%s not has project." % (line))
            return 2
        Hql="use %s;create table %s like %s.%s" % (dstprojectname,tablename,srcprojectname,tablename)
        r=do_odps_filecmd(gc,gc.kodps,Hql)
        if r[0] == 0:
            gc.log.info("dstprojectname:%s tablename:%s create sucess on 5k" % (dstprojectname,tablename))
        elif r[0] != 0:
            gc.log.error("line:%s create result error!" % (line))
    except Exception as e:
        gc.log.fatal("line:%s create result error!Exception: %s" % (line,traceback.format_exc()))
    gc.log.info("line:%s LineLength:%d CreateTable over" % (line,LineLength))
    return 0

def drag(gc,tpinfo):
    resultDict={0:'3',1:'4'} #32+3 , 32+4
    preflag='1'
    partid=tpinfo['id']
    flag=tpinfo['flag']
    tableid=tpinfo['table']
    partitionspec=tpinfo['part']
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    projectname=tablemeta.getProjectName()
    gc.log.debug("start tablename:%s" % (tablename))
    r={}
    if int(flag) & int(preflag) != int(preflag):
        gc.log.info("flag:%s preflag:%s  not included" % (flag,preflag))
        return 1
    if tablemeta.isPartedTable()!=0:
        stime=int(time.time())
        try:
            copySql='''use %s;copy -o -d import -url %s/%s/%s -t %s -n200; ''' % (projectname,gc.onlineTunnel,gc.outproject,tablename,tablename)
            gc.log.debug("begin copySql:%s" % (copySql))
            r=do_odps_filecmd(gc,gc.kodps,copySql,"")
            gc.log.info("finish copySql:%s r:%s" % (copySql,r))
            partname=""
            deletetable(gc,tablename,partname)
        except Exception as e:
            r[0]=1
            r[2]=format(str(e))
            gc.log.fatal("['error', ('Exception:%s r0:%s r2:%s')]" % (traceback.format_exc(),r[0],r[2]))
        finally:
            etime=int(time.time())
            cost=etime-stime
            gc.log.debug("updateMetaInfo,r:%s,cost:%d" % (r,cost))
            updateMetaInfo(r,partid,gc,resultDict,dragcost=str(cost))
    else:
        gc.log.info("parted table tablename:%s" % (tablename))
        partname=partitionspec.replace("/",",")
        gc.log.debug("partname:%s,partitionspec:%s" % (partname,partitionspec))
        gc.log.debug("partid:%s" % (partid))
        stime=int(time.time())
        try:
            gc.log.debug("partname:%s" % (partname))
            newpartname=" , ".join([partTransfer(gc,singlepart) for singlepart in partname.split(",")])
            addpart(tablename,newpartname,gc,projectname)
            newpartname=newpartname.replace(" ","")
            copySql='''use %s;copy -o -d import -url %s/%s/%s/%s -t %s -p %s -n 200;''' % (projectname,gc.onlineTunnel,gc.outproject,tablename,newpartname,tablename,newpartname)
            gc.log.debug("begin copySql:%s" % (copySql))
            r=do_odps_filecmd(gc,gc.kodps,copySql,"")
            gc.log.info("finish copySql:%s  r:%s" % (copySql,r))
            deletetable(gc,tablename,partname)
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (traceback.format_exc()))
            r[0]=1
            r[2]=format(str(e))
        finally:
            etime=int(time.time())
            cost=etime-stime
            updateMetaInfo(r,partid,gc,resultDict,dragcost=str(cost))
    gc.log.info("finish darg tablename:%s" % (tablename))
    return 0

def verify(gc,tpinfo):
    resultDict={0:'5',1:'6'} 
    preflag=str(int(resultDict[0])-2)
    partid=tpinfo['id']
    flag=tpinfo['flag']
    tableid=tpinfo['table']
    partitionspec=tpinfo['part']
    partname=partitionspec.replace("/",",")
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    projectname=tablemeta.getProjectName()
    gc.log.debug("start verify tablename:%s projectname:%s" % (tablename,projectname))
    r={0:1,1:"",2:""}
    if tablemeta.isPartedTable()!=0:
        if flag!=preflag:
            return 
        try:
            gc.log.debug("parted table tablename:%s\tpartid:%s\tflag:%s\ttableid:%s\tpartitionspec:%s" % (tablename,partid,flag,tableid,partitionspec))
            outodps=Odps(gc.dstodps,projectname,tablename,"",gc)
            kodps=Odps(gc.kodps,gc.projectname,tablename,"",gc)
            outcount=outodps.count()
            kcount=kodps.count()
            if outcount == kcount:
                ret=0
            else:
                ret=1
            gc.log.debug("ret:%d tablename:%s outcount:%s kcount:%s" % (ret,tablename,str(outcount),str(kcount)))
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (format(str(e))))
            r[2]=format(str(e))
        finally:
            r[0]=ret
        updateMetaInfo(r,partid,gc,resultDict,kcount=kcount,outcount=outcount)
    else:
        gc.log.debug("parted table tablename:%s\tpartid:%s\tflag:%s\ttableid:%s\tpartitionspec:%s" % (tablename,partid,flag,tableid,partitionspec))
        try:
            gc.log.debug("partname:%s" % (partname))
            outodps=Odps(gc.dstodps,projectname,tablename,partname,gc)
            kodps=Odps(gc.kodps,projectname,tablename,partname,gc)
            outcount=outodps.count()
            kcount=kodps.count()
            if outcount == kcount:
                ret=0
            else:
                ret=1
            gc.log.debug("ret:%d tablename:%s partname:%s outcount:%s kcount:%s" % (ret,tablename,partname,str(outcount),str(kcount)))
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (format(str(e))))
            r[2]=format(str(e))
        finally:
            r[0]=ret
        updateMetaInfo(r,partid,gc,resultDict,kcount=kcount,outcount=outcount)
    gc.log.info("finish verify tablename:%s" % (tablename))

def delete(gc,tpinfo):
    resultDict={0:'7',1:'8'} 
    preflag=str(int(resultDict[0])-2)
    #preflag='1'
    tableid=tpinfo['table']
    partid=tpinfo['id']
    flag=tpinfo['flag']
    tableid=tpinfo['table']
    partitionspec=tpinfo['part']
    partname=partitionspec.replace("/",",")
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    gc.log.debug("start delete tablename:%s tableid:%s" % (tablename,tableid))
    r={}
    if tablemeta.isPartedTable()!=0:
        (partid,flag)=tablemeta.getPartId("")
        try:
            truncSql='''use %s;drop table %s; ''' % (gc.outproject,tablename)
            gc.log.info("truncSql:%s " % (truncSql))
            r=do_odps_filecmd(gc,gc.dstodps,truncSql,"")
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (format(str(e))))
            r[0]=1
            r[2]=format(str(e))
        finally:
            updateMetaInfo(r,partid,gc,resultDict)
    else:
        gc.log.info("parted table tablename:%s" % (tablename))
        try:
            gc.log.debug("parted table tablename:%s\tpartid:%s\tflag:%s\ttableid:%s\tpartitionspec:%s" % (tablename,partid,flag,tableid,partitionspec))
            partname=partname.replace("=","=\"").replace(",","\",\"")+"\""
            droppartSql='''use %s;alter table %s drop  IF EXISTS partition(%s);''' % (gc.outproject,tablename,partname)
            gc.log.debug("droppartSql:%s" % (droppartSql))
            r=do_odps_filecmd(gc,gc.dstodps,droppartSql,"")
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (format(str(e))))
            r[0]=1
            r[2]=format(str(e))
        finally:
            gc.log.debug("r:%s partid:%s resultDict:%s" % (r,partid,resultDict))
            updateMetaInfo(r,partid,gc,resultDict)
    gc.log.info("finish delete tablename:%s" % (tablename))


def upJobInfo(gc,filename):
    type="jobruntime"
    jobtype="fuxi"
    if not os.path.exists(filename):
        return jobtype
    instanceid=""
    cost=0.0
    jobcost=0.0
    nodeid=filename.split(os.sep)[-1].split("_stderr")[0]
    for line in open(filename,"r"):
        line=line.strip("\n")
        if line =="": continue
        elif line.startswith("Job run time") and line.find(":")>0:
            cost=float(line.split(":")[1].strip())
            gc.log.debug("filename:%s cost:%f" % (filename,cost))
            jobcost=jobcost+cost
        elif line.startswith("ID") and line.find("=")> 0:
            instanceid=instanceid+" "+line.split("=")[1].strip()
            gc.log.debug("filename:%s instanceid:%s" % (filename,instanceid))
        elif line.find("Job run mode")>=0 and line.find("service job")>=0:
            jobtype="smode"
    if(instanceid is None or instanceid == ""):
        print("instanceid:%s not found in filename:%s" % (instanceid,filename))
        return jobtype
    sql='''select count(1) from e2e.regression_sqlinfo where dirname='%s' and nodeid='%s';''' % (gc.timestamp,nodeid)
    cnt=execMysql(gc,sql)
    if cnt == 0:
        sql='''insert into e2e.regression_sqlinfo(dirname,nodeid,instanceid,tablename) values('%s','%s','%s','');''' % (gc.timestamp,nodeid,instanceid)
        execMysql(gc,sql)
    elif cnt == 1:
        sql='''update e2e.regression_sqlinfo set instanceid='%s' where dirname='%s' and nodeid='%s';''' % (instanceid,gc.timestamp,nodeid)
        execMysql(gc,sql)
    sql='''select count(1) from e2e.regression_costinfo where dirname='%s' and nodeid='%s' and type='%s'; ''' % (gc.timestamp,nodeid,type)
    if execMysql(gc,sql) >0:
        return jobtype
    sql='''insert into e2e.regression_costinfo(dirname,cost,nodeid,type) values('%s','%s','%s','%s'); ''' % (gc.timestamp,str(jobcost),nodeid,type)
    cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
    gc.log.info(cmd)
    r=do_cmd(cmd)
    if(r[0]!=0):
        gc.log.fatal("insert into table e2e.regression_costinfo error!cmd:%s\nstdout:%s\nstderr:%s\n" % (cmd,r[1],r[2]))
    return jobtype

def upTotalInfo(gc,filename,jobtype="fuxi"):
    type='full'
    nodeid=filename.split(os.sep)[-1].split("_info")[0]
    if not os.path.exists(filename):
        cost=0.0
        sql='''insert into e2e.regression_costinfo(dirname,cost,nodeid,type,jobtype) values('%s','%s','%s','%s','%s'); ''' % (gc.timestamp,cost,nodeid,type,jobtype)
        print(sql)
        cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
        gc.log.info(cmd)
        r=do_cmd(cmd)
        if(r[0]!=0):
            gc.log.fatal("insert into table e2e.regression_costinfo error!cmd:%s\nstdout:%s\nstderr:%s\n" % (cmd,r[1],r[2]))
        return (nodeid,cost)
    cost=open(filename,"r").readlines()[0]
    cost=cost.split(":")[1]
    gc.log.info("filename:%s cost:%s" % (filename,cost))
    sql='''select count(1) from e2e.regression_sqlinfo where dirname='%s' and nodeid='%s';''' % (gc.timestamp,nodeid)
    if execMysql(gc,sql) ==0:
        sql='''insert into e2e.regression_sqlinfo(dirname,nodeid,instanceid,tablename) values('%s','%s','','');''' % (gc.timestamp,nodeid)
        execMysql(gc,sql)
    sql='''select count(1) from e2e.regression_costinfo where dirname='%s' and nodeid='%s' and type='%s'; ''' % (gc.timestamp,nodeid,type)
    if execMysql(gc,sql) >0: return (nodeid,0.0)
    sql='''insert into e2e.regression_costinfo(dirname,cost,nodeid,type,jobtype) values('%s','%s','%s','%s','%s'); ''' % (gc.timestamp,cost,nodeid,type,jobtype)
    cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
    gc.log.info(cmd)
    r=do_cmd(cmd)
    if(r[0]!=0):
        gc.log.fatal("insert into table e2e.regression_costinfo error!cmd:%s\nstdout:%s\nstderr:%s\n" % (cmd,r[1],r[2]))
    return (nodeid,cost)


def render(gc,sqlfile):
    tablenames=""
    renderPath=gc.work_dir+os.sep+"logs"+os.sep+"render"+os.sep+gc.timestamp+os.sep
    if sqlfile.find("dst_table") >=0 : return
    if not os.path.exists(renderPath):
        try:
            os.makedirs(renderPath)
        except OSError:
            gc.log.debug("renderPath:%s already exists." % (renderPath))
        gc.log.debug("create renderPath:%s" % (renderPath))
    hql="".join([line for line in open(sqlfile,"r")])
    if gc.sessionflag is not None and gc.sessionflag !="" and  gc.sessionflag !="''" and  gc.sessionflag !="\"\"":
        hiveconf=gc.sessionflag
        hql="use %s;\n%s\n%s" % (gc.baseproject,hiveconf,hql)
    else:
        hql="use %s;\n%s" % (gc.baseproject,hql)
    stime=time.time()
    r=(1,"","")
    nodeid=sqlfile.split('/')[-1]
    fullnodeid=sqlfile.split('/')[-1]
    gc.log.debug("render nodefile:%s" % (renderPath+os.sep+fullnodeid))
    outfp=open(renderPath+os.sep+fullnodeid+"_stdout","w")
    errfp=open(renderPath+os.sep+fullnodeid+"_stderr","w")
    r=do_odps_filecmd(gc,gc.kodps,hql,outfp,errfp)
    etime=time.time()
    durtime=etime-stime
    gc.log.info("sqlfile:%s durtime:%s" % (sqlfile,str(durtime)))
    open(renderPath+os.sep+fullnodeid+"_info","w").write("durtime:"+str(durtime))
    jobtype=upJobInfo(gc,renderPath+os.sep+fullnodeid+"_stderr")
    upTotalInfo(gc,renderPath+os.sep+fullnodeid+"_info",jobtype)
    outfp.close()
    errfp.close()
    if nodeid in gc.node2resDict:
        tablenames=gc.node2resDict[nodeid]
    else:
        gc.log.info("nodeid:%s not found in gc.res2nodeDict" % (nodeid))
        return 
    gc.log.info("issucc:%d nodeid:%s tablenames:%s gc.verify:%s gc.isverify:%s" % (r[0],nodeid,tablenames,gc.verify,gc.isverify))
    if r[0] != 0:
        gc.log.info("sql exec return val not 0.skip verify,tables:%s" % (tablenames))
    if gc.verify.lower() == "yes" and gc.isverify.lower() == "yes":
        for tablename in tablenames.split(","):
            if tablename not in gc.verifyplanList:
                gc.log.info("table:%s not in verifyplanlist." % (tablename))
                continue
            gc.log.info("table:%s in verifyplanlist,begins to verify." % (tablename))
            doverify(gc,tablename=tablename,projectname=gc.resultproject)
            gc.log.info("table:%s in verifyplanlist,verify over." % (tablename))
    

def doverify(gc,**argdict):
    verifyPath=gc.work_dir+os.sep+"logs"+os.sep+"verify"+os.sep+gc.timestamp+os.sep
    gc.log.info(" verifyPath:%s" % (verifyPath))  
    if not os.path.exists(verifyPath):
        os.makedirs(verifyPath)
        gc.log.info("create verifyPath:%s" % (verifyPath))
    tablename=argdict['tablename']
    projectname=argdict['projectname']
    gc.log.debug("tablename:%s projectname:%s"  % (tablename,projectname))
    sql=""
    r=do_odps_filecmd(gc,gc.kodps,sql)
    rfilename=verifyPath+os.sep+tablename
    gc.log.debug("rfilename:%s" % (rfilename))
    rfile=open(rfilename,"w")
    rfile.write(r[2])
    rfile.close()

def verifyResult(gc,line):
    if len(line.split()) == 3:
        tablename=line.split()[0]
        projectname=line.split()[2]
    elif len(line.split())>0:
        tablename=line.split()[0]
        projectname=gc.resultproject
    gc.log.info("start verifyResult tablename:%s projectname:%s"  % (tablename,projectname))
    doverify(gc,tablename=tablename,projectname=projectname)
    gc.log.info("finish verifyResult tablename:%s projectname:%s"  % (tablename,projectname))

def tagsize(gc,tpinfo):
    partid=tpinfo['id']
    flag=tpinfo['flag']
    tableid=tpinfo['table']
    partitionspec=tpinfo['part']
    tablemeta=OdpsTableMeta(gc)
    tablename=tablemeta.getTableName(tableid)
    r={}
    gc.log.debug("start tagsize tablename:%s partid:%s" % (tablename,partid))
    if tablemeta.isPartedTable()!=0:
        try:
            partname=""
            gc.log.debug("%s has no part" % (tablename))
            size=getSize(gc,tablename,partname)
            gc.log.info("tablename:%s partname:%s size:%s " % (tablename,partname,str(size)))
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (format(str(e))))
            r[0]=1
            r[2]=format(str(e))
        finally:
            gc.log.debug("r:%s partid:%s" % (r,partid))
            updateMetaInfo(r,partid,gc,"",size=size)
    else:
        partname=partitionspec.replace("/",",")
        gc.log.debug("partname:%s partitionspec:%s" % (partname,partitionspec))
        try:
            gc.log.info("tablename:%s partname:%s" % (tablename,partname))
            size=getSize(gc,tablename,partname)
            gc.log.info("tablename:%s partname:%s size:%s " % (tablename,partname,str(size)))
        except Exception as e:
            gc.log.fatal("['error', ('Exception: %s')]" % (format(str(e))))
            r[0]=1
            r[2]=format(str(e))
        finally:
            gc.log.debug("r:%s partid:%s" % (r,partid))
            updateMetaInfo(r,partid,gc,"",size=size)
    gc.log.info("finish tagsize tablename:%s" % (tablename))


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
            setclause=" flag="+partinfo['flag']
        else:
            setclause=setclause+",flag="+partinfo['flag']
 
    gc.log.debug("setclause:%s" % (setclause))
    updatePartSql="update partition set %s where id=%s;"  % (setclause,partid)
    gc.log.debug("updatePartSql:%s" % (updatePartSql))
    updateMysql(gc,updatePartSql,"-N")

class Odps:
    def __init__(self,odps,project,tablename,partname,gc):
        self.odps=odps
        self.project=project
        self.tablename=tablename
        self.partname=partname
        self.gc=gc
    def count(self):
        if self.partname != None and self.partname !="":
            hql='''count %s.%s partition(%s)''' % (self.project,self.tablename,self.partname)
            r=do_odps_filecmd(self.gc,self.odps,hql)
            if r[0]==0:
               return r[1].replace("\n","").replace("\r\n","").strip()
            else:
               return ""
            
        hql='''count %s.%s ''' % (self.project,self.tablename)
        r=do_odps_filecmd(self.gc,self.odps,hql)
        if r[0]==0:
           return r[1].replace("\n","").replace("\r\n","").strip()
        else:
           return ""

def main():
    resultDict={0:'5',1:'6'} 
    gc=GlobalConf()
    if gc.load("./config.py","test.log")!=0:
        print("can not load config file:%s" % (configFile))
        sys.exit(1) 
    for line in open("./test.txt","r"):
        line=line.strip("\n")
        datacopy(gc,line)
        
    sys.exit(1) 
    tablename="ds_et_sel_platform_product_fdt0" 
    size=1
    partname=""
    partid=2497271
    r={0:0,1:"",2:""}
    kcount=1
    outcount=1
    updateMetaInfo(r,partid,gc,resultDict,kcount=kcount,outcount=outcount)

def parseLog(gc):
    needverifyfp=open("./needverifyfile.txt","w")
    alreadyverifyfile="./alreadyverifyfile.txt"
    alreadydict={}
    for line in open(alreadyverifyfile,"r"):
        tablename=line.strip("\n")
        alreadydict[tablename]=""
    gc.log.debug("alreadydict:%s" % (alreadydict))
    for line in open("./log/render.log","r"):
        if line.find("not in verifyplanlist") >= 0:
            tablename=line.split("table:")[1].split()[0]
            tbkey=tablename+" tbbi_isolation_prd5_dst_0" 
            if tbkey not in alreadydict:
                if tbkey in gc.verifyplanList:
                    gc.log.info("tbkey:%s not in alreadydict and in verifyplanList" % (tbkey))
                    needverifyfp.write(tablename+" tbbi_isolation_prd5_dst_0\n")
                else:
                    gc.log.info("tbkey:%s not in gc.verifyplan" % (tbkey))
            else:
                gc.log.info("tbkey:%s in alreadydict" % (tbkey))


if __name__ == '__main__':
    main()
