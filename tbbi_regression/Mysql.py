# -*- coding: utf-8 -*-
import ConfigParser,traceback
import sys,re
import logging
import commonUtility
import threading
import random
import string,json,time
from commonUtility import do_cmd,do_odps_cmd,do_odps_filecmd

def getDragList(gc,flag,tablename="",projectname=""):
    flag=int(flag)
    if tablename=="":
        dragSql="select * from partition where flag in (%d,%d);" % (flag,flag+128)
    else:
        tablemeta=OdpsTableMeta(gc)
        tableid=tablemeta.getTableId(tablename,projectname)
        dragSql="select * from partition where flag in (%d,%d) and `table`=%s;" % (flag,flag+128,tableid)
    dragdata=MysqlData(gc,dragSql)
    return dragdata.Rows()

def getUDF(projectName,tableName,fieldName,fieldType,fieldDesc):
    retrytimes=3
    udf=""
    cmd="http://10.125.10.15:8080/beyond/rpc/api/getDesenseUDF.json?projectName=%s&tableName=%s&fieldName=%s&fieldType=%s&fieldDesc=%s" % (projectName,tableName,fieldName,fieldType,fieldDesc)
    i=0
    while i < retrytimes :
        i=i+1
        try:
            r=do_cmd("curl \"%s\"" % (cmd))
            if r[0] == 0:
                js=json.loads(r[1])
                udf=js['content']['udf']
                break
            elif i<retrytimes:
                print("error retytimes:%d cmd:%s r1:%s r2:%s" % (i,cmd,r[1],r[2]))
        except Exception as e:
            print("error cmd:%s Exception: %s r1:%s r2:%s" % (cmd,format(str(e)),r[1],r[2]))
        time.sleep(120)
    udf=udf.replace("md5(","md5_desen(").replace("MD5(","md5_desen(")
    return udf

def updateMysql(gc,sql,opt=""):
    if self.gc.ismysql == "no": return ""
    gc.log.debug("sql:%s" % (sql))
    cmd='''%s %s -e '%s' ''' % (gc.mysql,opt,sql)
    gc.log.debug("cmd:%s" % (cmd))
    r=do_cmd(cmd)
    if r[0] !=0 :
        gc.log.error("cmd error:%s" % (cmd))
        return r
    gc.log.info("cmd:%s over" % (cmd))
    return  r

def deletetable(gc,tablename,partname):
    if self.gc.ismysql == "no": return ""
    gc.log.debug("start delete tablename:%s partname:%s" % (tablename,partname))
    r={}
    executor=""
    try:
        truncSql='''use tbbi_out_dev;drop table IF EXISTS %s;''' % (tablename)
        gc.log.info("truncSql:%s " % (truncSql))
        r=do_odps_filecmd(gc,gc.dstodps,truncSql,executor)
    except Exception as e:
        gc.log.fatal("['error', ('Exception: %s')]" % (traceback.format_exc()))
        r[0]=1
        r[2]=format(str(e))
    gc.log.info("finish delete tablename:%s partname:%s" % (tablename,partname))

def getPartList(gc,odps,tablename,projectname=""):
    if projectname == "":
        projectname=gc.rawproject
    partList=[]
    try:
        hql='''show partitions %s.%s ''' % (projectname,tablename)
        r=do_odps_filecmd(gc,odps,hql)
    except :
        return partList
    if r[0] == 0:
        partList=[i.strip("\n") for i in r[1].split("\n") if i != "" ]
    return partList

class OdpsColumn:
    def __init__(self,mysqlrow):
        self.property=mysqlrow
        self.name=self.property['name']
    def __str__(self):
        if self.property['isSecurity']=="0" and (self.property['desen'] != '' or self.property['desen'] != None):
            desenudf=getUDF("tbbi_isolation",self.name,self.name,self.property['type'],"")
            self.property['desen']=desenudf
            return self.property['desen']
        else:
            return self.name
    def isPart(self):
        return self.property['isPart']

class OdpsTableMeta:
    def getColDict(self,tablename,projectname):
        colDict={}
        ptDict={}
        retDict={}
        cmd='''use %s;desc %s; ''' % (projectname,tablename)
        r=do_odps_cmd(self.gc,self.gc.kodps,cmd)
        flag=0
        if r[0] == 0:
            self.gc.log.info("r[1]:%s" % (r[1]))
            for line in r[1].split("\n"):
                line = line.strip("\n")
                if line == "": continue
                if line.find("Field") >0 and  flag ==0:
                    flag=2
                    continue
                if line.find("Partition Columns") >0 and  flag & 2 ==2 :
                    flag=4
                    continue
                if flag & 2 == 2 and  line.startswith('|'):
                    self.gc.log.debug("line:%s non parted table" % (line))
                    fieldname=line.split('|')[1].strip()
                    fieldtype=line.split('|')[2].strip()
                    colDict[fieldname]=fieldtype 
                if flag & 4 == 4 and line.startswith('|'):
                    self.gc.log.debug("line:%s parted table" % (line))
                    fieldname=line.split('|')[1].strip()
                    fieldtype=line.split('|')[2].strip()
                    ptDict[fieldname]=fieldtype 
            retDict["col"]=colDict
            retDict["pt"]=ptDict
            self.gc.log.info("retDict:%s" % (retDict))
        self.allColDict=retDict
        return retDict
    def __init__(self,gc):
        self.gc=gc
        self.isTableInited=0
        self.tableinfo={}
        self.isinit=1
        self.projectname=""
        self.tablename=""
    def initTable(self,tableid):
        if self.gc.ismysql == "no":return ""
        self.tableid=tableid
        tableSql='select * from `table` where id=%s;' % (self.tableid)
        mytabledata=MysqlData(self.gc,tableSql)
        if mytabledata.RowNum()==0:
            return ""
        self.tableinfo=mytabledata.Row(0)
        self.gc.log.debug( "tableinfo:%s"  % (self.tableinfo))
        self.isinit=0
    def getProjectName(self):
        if self.gc.ismysql == "no":return ""
        projectname=self.tableinfo['projectname'].strip().strip("\n")
        if projectname == "" or projectname is None:
            projectname=gc.defaultproject
        self.projectname=projectname
        return projectname
    def getTableName(self,tableid):
        if self.gc.ismysql == "no":return ""
        self.tableid=tableid
        if self.isinit !=0 :
            self.initTable(self.tableid)
        tableNameSql='select name from `table` where id=%s;' % (self.tableid)
        myiddata=MysqlData(self.gc,tableNameSql)
        if myiddata.RowNum()==0:
            return ""
        self.gc.log.debug( "0 row:%s"  % (myiddata.Row(0)))
        self.tablename=myiddata.Row(0)['name']
        self.gc.log.debug( "tablename:%s"  % (self.tablename))
        return self.tablename
    def getTableId(self,tablename,projectname):
        if self.gc.ismysql == "no":
            self.getColDict(tablename,projectname)
            return ""
        self.tablename=tablename
        self.projectname=projectname
        self.gc.log.debug("tablename:%s" % (tablename))
        tableIdSql='select id from `table` where name="%s" and projectname="%s";' % (self.tablename,self.projectname)
        #projectname不相同而talename相同，这样的表可能会有多个
        #tableIdSql='select id from `table` where name="%s" and projectname="%s";' % (self.tablename,self.projectname)
        self.gc.log.debug("tableIdSql:%s" % (tableIdSql))
        myiddata=MysqlData(self.gc,tableIdSql)
        if myiddata.RowNum()==0:
            self.gc.log.debug("tableIdSql:%s" % (tableIdSql))
            return ""
        self.tableid=myiddata.Row(0)['id']
        if self.isinit !=0 :
            self.initTable(self.tableid)
        return self.tableid
    def getPartId(self,partname):
        if self.gc.ismysql == "no":
            return ("","")
        if partname != "" and partname != None:
            partIdSql='select id,flag from partition where `table`="%s" and part="%s"' % (self.tableid,partname)
        else:
            partIdSql='select id,flag from partition where `table`="%s"' % (self.tableid)
        self.gc.log.debug("partIdSql:%s" % (partIdSql))
        mypartiddata=MysqlData(self.gc,partIdSql)
        if mypartiddata.RowNum()==0: 
            return ("","")
        self.partid=mypartiddata.Row(0)['id']
        flag=mypartiddata.Row(0)['flag']
        self.gc.log.debug("partid:%s flag=%s" % (self.partid,flag))
        return (self.partid,flag)
    def isPartedTable(self):
        if self.gc.ismysql == "no":
            if "pt" in self.allColDict:
                return int(len(self.allColDict["pt"].keys()))
            else:
                return 0
        isPartedSql='select ispart from `table` where id="%s"' % (self.tableid)
        self.gc.log.debug("isPartedSql:%s" % (isPartedSql))
        mypartdata=MysqlData(self.gc,isPartedSql)
        ispart=int(mypartdata.Row(0)['ispart'])
        self.gc.log.debug("ispart:%d" % (ispart))
        return ispart
    def getAllColList(self,type=""):
        if self.gc.ismysql == "no":
            allColList=self.allColDict["col"].keys()+self.allColDict["pt"].keys()
        else:  
            allColSql=''' select name,udf,type,desen,ispart,isSecurity,isFK from `column` where `table`=%s; ''' % (self.tableid)
            mycoldata=MysqlData(self.gc,allColSql)
            if type != "desen":
                allColList=[row["name"] for row in mycoldata.Rows()]
            else:
                allColList=[str(OdpsColumn(row)) for row in mycoldata.Rows()]
        self.gc.log.debug("all cols:%s" % (allColList))
        return allColList
    def getNonPartedColList(self,type="desen"):
        if self.gc.ismysql == "no":
            colList=self.allColDict["col"].keys()
        else:  
            colSql=''' select name,udf,type,desen,ispart,isSecurity,isFK from `column` where `table`=%s and ispart!=0; ''' % (self.tableid)
            mycoldata=MysqlData(self.gc,colSql)
            if type != "desen":
                colList=[row["name"] for row in mycoldata.Rows()]
            else:
                colList=[str(OdpsColumn(row)) for row in mycoldata.Rows()]
        self.gc.log.debug("non parted cols:%s" % (colList))
        return colList
    def getPartDetailPairList(self,preflag='0'): 
        if self.gc.ismysql == "no": return []
        partDetailSql=''' select part,flag,size from partition where `table`=%s; ''' % (self.tableid)
        mypartdata=MysqlData(self.gc,partDetailSql)
        self.gc.log.debug("Rows() :%s" % (mypartdata.Rows()))
        partDetailPairList=[(row['part'].replace("/",","),row['part']) for row in mypartdata.Rows() if row['flag'] == preflag]
        self.gc.log.debug("parted cols detail:%s,preflag:%s" % (partDetailPairList,preflag))
        return partDetailPairList

class OdpsPartColumn:
    def __init__(self,gc,tablename,mysqlrow):
        self.gc=gc
        self.tablename=tablename
        if 'table' in mysqlrow:
            self.tableid=mysqlrow['table']
        if 'partition' in mysqlrow:
            self.partDetail=mysqlrow['partition']
    def getPart(self):
        self.part=",".join([p for p in self.partDetail.split("/")])
        return self.part
    def getPartSize(self):
        hql="desc %s partition(%s);" % (self.tablename,self.partDetail)
        cmd="%s -e '%s'" % (self.gc.srcodps,hql)

def ifColExist(gc,tableid,colname):
    cmd='''%s -N -e 'select count(1) from `column` where `table`=%s and name="%s"; ' ''' % (gc.mysql,tableid,colname)
    gc.log.debug("cmd:%s" % (cmd))
    r=do_cmd(cmd)
    if r[0] !=0 :
        gc.log.error("cmd error:%s" % (cmd))
        return -1
    else:
        return r[1]

def mysqltableDelete(gc,tablename,projectname):
    sql="delete from `table` where name=\"%s\" and projectname=\"%s\";" % (tablename,projectname)
    updateMysql(gc,sql)

def tableIsExist(gc,tablename,projectname):
    if self.gc.ismysql == "no": return 0
    tableIsExistSql="select * from `table` where name=\"%s\" and projectname=\"%s\";" % (tablename,projectname)
    data=MysqlData(gc,tableIsExistSql)
    rownum=data.RowNum()
    gc.log.debug("tableIsExistSql:%s rownum:%d" % (tableIsExistSql,rownum))
    if rownum !=0:
        gc.log.info("tablename:%s already exists,no need to proecess" % (tablename))
        return 1
    else:
        return 0

def partIsExist(gc,tableid,partname):
    partname=partname.replace("'","").replace("\"","")
    partIsExistSql="select * from partition where part=\"%s\" and `table`=%s and flag is not NULL;" % (partname,tableid)
    gc.log.debug("partIsExistSql:%s" % (partIsExistSql))
    data=MysqlData(gc,partIsExistSql)
    rownum=data.RowNum()
    gc.log.debug("rownum:%d" % (rownum))
    if rownum !=0:
        gc.log.info("tableid:%s partname:%s other partition already exists,no need to transfer" % (tableid,partname))
        return 1
    else:
        return 0

#MysqlData(self.gc,"select count(1) from `table`")
class MysqlData:
    def __init__(self,gc,sql):
        self.ColNameList=[]
        self.data=[]
        lineno=0
        gc.log.debug("sql:%s" % (sql))
        r=updateMysql(gc,sql)
        if r[0]!=0:
            rawMysqlData=None
        else:
            rawMysqlData=r[1]
        for line in rawMysqlData.split("\n"):
            line=line.strip()
            if line=="" or line is None:
                continue
            if lineno==0:
                for col in line.split("\t"):
                    self.ColNameList.append(col)
            else:
                datadict={}
                contents=[content for content in line.split("\t")]
                for i in range(len(contents)):
                    datadict[self.ColNameList[i]]=contents[i]
                #gc.log.debug("datadict:%s" % (datadict))
                self.data.append(datadict)
            lineno=lineno+1
    def RowNum(self):
        return len(self.data)
    def Rows(self):
        return self.data
    def Row(self,rowno):
        return self.data[rowno]
    def Cell(self,rowno,cellname):
        return self.data[rowno][cellname]
    def RowMeta(self):
        return self.ColNameList


def execMysql(gc,sql):
    cmd='''%s -N -e "%s" ''' % (gc.mysql,sql)
    gc.log.debug("cmd:%s" % (cmd))
    r=do_cmd(cmd)
    if r[0] !=0 :
        gc.log.fatal("cmd:%s\nstdout:%s\nstderr:%s\n" % (cmd,r[1],r[2]))
        return -1
    else:
        return r[1]

