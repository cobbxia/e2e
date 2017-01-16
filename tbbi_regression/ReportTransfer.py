#coding=utf-8
import ConfigParser
import sys,os,json
import logging
import commonUtility
import threading
import random
import string,time,traceback
from report import mail
from commonUtility import do_cmd,do_odps_filecmd,getExecutor,getSize,partTransfer,addpart,do_odps_cmd,do_mysql_cmd
from TransferUtility import GlobalConf
from Mysql import MysqlData,OdpsTableMeta,updateMysql,getDragList,tableIsExist,mysqltableDelete,deletetable,getPartList,execMysql


def parse_verify_log_error(entry, error):
    entry['ay42_rowcount'] = 'n/a'
    entry['ay39_rowcount'] = 'n/a'
    entry['unmatched'] = 'n/a'
    entry['result'] = 'FAIL'
    entry['comment'] = None
    return entry


def count_equal(base, run):
    if base == 'n/a' and run == 'n/a':
        return 
    if base is None:
        return False
    try:
        base = int(base)
        return base == run
    except ValueError:
        return False

def find_with_prefix(d, key, default=None):
    for k in d.keys():
        if key.startswith(k):
            return d[k]
    return default


def lineiter(pfile):
    if isinstance(pfile, basestring):
        file_path = pfile
        try:
            fobj = file(file_path)
        except IOError:
            return []
    else:
        fobj = pfile

    ret = []
    for line in fobj:
        line = line.strip()
        if line:
            tablename=line.split()[0]
            ret.append(tablename)
    return ret


def log_summary2(log_file):
    counters_json = False
    js = None
    for line in lineiter(log_file):
        if line[0] == '{':
            js = json.loads(line)
            break
    ret = {}
    if js is None:
        ret['left_count'] = 'n/a'
        ret['right_count'] = 'n/a'
        ret['unmatched_count'] = 'n/a'
        ret['status'] = 'FAIL'
        return ret
    ret['left_count'] = js['LeftRecords']
    ret['right_count'] = js['RightRecords']
    ret['unmatched_count'] = js['MismatchRecords']
    if js['MismatchRecords'] != 0 :
        ret['status'] = 'FAIL'
    else:
        ret['status'] = 'PASS'
    return ret

def gencostp(gc):
    renderlogdir=gc.work_dir+os.sep+"logs"+os.sep+"render"+os.sep+gc.timestamp
    httpprefix=gc.httpprefix+"/render/"+gc.timestamp
    gnuscriptname="costp.conf"
    costppng=httpprefix+os.sep+"costp.png"
    open(renderlogdir+os.sep+gnuscriptname,"w").write('''
set terminal png truecolor
set output "costp.png"
set grid
set xtic rotate by 90
set style data histograms
set style fill solid 1.00 border -1
plot "costp.txt"  using 2:xtic(1) title "timespan(hours)"
''')
    cmd='''cd %s;cat %s|gnuplot ''' % (renderlogdir,gnuscriptname)
    r=do_cmd(cmd)
    gc.log.info("costppng:%s" % (costppng))
    print(costppng)
    return costppng


def gencost(gc):
    renderlogdir=gc.work_dir+os.sep+"logs"+os.sep+"render"+os.sep+gc.timestamp
    httpprefix=gc.httpprefix+"/render/"+gc.timestamp
    gnuscriptname="cost.conf"
    costpng=httpprefix+os.sep+"cost.png"
    open(renderlogdir+os.sep+gnuscriptname,"w").write('''
set terminal png truecolor
set output "cost.png"
set style data lines
plot "cost.txt"  using 2:xtic(1) title "timespan(sec)"
''')
    cmd='''cd %s;cat %s|gnuplot ''' % (renderlogdir,gnuscriptname)
    r=do_cmd(cmd)
    gc.log.info("costpng:%s" % (costpng))
    print(costpng)
    return costpng



def uploadCostInfo(gc):
    #gc.timestamp="197001010000"
    sum=0.0
    costdict={}
    costpdict={}
    renderlogdir=gc.work_dir+os.sep+os.sep+"logs"+os.sep+"render"+os.sep+gc.timestamp
    if not os.path.exists(renderlogdir):
        gc.log.info("renderlogdir:%s not exists!" % (renderlogdir))
        return ""
    costfp=open(renderlogdir+os.sep+"cost.txt","w")
    costpfp=open(renderlogdir+os.sep+"costp.txt","w")
    r=(1,"","")
    processedNum=0
    for filename in os.listdir(renderlogdir):
        if filename.endswith("_info"):
            processedNum=processedNum + 1
            nodeid=filename.split("_info")[0]
            cost=(open(renderlogdir+os.sep+filename,"r").readlines()[0]).split(":")[1]
            if nodeid == "" :continue
            costp=int(float(cost))/3600+1
            costdict[nodeid]=cost
            if costp in costpdict:
                costpdict[costp]=costpdict[costp]+1
            else:
                costpdict[costp]=1
    sql='''update e2e.regression_runstat set processedNum='%s' where dirname='%s'; ''' % (processedNum,gc.timestamp)
    execMysql(gc,sql)
    sortedList=sorted(costdict.iteritems(),key=lambda d:float(d[1]),reverse=True)
    for item in sortedList:
        costfp.write("%s\t%s\n" % (item[0],item[1]))
        sum=sum+float(item[1])
    sortedpList=sorted(costpdict.iteritems(),key=lambda d:int(d[0]),reverse=True)
    for itemp in sortedpList:
        costpfp.write("%s\t%s\n" % (itemp[1],itemp[0]))

def getcharacter(gc,statList):
    character["sum"]=0
    character["avg"]=0
    character["max"]=0
    character["min"]=0
    for item in statList:
        item = float(item)
        character["sum"]=character["sum"] + float(item)
        if item > character["max"]:
            character["max"]=item
        if item < character["min"]:
            character["max"]=item
    character["avg"]=character["sum"]/len(statList)
    return character

def statistics(gc):
    message=""
    failret=0
    renderPath  =gc.work_dir+os.sep+"logs"+os.sep+"render"+os.sep+gc.timestamp+os.sep
    httpprefix=gc.httpprefix+"/render/"+gc.timestamp
    if not os.path.exists(renderPath+os.sep):
        gc.log.info("renderPath:%s not exists,do not report." % (renderPath+os.sep))
        return 0
    p=renderPath
    errorpath=gc.work_dir+os.sep+"logs"+os.sep+gc.timestamp
    if not os.path.exists(errorpath):
        os.makedirs(errorpath)
    gc.log.info("errorpath:%s" % (errorpath))
    errorfile=errorpath+os.sep+"error.html"
    gc.log.info("errorfile:%s FAILED uniq" % (errorfile))
    costpng=gencost(gc)
    costppng=gencostp(gc)
    cmd='''grep -i FAILED %s/* ''' % p
    gc.log.info("cmd:"+cmd)
    failedContent=do_cmd(cmd)[1]
    deleteSqlFlag=1
    if failedContent == "":
        gc.log.info("failedContent is empty")
    else:
        failedQueries=failedContent.split("\n")
        failedDict={}
        countDict={}
        sqlDict={}
        gc.log.info("httpprefix:%s baselogdir:%s" % (httpprefix,p))
        for line in failedQueries:
            if (line.strip()!= "" or line != "") and line.find("FAILED:") >= 0:
                sqlFileName=line.split(":")[0]
                key=line.split("FAILED:")[1]
                if "PotGen.pot.gen.failed" in key:
                    key=":".join(key.split(":")[-2:])
                sqlFileName="/render/"+gc.timestamp+line.split("FAILED:")[0].rstrip(":").split(p)[1]
                if key in failedDict:
                    countDict[key]=countDict[key]+1
                else:
                    failedDict[key]=sqlFileName
                    countDict[key]=1
                if sqlFileName not in sqlDict:
                    sqlDict[sqlFileName]=1
                else:
                    sqlDict[sqlFileName]=sqlDict[sqlFileName]+1
                    if sqlDict[sqlFileName]>=3:
                        deleteSqlFlag=0
                gc.log.debug("key:%s cnt:%d" % (key,countDict[key]))
        try:
            errornum=len(sqlDict)
            errornumSql='''update e2e.regression_regression set errornum=%d where dirname='%s';''' % (errornum,gc.timestamp)
            do_mysql_cmd(gc,errornumSql)
        except Exception as e:
            gc.log.fatal("error sqlDict Exception: %s" % (traceback.format_exc()))
        deleteSql='''delete from e2e.regression_errorinfo where dirname='%s';''' % (gc.timestamp)
        do_mysql_cmd(gc,deleteSql)
        for key in failedDict:
            failret=failret + 1
            gc.log.debug("%s,%s" % (key,failedDict[key]))
            sqlFileName = failedDict[key]
            sql='''insert into e2e.regression_errorinfo(dirname,errorkey,errorval,errorcnt) value('%s','%s','%s','%s');''' % (gc.timestamp,key.replace("\"","\\\"").replace("'","\\\'"),sqlFileName,str(countDict[key]))
            cmd='''%s -e "%s" ''' % (gc.mysql,sql)
            gc.log.debug("cmd:%s" % (cmd))
            do_cmd(cmd)
            message +="%d&nbsp&nbsp&nbsp&nbsp<a href=%s>%s</a><br>\n" % (countDict[key],failedDict[key],key)
        if deleteSqlFlag == 0:
            deleteSqlError='''delete from e2e.regression_sqlerrorinfo where dirname='%s';''' % (gc.timestamp)
            do_mysql_cmd(gc,deleteSqlError)
        for sqlFileName in sqlDict:
            if int(sqlDict[sqlFileName]) >=3:
                instanceid=sqlFileName.split("/")[-1].split("_stderr")[0]
                errorNameSql= '''insert into e2e.regression_sqlerrorinfo(dirname,sqlfilename,instanceid) values('%s','%s','%s') ''' % (gc.timestamp,sqlFileName,instanceid)
                cmd='''%s -e "%s" ''' % (gc.mysql,errorNameSql)
                gc.log.debug("cmd:%s" % (cmd))
                do_cmd(cmd)
######################################
######################################
    gc.log.debug("message:%s" %(message))
    open(errorfile,"w").write("<html>"+message+"</html>")
    print("errorfile:%s" % (errorfile))
    print("failret:%s" % (failret))
    gc.log.info(httpprefix+"/"+p.split(p)[1]+"error.html")
    return failret


def VerifyReport(gc,ret):
    try:
        known_rowcounts={}
        verifyPath=gc.work_dir+os.sep+"logs"+os.sep+"verify"+os.sep+gc.timestamp+os.sep
        if not os.path.exists(verifyPath) :
            os.makedirs(verifyPath)
            gc.log.info("path:%s not exits,create it!" % (verifyPath))
        gc.log.info(verifyPath)
        verify_plan = [item for item in gc.verifyplanList if item in gc.res2nodeDict and item !='fail.list' and item !='succ.list']
        gc.log.info("verify_plan:%s" % (verify_plan))
        httpurl = os.path.join(gc.httpurl,"verify", gc.timestamp)
        verifymailurl=os.path.join(httpurl,'mail.html')
        gc.log.info("verifyPath:%s verifymailurl:%s " % (verifyPath,verifymailurl))
        cmd="cp %s %s" % (gc.verifyplan,verifyPath)
        print("cmd:%s" % (cmd))
        do_cmd(cmd)
        gc.log.info("verifyPath:%s " % (verifyPath))
        verify_plan = list(verify_plan)
       
        summary = {}
        pre_total_num=len(verify_plan)
        gc.log.info("pre_total_num:%s" % (pre_total_num))
        pre_succ = verify_plan
        pre_fail = []
        pre_mail = os.path.join(verifyPath, 'mail.html')

        pre = {}
        jobs = {'pre': pre}
        report = {}
        report['title'] = ''
        import socket
        env = {}
        env['Client Machine'] = socket.gethostname()
        env['Local Log Directory'] = ""
        env['Result Project'] = gc.resultproject
        report['info'] = env

        report['jobs'] = jobs
        pre['Total Queries'] = pre_total_num
        pre['Success'] = len(pre_succ)
        pre['Fail'] = len(pre_fail)

        report['date'] = gc.currentday
        report['summary'] = summary
        report['description'] = ''
        summary['Table Count'] = len(verify_plan)
        table_list = []
        succ = 0
        fail = 0
        not_run = 0
        not_run_list=[]
        report['table_list'] = table_list
        if os.path.exists(os.path.join(verifyPath)):
            gc.log.info("fail.list:%s" % (os.path.join(verifyPath,'fail.list')))
            fail_file = open(os.path.join(verifyPath,'fail.list'), 'w')
            gc.log.info("succ.list::%s" % (os.path.join(verifyPath,'succ.list')))
            succ_file = open(os.path.join(verifyPath,'succ.list'), 'w')
            not_run_file = os.path.join(verifyPath,'not_run.txt')
            gc.log.info("not_run_file:%s" % (not_run_file))
            not_run_fp=open(not_run_file,"w")
            gc.log.debug("verify_plan:%s" % (verify_plan))
            for i in verify_plan:
                i = i.split('\t')[0]
                entry = {}
                log_file = os.path.join(verifyPath,i)
                gc.log.debug("log_file:%s" % (log_file))
                page = os.path.join(os.path.basename(verifyPath),i)
                entry['table'] = i
                if not os.path.lexists(log_file):
                    not_run += 1
                    not_run_list.append(i)
                    gc.log.info("log_file:%s not exists." % (log_file))
                    parse_verify_log_error(entry, 'Not Run')
                else:
                    try:
                        gc.log.debug("try to parse table:%s" % (i))
                        log = log_summary2(log_file)
                        entry['ay42_rowcount'] = log['left_count']
                        entry['ay39_rowcount'] = log['right_count']
                        unmatched = log['unmatched_count']
                        #TODO:需要从alisa的执行机器上面把sql和日志抓下来；同时需要分析结果表和输入sql之间的对应关系
                        url = os.path.join(httpurl,i)
                        unmatched = '<a href="%s"> %s </a>' % (url, unmatched)
                        if i not in gc.res2nodeDict:
                            gc.log.debug("i:%s not in res2nodeDict" % (i))
                            continue
                        else:
                            caseurl=os.path.join(gc.httpurl, 'renderSql',gc.res2nodeDict[i])
                            gc.log.debug("i:%s in res2nodeDict,nodeid:%s,caseurl:%s" % (i,gc.res2nodeDict[i],caseurl))
                        entry['table']='<a href="%s"> %s </a>' % (caseurl,i)
                        entry['caseurl'] = caseurl
                        unmatched = '<a href="%s/%s"> %s </a>' % (httpurl,i, unmatched)
                        gc.log.debug("unmatched:%s" % (unmatched))
                        entry['unmatched'] = unmatched
                        entry['result'] = log['status']
                        entry['comment'] = ''
                        gc.log.debug("comment:%s" % (entry['comment']))
                        if 'n/a' == str(log['right_count']) or 'n/a' == str(log['left_count']):
                            not_run += 1
                            not_run_list.append(i)
                            gc.log.debug("i:%s left_count and right_count n/a" % (i))
                            print >>fail_file, i
                            continue
                        if count_equal(0, log['right_count']) or count_equal(0, log['left_count']):
                            not_run += 1
                            not_run_list.append(i)
                            gc.log.debug("i:%s left_count and right_count 0" % (i))
                            print >>fail_file, i
                            continue
                        if log['status'] == 'PASS':
                            succ += 1
                            gc.log.debug("only report failed entries.i:%s succ"  % (i))
                            succ_file.write(i+"\n")
                            # Only report failed entries , or the report will be 
                            # too large.
                            table_list.append(entry)
                        else:
                            pre_count = known_rowcounts.get(i)
                            if count_equal(pre_count, log['right_count']) :
                                gc.log.debug("table:%s known_rowcounts,pre_count:%s right:%s"  % (i,pre_count, log['right_count']))
                                entry['result'] = 'PASS'
                                succ += 1
                                succ_file.write(i+"\n")
                            else:
                                gc.log.debug("table:%s not equal,left:%s right:%s" % (i,log['left_count'], log['right_count']))
                                fail += 1
                            table_list.append(entry)
                            print >>fail_file, i
                    except Exception as e:
                        not_run=not_run+1
                        not_run_list.append(i)
                        gc.log.fatal("error tablename:%s Exception: %s" % (i,traceback.format_exc()))
                    gc.log.debug("table:%s sparse result over" % (i))
            summary['Verify Passed'] = succ
            summary['Verify Failed'] = fail
            summary['Verify Not Run'] = not_run
            for item in not_run_list:
                not_run_fp.write('%s\t%s\t%s\n' % (item,gc.baseproject,gc.resultproject))
            gc.log.debug("begin verify json")
            json_path = os.path.join(verifyPath, 'report.json')
            json_file = open(json_path, 'w')
            json.dump(report, json_file)
            json_file.close() 
            gc.log.debug("json_path:%s" % (json_path))
            from report import mail
            format_tool = os.path.join(os.path.dirname(__file__), 'report','format_report.py')
            o = os.popen('python2.6 %s < %s' % (format_tool, json_path)).read()
            today=time.strftime('%Y%m%d',time.localtime(time.time()))
            title = ''
            gc.log.debug("pre_mail:%s" % (pre_mail))
            gc.log.debug("verifymailurl:%s" % (verifymailurl))
            mailfile=open(pre_mail,"w")
            mailfile.write("%s\n" % (title))
            mailfile.write("%s\n" % (o))
            # Step2: send mail
            if os.path.exists(gc.restop):
                gc.log.debug("restop file:%s exists." % (gc.restop))
                sendVerifyMail(gc,title,o)
        if not os.path.exists(gc.restop):
            if ret == 0:
                gc.log.debug("job run success." )
                jobstatus=2
            if fail == 0 and (not_run != len(verify_plan)):
                gc.log.debug("all needed verify passed"  )
                jobstatus=4
            if ret != 0:
                gc.log.debug("job run failed." )
                jobstatus=3
            if fail != 0 :
                gc.log.debug("verify failed:%d" % (fail) )
                jobstatus=5
        elif os.path.exists(gc.restop):
            if fail == 0 and ret == 0 :
                jobstatus=0
            else:
                jobstatus=1
            import xcase_upload
            module="tbbi"
            group="tbbi"
            case_time=timetrans(gc.timestamp)
            xcase_upload.add_report(module, group, case_time, link, 1-jobstatus, "", "tbbi",pre_total_num, fail)
        gc.log.debug("ret:%d\tnot_run:%d\tlength of verify_plan:%d\tjobstatus:%d" % (ret,not_run,len(verify_plan),jobstatus))
        mod(gc,verifymailurl,jobstatus)
    except Exception as e:
        gc.log.fatal("error report Exception: %s" % (traceback.format_exc()))

def timetrans(intime):
    return intime[0-3]+"-"+intime[4-5]+"-"+intime[6-7]+" "+intime[8-9]+":"+intime[10-11]+":"+intime[12-13]

#标记运行最后状态，用0代表全部运行完毕而且成功，1代表全部运行完毕而且失败；
#标记运行中的状态：用2代表运行过程是成功的，用3代表运行过程出错；
#标记对比中的状态：用4代表对比过程是成功的，用5代表对比过程出错；
def sendVerifyMail(gc,title,o):
    emailSender = mail.SMTP_SSL('smtp-inc.test-inc.com')
    emailSender.SendHTML(gc.mail_from, gc.mail_passwd, gc.mail_from, gc.mail_to, gc.mail_cc,title, o)


def mod(gc,verifymailurl,jobstatus=3):
    r=do_cmd(cmd)
    gc.log.info("cmd:%s r1:%s r2:%s" % (cmd,r[1],[2]))


def main(argDict={}):
    gc=GlobalConf()
    gc.timestamp = "20160623114909"
    if gc.load(configFile="./config.py",operator="report")!=0:
        print("can not load config file:%s" % (configFile))
        sys.exit(1) 
    uploadCostInfo(gc)
    ret=statistics(gc)
    VerifyReport(gc,ret)

    
if __name__ == '__main__':
    main()
