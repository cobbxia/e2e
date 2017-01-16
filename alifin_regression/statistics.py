#-*-coding:utf-8 -*-

import os,traceback
import sys,subprocess
import email
import smtplib
from email.Message import Message
import xcase_upload

def timefmt(intime):
    timeList=intime.split("_") 
    print(timeList)
    return timeList[0]+"-"+timeList[1]+"-"+timeList[2]+" "+timeList[3]+":"+timeList[4]+":"+timeList[5]

def do_cmd(cmd):
    print(cmd)
    subp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = subp.communicate()
    return (subp.returncode, out.strip("\n"), err)

def filterContent(filename):
    f=open(filename,"r")
    realcontent=""
    c=f.readlines()
    flag=0
    for line in c:
        if line.find("<body>")>=0 and flag==0:
            if len(line.split("<body>"))==2:
                line=line.split("<body>")[1]
            flag=1
        if line.find("</body>")>=0 and flag==1:
            line=line.split("</body>")[0]
            realcontent=realcontent+line
            break
        if flag==1:
            realcontent=realcontent+line
    return realcontent

class SMTP_SSL(smtplib.SMTP):
    def __init__(self, host=''):
        self.host = host
        self.filename=""
    def SendHTML(self, account, passwd, fromAdd, toList, ccList, subject, content):
        msg = Message()
        msg['Mime-Version']='1.0'
        msg['Content-Type']='text/html;charset=UTF-8'
        msg['From'] = fromAdd
        msg['To'] = toList
        msg['CC'] = ccList
        msg['Subject'] = subject
        msg['Date'] = email.Utils.formatdate()
        msg.set_payload(content)
        smtp = smtplib.SMTP(host=self.host, port=25)
        smtp.login(account, passwd)
        smtp.sendmail(fromAdd, toList.split(',')+ccList.split(','), msg.as_string())
        if os.path.exists(self.filename):
            file(self.filename,"w").write("<html>"+msg.as_string()+"</html>")
        smtp.quit()

if __name__ == '__main__':
    try:
        helpdoc=""
        FAQdoc =""
        ruledoc=""
        sessionflag=""
        if "sessionflag" in os.environ:
            sessionflag = os.environ['sessionflag']
        mail_to = ','.join([""])
        os.chdir(sys.argv[1])
        mail_title = sys.argv[2] 
        cmd="ls -lrt | grep log_ | awk '{print $9}' | tail -n1"
        r = do_cmd(cmd)
        if r[0] != 0:
            print("d_path:%s not exist." % (cmd))
        d_path = r[1].strip("\n")
        log_dir = "%s/odps/logs/" % d_path
        
        print("d_path:%s log_dir:%s"  % (d_path,log_dir))
        failedDict={}
        countDict={}
    
        ip=do_cmd("hostname -i")[1]
        dirname=log_dir.split("/")[0].split("log_")[1]
        print("dirname:"+dirname)
        workprefix=jenkins_job_url+"/ws"
        mailfilename="%s/odps/%s__%s__mail__%s.html" % (d_path,groupname,dirname,ip)
        link="%s/%s/%s" % (jenkins_job_url,"ws",mailfilename )
        print("mailfilename:%s link:%s" % (mailfilename,link))
        corefilename="./alifin_regression/coreMachine.txt"
        corelist=[]
        if os.path.exists(corefilename):
            corelist=file(corefilename,"r").readlines()
        coremessage="<br>\n".join([line for line in corelist])
        print("coremessage:%s" % coremessage)
        if len(corelist) == 0 :
            coremessage="no core file founded"
        reportListUrl=''
        message = '''<br>\n
                        sessionflag:%s                 <br>\n                
			<a href="%s">历史报告      </a><br>\n
			<a href="%s">jenkins回归任务</a><br>\n
                        <a href="%s">本次jenksin回归信息</a><br>\n
                        <a href="%s">金融回归FAQ</a><br>\n
			<a href="%s">帮助文档        </a><br>\n
			<a href="%s">金融回归使用规范</a><br>\n''' % (sessionflag,reportListUrl,os.environ['JOB_URL'],os.environ['BUILD_URL'],FAQdoc,helpdoc,ruledoc)
        message = message + "<br>\n1. nvm job / service job / fuxi job比例:<br>\n"
        print("message:%s" % message)

        service_job_count = int(do_cmd("grep 'service job' %s/* -rl | wc -l" % log_dir)[1])
        nvm_job_count = int(do_cmd("grep 'service job, nvm' %s/* -rl | wc -l" % log_dir)[1])
        fuxi_job_count = int(do_cmd("grep 'fuxi job' %s/* -rl | wc -l" % log_dir)[1])
        total_job_count=service_job_count + fuxi_job_count
        is_service=1
        if mail_title.find("servicemode") >=0:
            is_service=0
        print("is_service:%d" % (is_service))
        if total_job_count != 0:
            percentile=(fuxi_job_count)/(total_job_count *1.0)
        else:
            percentile=0
        print("percentile:%f total_job_count:%d" % (percentile,total_job_count))
        message += "%d / %d / %d<br>\n" % (nvm_job_count, (int(service_job_count)-int(nvm_job_count)), fuxi_job_count)
        if percentile > 0.90 and is_service == 0:
            print("service mode not normal,please check")
            message=message+"<font size=\"10\" face=\"Verdana\" color=\"red\">service mode not normal,please check</font>\n<br>"
            mail_title="FAIL "+mail_title 
        
        message += "2. sql重试占比:(不重试/重试1次/重试2次):"

        retry0 = int(do_cmd("ls -al %s/*.err | wc -l"   % log_dir)[1])
        retry1 = int(do_cmd("ls -al %s/*.err.1 | wc -l" % log_dir)[1])
        retry2 = int(do_cmd("ls -al %s/*.err.2 | wc -l" % log_dir)[1])
        print("retry0:%d retry1:%d retry2:%d" % (retry0,retry1,retry2))
        message += "%s / %s / %s<br>\n<br>\n" % ((retry0-retry1), (retry1-retry2), retry2)
        print("3. FAILED uniq")
        message += "3. FAILED uniq:<br>\n"
        cmd='''grep FAILED %s/* ''' % log_dir
        failedContent=do_cmd(cmd)[1]
        print("cmd:%s\tfailedContent:%s" % (cmd,failedContent))
        failure_num=0
        if failedContent != "":
            failedQueries=failedContent.split("\n")
            print("workprefix:%s" % (workprefix))
            for line in failedQueries:
                failure_num = failure_num + 1
                if (line.strip()!= "" or line != "") and line.find("FAILED:") >= 0:
                    key=line.split("FAILED:")[1]               
                    val=workprefix+"/"+line.split("FAILED:")[0].rstrip(":")+"/*view*/"
                    if key in failedDict:
                        countDict[key]=countDict[key]+1
                    else:
                        failedDict[key]=val
                        countDict[key]=1
            for key in failedDict:
                print("%s,%s" % (key,failedDict[key]))
                message +="%d&nbsp&nbsp&nbsp&nbsp<a href=%s>%s</a><br>\n" % (countDict[key],failedDict[key],key)
        status = "1"
        if failure_num !=0:
            status="0"
        if os.path.exists(os.path.join(d_path, 'status.json')):
            status="0"
            
        message += "4. servicemode长尾top10:<br>\n"
        message += "%s<br>\n" % str(do_cmd("grep 'service job' %s/*  -1 | grep 'run time' | sort -n -k 4 | tail" % log_dir)[1])
        message += "5. servicemode运行时间总和:<br>\n"
        message += "%s<br>\n" % str(do_cmd("grep 'service job' %s/*  -1 | grep 'run time' | awk '{print $4}' | awk '{sum += $1};END {print sum}'" % log_dir)[1])
        message += "6. fuxi job长尾top10:<br>\n"
        message += "%s<br>\n" % do_cmd("grep 'fuxi job' %s/*  -1 | grep 'run time' | sort -n -k 4 | tail" % log_dir)[1]
        message += "7. core文件统计情况:<br>\n"
        message += coremessage
        innerMailFile="%s/../mail.html" % log_dir
        print("innerMailFile:"+innerMailFile)
        if os.path.exists(innerMailFile): 
            message += filterContent(innerMailFile)
        
        emailSender = SMTP_SSL('smtp-inc.test-inc.com')
        fromAdd = "apsara.testmail@test-inc.com"
        toList = mail_to
        ccList = "mingchao.xiamc@test-inc.com"
        passwd = 'apsaratest'
        reportURL=link
        print("mail_title:%s\treportURL:%s" % (mail_title,reportURL))
        message="<br>\n<a href="+reportURL+">"+mail_title+"</a><br>\n"+message
        message="<html lang=\"zh-CN\" xmlns=\"http://www.w3.org/1999/xhtml\"><head>    \
        <meta charset=\"utf-8\">   \
        <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\"> \
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">  \
        <meta http-equiv=\"refresh\" content=\"20\"><title>"+mail_title+"</title></head><body>"+message+"</body><html>"
        open(mailfilename,"w").write(message)
        #emailSender.setfilename(d_path)
        cnt=0
        retrytimes=3
        while cnt < retrytimes:
            try:
                print("send %d mail,fromAdd:%s, passwd:%s, fromAdd:%s, toList:%s, ccList:%s, mail_title:%s, message:%d" % (cnt,fromAdd, passwd, fromAdd, toList, ccList, mail_title, len(message)))
                emailSender.SendHTML(fromAdd, passwd, fromAdd, toList, ccList, mail_title, message)
                break
            except  Exception ,e: 
                print("Exception:%s" % traceback.format_exc())
            cnt=cnt+1
        case_time=timefmt(dirname)
##############
        xcase_upload.add_report(realModuleName, realGroupName, case_time, link, status, 0, realGroupName,total_job_count, failure_num)
##############
    except  Exception ,e: 
        print("Exception:%s" % traceback.format_exc())


