#! /usr/bin/python 
import sys
import email, smtplib

class SMTP_SSL(smtplib.SMTP):
    def __init__(self, host=''):
         self.host = host
        
    def SendHTML(self,account,passwd,fromAdd,toList,ccList,subject,content):
        msg = email.Message.Message()
        msg['Mime-Version']='1.0'
        msg['Content-Type']='text/html;charset=utf-8'
        msg['From'] = fromAdd
        msg['To'] = toList
        msg['CC'] = ccList
        msg['Subject'] = subject
        msg['Date']    = email.Utils.formatdate()          # curr datetime, rfc2822
        msg.set_payload(content)

        smtp = smtplib.SMTP(host=self.host, port=25)
        smtp.login(account, passwd)
        smtp.sendmail(fromAdd, toList.split(',')+ccList.split(','), msg.as_string())
        smtp.quit()

if __name__ == '__main__':
    from config import * 
    emailSender = SMTP_SSL('smtp-inc.test-inc.com')
    emailSender.SendHTML(mail_from, mail_passwd, mail_from, mail_to, mail_cc, sys.argv[1], sys.stdin.read())
