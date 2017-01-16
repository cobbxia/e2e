#a! /usr/ali/bin/python

import os
import re
import time
import datetime
import string 
import random
import logging
import threading
import getopt, sys 
import logging.handlers
import traceback
#import ConfigParser
import math
from time import localtime, strftime, sleep
from datetime import date, datetime
import subprocess
from subprocess import *
import simplejson as json
sys.path.append(sys.path[0] + "/../common/")
import config as Config

File_Type = {0 : 'text', 1 : 'cfile', 2 : 'rcfile', 3 : 'seqfile'}
Conf_Out_Dir = Config.verify_conf_dir 
RetryTimes = 3 

#conf_file = sys.path[0] + 'default.cfg'
#if not os.path.isfile(conf_file):
#    print conf_file + ' is invalid'
#    exit(-1)
#config = ConfigParser.ConfigParser()
#config.read(conf_file)


def Exit_Verify(msg, type):
    print 'hello###############Exit_Verify'
    ret_msg = 'yongfeng.chai say: Execute verify_table.py end. '
    ret_msg += msg.replace("\n", "\\n")
    if type:
        print ret_msg
        exit(0)
    else:
        ret_msg += '. UNFORTUNATELY!!'
        print ret_msg
        exit(-1)

def DescTable(declient, table_name):
    print 'hello###############DescTable'
    cmd = declient + ' -j -e "desc ' + table_name + ';"'
    for i in range(0, RetryTimes):
        out = os.popen(cmd).read()
        try:
            print "return value of desc1: ****\n" + out
            param = json.loads(json.loads(out)[0]['Message'])
            return param
        except Exception, e:
            if i == RetryTimes - 1:
                msg = "can't desc table " + table_name + '(' + cmd + ')' + str(e) + str(traceback.format_exc())
                Exit_Verify(msg, False)
            else:
                time.sleep(5)

def GetPartitionInfo(declient, table_name):
    cmd = declient + ' -j -e "GET_PARTITION_INFO ' + table_name + ';"'
    print 'get_partition_info from here: ', cmd
    for i in range(0, RetryTimes):
        out = os.popen(cmd).read()
        try:
            param = json.loads(json.loads(out)[0]['Message'])
            return param
        except Exception, e:
            if i == RetryTimes - 1:
                msg = "can't get_partition_info " + table_name + '(' + cmd + ')' + str(e)  + str(traceback.format_exc())
                Exit_Verify(msg, False)
            else:
                print "get_partition_info failed", out, ', will retry'
                time.sleep(5)


def CreateExternalTable(declient, table_name):
    tmp_tablename = 'external_for_verify_' + table_name
    #cmd = declient + ' -j -e "drop table ' + tmp_tablename + '; CREATE EXTERNAL TABLE ' + tmp_tablename + ' LIKE ' + table_name + ';"'
    cmd = declient + ' -j -e " CREATE EXTERNAL TABLE ' + tmp_tablename + ' LIKE ' + table_name + ';"'
    print cmd
    for i in range(0, RetryTimes):
        out = os.popen(cmd).read()
        print out
        try:
            param = json.loads(out)[0]['Result']
            if param == 'OK':
                return
            else:
                msg = "create external table failed. (" + cmd + ')' + json.loads(out)[0]['Message'] + str(traceback.format_exc())
                Exit_Verify(msg, False)
        except Exception, e:
            if i == RetryTimes - 1:
                msg = "create external table failed. (" + cmd + ')' + str(e) + str(traceback.format_exc())
                Exit_Verify(msg, False)
            else:
                time.sleep(5)

def AddPartition(declient, table_name, partStr, partLocation):
    cmd = declient + ' -j -e "ALTER TABLE external_for_verify_' + table_name + ' ADD PARTITION (' + partStr + ') LOCATION \\"' + partLocation + '\\";"'
    print cmd
    for i in range(0, RetryTimes):
        out = os.popen(cmd).read()
        try:
            param = json.loads(out)[0]['Result']
            if param == 'OK':
                return
            else:
                msg = "add partition failed. (" + cmd + ')' + json.loads(out)[0]['Message'] + str(traceback.format_exc())
                Exit_Verify(msg, False)
        except Exception, e:
            if i == RetryTimes - 1:
                msg = "add partition failed. (" + cmd + ')' + str(e) + str(traceback.format_exc())
                Exit_Verify(msg, False)
            else:
                time.sleep(5)

def DropTable(declient, table_name):
    cmd = declient + ' -j -e "DROP TABLE external_for_verify_' + table_name + ';"'
    print cmd
    out = os.popen(cmd).read()
    i = 0
    try:
        param = json.loads(out)[0]['Result']
        if param == 'OK':
            return
        else:
            msg = "drop table failed. (" + cmd + ')' + json.loads(out)[0]['Message'] + str(traceback.format_exc())
            Exit_Verify(msg, False)
    except Exception, e:
        if i == RetryTimes - 1:
            msg = "drop table failed. (" + cmd + ')' + str(e) + str(traceback.format_exc())
            Exit_Verify(msg, False)
        else:
            time.sleep(5)


class VerifyConf(object):

    def __init__(self, table_l, table_r, today, ignore_cols = None):

        print 'hello###############__init__'
        '''conf_file = '/apsara/de4collie/AliLoanRegression/verify_tool/default.cfg'
        if not os.path.isfile(conf_file):
            msg = 'Config file "/apsara/de4collie/AliLoanRegression/verify_tool/default.cfg" is invalid'
            Exit_Verify(msg, False)
        config = ConfigParser.ConfigParser()
        config.read(conf_file)'''

        self.declient = Config.de_client #config.get("tool", "declient.home")
        self.hadoop = Config.hadoop_home #config.get("tool", "hadoop.home")
        self.verifier = Config.verifier_path #config.get("tool", "verifier.path")
        self.pu = Config.pu #config.get("tool", "pu.home")

        vec = table_l.split(';')
        self.left_part = []
        if len(vec) == 2: #has partition
            self.left_part = vec[1].replace("%3A", ":").split(',') #
        self.left_table = vec[0]

        vec = table_r.split(';')
        self.right_part = []
        if len(vec) == 2:
            self.right_part = vec[1].replace("%3A", ":").split(',')
        self.right_table = vec[0]

        self.today = today
        self.left_dir = None
        self.right_dir = None
        self.left_format = None
        self.right_format = None
        self.schema = None
        self.left_field_delim = None
        self.right_field_delim = None
        self.ignore_cols = ''
        if ignore_cols != None:
            self.ignore_cols = ignore_cols
        if len(self.left_part) == 0 and len(self.right_part) == 0:  #both left and right have no partition
            self.out_file = '%s%s-basetable_%s.conf' % (Conf_Out_Dir, self.left_table, self.left_table)
        else:
            self.out_file = '%s%s%s-basetable%s%s.conf' % (Conf_Out_Dir, self.left_table, str(self.left_part), self.left_table, str(self.right_part))
            self.out_file = self.out_file.replace("'", '').replace('[', '.').replace(']', '').replace(' ', '_').replace(':', '_')

class Verifier:

    def __init__(self, conf):

        print 'hello###############__init__2'
        self.conf = conf
        self.verify_param = None

    def run(self):
        print 'hello###############run'
        try:
            self.gen_verify_param()
            self.export()
            self.run_verify()
        except Exception, e:
            msg = 'unknown exception: ' + str(e) + str(traceback.format_exc())
            Exit_Verify(msg, False)

    def export(self):
        print 'hello###############export'
        cmd = self.conf.declient + ' -N -j -e "export data topath pangu://localcluster/tmp/collie/' + self.conf.left_table + ' from table ' + self.conf.left_table + ';" 2>&1 | grep -v "declient.LOG"'
        print cmd
        pipe = os.popen(cmd)
        returnStr = pipe.read()
        print "return value of export2: ****\n" + returnStr
        res = json.loads(returnStr)
        pipe.close()
        result = res[len(res) - 1]
        query_id = []
        if result['Result'] == 'OK':
            if result['Message'].find('only shema is exported to pangu') == -1:
                query_id.append(result['QueryID'])
                print cmd + '  ' + str(result['QueryID'])
            else:
                print cmd + '  ' + str(result['Message'])
        else:
            msg = 'export job submit failed' + str(result)
            Exit_Verify(msg, False)
        cmd = self.conf.declient + ' -N -j -e "export data topath pangu://localcluster/tmp/collie/' + self.conf.right_table + ' from table ' + self.conf.right_table + ';" 2>&1 | grep -v "declient.LOG"'
        pipe = os.popen(cmd)
        returnStr = pipe.read()
        print "return value of export3: ****\n" + returnStr
        res = json.loads(returnStr)
        pipe.close()
        result = res[len(res) - 1]
        if result['Result'] == 'OK':
            if result['Message'].find('only shema is exported to pangu') == -1:
                query_id.append(result['QueryID'])
                print cmd + '  ' + str(result['QueryID'])
            else:
                print cmd + '  ' + str(result['Message'])
        else:
            msg = 'export job submit failed: ' + str(result)
            Exit_Verify(msg, False)
        while len(query_id) != 0:
            for id in query_id:
                time.sleep(10)
                cmd = self.conf.declient + ' -j -N -e "status ' + id + ';" 2>&1 | grep -v "declient.LOG"'
                pipe = os.popen(cmd)
                get_status_retryTimes = 0
                try:
                    r = pipe.read()
                    print "return value of status4: ****\n" + r
                    res = json.loads(r)[0]
                except Exception:
                    msg = 'get export job status failed: ' + str(r) + str(traceback.format_exc())
                    print msg, 'retry times:', get_status_retryTimes
                    get_status_retryTimes += 1
                    if get_status_retryTimes == 3:
                        Exit_Verify(msg, False)
                if res['Message'] == 'Terminated':
                    query_id.remove(id)
                    cmd = self.conf.declient + ' -N -j -e "kill ' + id + ';"'
                    os.popen(cmd)
                    break
                elif (res['Message'].find('ErrorMessage') != -1) or (res['Message'].find('Unknown') != -1) or (res['Message'].find('Failed') != -1):
                    msg = 'export job failed: ' + id + str(res)
                    Exit_Verify(msg, False)
        cmd = self.conf.pu + ' rm -f pangu://localcluster/tmp/collie/' + self.conf.left_table + '/schema'
        os.popen(cmd)
        cmd = self.conf.pu + ' rm -f pangu://localcluster/tmp/collie/' + self.conf.right_table + '/schema'
        os.popen(cmd)
        print 'export finish' 



    def gen_verify_param(self):
        print 'hello###############gen_verify_param'
#        today = self.conf.today
#        if os.path.exists(self.conf.out_file):
#            os.system("sed -i 's/basetable[0-9]*_/basetable%s_/g' %s" % (today, self.conf.out_file))
#            return
        out_file = file(self.conf.out_file, 'w')
        paramAll = DescTable(self.conf.declient, self.conf.left_table)
        param = json.loads(paramAll['GetTableMetaString'])

        #get column schema for left table
        left_schema = ''
        left_part_cols = []

        for col in param['Schema']:
            if col['IsPartitionColumn']:
                left_part_cols.append(col['Name'])
                continue
            if col['Type'] == 0:
                left_schema = left_schema + 'l'
            elif col['Type'] == 1:
                left_schema = left_schema + 'd'
            elif col['Type'] == 2:
                left_schema = left_schema + 'b'
            elif col['Type'] == 3:
                left_schema = left_schema + 's'
            else:
                msg = 'unknown column type for left schema: ' + col['Name'] + '[' + str(col['Type']) + ']'
                Exit_Verify(msg, False)

        if len(left_part_cols) != len(self.conf.left_part):
            msg = 'left partition value is invalid for columns: ' + str(left_part_cols) + str(self.conf.left_part)
            Exit_Verify(msg, False)

        #self.conf.left_dir = param['Location']
        print left_part_cols
        if len(left_part_cols) != 0:
            DropTable(self.conf.declient, self.conf.left_table)
            CreateExternalTable(self.conf.declient, self.conf.left_table)
            partStr = ''
            for i in range(0, len(left_part_cols)):
                partStr = partStr + left_part_cols[i] + '=\\"' + self.conf.left_part[i] + '\\"' + ','
            partParam = GetPartitionInfo(self.conf.declient, self.conf.left_table)
            for part in partParam:
                if part['PartitionValue'] == self.conf.left_part:
                    AddPartition(self.conf.declient, self.conf.left_table, partStr[:-1], part['PartitionLocation'].replace('pangu://localcluster', ''))
            self.conf.left_table = 'external_for_verify_' + self.conf.left_table
        self.conf.left_dir = 'pangu://localcluster/tmp/collie/' + self.conf.left_table + '/'
        out_file.write('left.dir = ' + self.conf.left_dir.replace(' ', '\\ ') + '\n')
        delim = '%x' % int(param['FieldDelimiter'])
        self.conf.left_field_delim = '\u' + '0' * (4 - len(delim)) + delim
        out_file.write('left.field.delim = ' + self.conf.left_field_delim + '\n')
        if param['NullIndicator'] == '':
            out_file.write('left.null.indicator = ' + '\n')
        else:
            out_file.write('left.null.indicator = \\' + param['NullIndicator'] + '\n')
        out_file.write('left.use.bin = false' + '\n')

        #out_file.write('left.format = cfile' + '\n')
        out_file.write('left.format = text' + '\n')
        #cmd = os.path.dirname(os.path.abspath(__file__)) + '/check_file_type "' + self.conf.left_dir + '" ' + delim + ' ' + str(param['RowDelimiter']) + ' "' + param['NullIndicator'] + '"'
        #res = os.popen(cmd).read().strip()
        #print cmd
        #if res == 'cfile' or res == 'seqfile' or res == 'text':
        #    out_file.write('left.format = ' + res + '\n')
        #elif res.startswith('have no file in this dir'):
        #    out_file.write('left.format = cfile' + '\n')
        #else:
        #    msg = 'get file type failed: ' + self.conf.left_dir + ', ' + cmd + ', ' + res
        #    Exit_Verify(msg, False)
        #print res

        paramAll = DescTable(self.conf.declient, self.conf.right_table)
        param = json.loads(paramAll['GetTableMetaString'])
        right_schema = ''
        right_part_cols = []
        for col in param['Schema']:
            if col['IsPartitionColumn']:
                right_part_cols.append(col['Name'])
                continue
            if col['Type'] == 0:
                right_schema = right_schema + 'l'
            elif col['Type'] == 1:
                right_schema = right_schema + 'd'
            elif col['Type'] == 2:
                right_schema = right_schema + 'b'
            elif col['Type'] == 3 or col['Type'] == 4:
                right_schema = right_schema + 's'
            else:
                msg = 'unknown column type for right schema: ' + col['Name'] + '[' + str(col['Type']) + ']'
                Exit_Verify(msg, False)
        if left_schema == '' or right_schema == '' or left_schema != right_schema:
            msg = 'check schema failed: ' + left_schema + '\t' + right_schema
            Exit_Verify(msg, False)

        if len(left_part_cols) != 0:
            DropTable(self.conf.declient, self.conf.right_table)
            CreateExternalTable(self.conf.declient, self.conf.right_table)
            partStr = ''
            for i in range(0, len(right_part_cols)):
                partStr = partStr + right_part_cols[i] + '=\\"' + self.conf.right_part[i] + '\\"' + ','
            partParam = GetPartitionInfo(self.conf.declient, self.conf.left_table)
            for part in partParam:
                if part['PartitionValue'] == self.conf.right_part:
                    AddPartition(self.conf.declient, self.conf.right_table, partStr[:-1], part['PartitionLocation'].replace('pangu://localcluster', ''))
            self.conf.right_table = 'external_for_verify_' + self.conf.right_table
        #self.conf.right_dir = param['Location']
        self.conf.right_dir = 'pangu://localcluster/tmp/collie/' + self.conf.right_table
        out_file.write('right.dir = ' + self.conf.right_dir.replace(' ', '\\ ') + '\n')
        delim = '%x' % int(param['FieldDelimiter'])
        self.conf.right_field_delim = '\u' + '0' * (4 - len(delim)) + delim
        #just fucking hack
        #out_file.write('right.field.delim = \u0001\n')
        out_file.write('right.field.delim = ' + self.conf.right_field_delim + '\n')
        if param['NullIndicator'] == '':
            out_file.write('right.null.indicator = ' + '\n')
        else:
            out_file.write('right.null.indicator = \\' + param['NullIndicator'] + '\n')
        out_file.write('right.use.bin = false' + '\n')

        out_file.write('right.format = text' + '\n')

        #get ignore columns from schema
        for col in param['Schema']:
            if col['Name'] == 'dw_ins_date':
                self.conf.ignore_cols += str(col['Index']) + ',' 
        #if len(self.conf.ignore_cols) != 0:
        if self.conf.ignore_cols.endswith(','):
            self.conf.ignore_cols = self.conf.ignore_cols[:-1]

        print self.conf.ignore_cols
        self.conf.schema = left_schema
        out_file.write('schema = ' + self.conf.schema + '\n')
        outDiff = self.conf.left_table + '-'
        for i in range(0, len(self.conf.left_part)):
            outDiff += self.conf.left_part[i] + '.'
        outDiff = outDiff[:-1]
        outDiff += '-' + self.conf.right_table + '-'
        for i in range(0, len(self.conf.right_part)):
            outDiff += self.conf.right_part[i] + '.'
        outDiff = outDiff[:-1]
        out_file.write('verify.output.dir = /tmp/verify/output/' + outDiff.replace(":", "%3A") + '\n') #java URI can't have ':'
        out_file.write('verify.job.local.mode.auto = false' + '\n')
        out_file.write('verify.dirty.records.per.map.limit = -1' + '\n')
        out_file.write('verify.dirty.records.per.reduce.limit = -1' + '\n')
        out_file.write('verify.mega.bytes.per.reducer = 2048' + '\n')
        out_file.write('ignore.columns = ' + self.conf.ignore_cols + '\n')

        out_file.flush()
        out_file.close()
        print 'get verify config OK: ' + self.conf.out_file

    def run_verify(self):

        print 'hello###############run_verify'
        hadoop = self.conf.hadoop
        hivejar = self.conf.verifier + "hive-exec-0.7.0.jar"
        verifyjar = self.conf.verifier + "verifier.jar"
        cmd = "%s  jar -libjars %s %s com.testyun.Verifier %s 2>&1" % (hadoop, hivejar, verifyjar, self.conf.out_file)
        print cmd
        #os.system(cmd)
        verify_retryTimes = 0
        while verify_retryTimes < 3:
            pipe = os.popen(cmd)
            returnStr = pipe.read()
            if 'INFO testyun.Verifier: Job failed' not in returnStr:
                break
            else:
                verify_retryTimes += 1
        print "return value of verify : #####################\n" + returnStr
        DropTable(self.conf.declient, self.conf.left_table)
        DropTable(self.conf.declient, self.conf.right_table)
        if returnStr.find("Verify fail!") != -1:
            Exit_Verify('Verify failed.', False)
        #Exit_Verify('', True)


class VerifyTable:
    def __init__(self, tl, tr):
        print 'hello###############__init__3'
        self.table_left = tl
        self.table_right = tr
        self.left_partition_values = {}
        self.right_partition_values = {}
        self.declient = Config.de_client #config.get("tool", "declient.home")

    def GetPartitionValues(self):
        print 'hello###############GetPartitionValues'
        print 'fuck U!!!'
        cmd = 'me |grep "Local_mysql"'
        res = os.popen(cmd).read().strip()
        if len(res) < len("Local_mysql: "):
            msg = 'get mysql host failed using "me": ' + res
            Exit_Verify(msg, False)
        #wangzhao hack
        #mysql_host = res[13:]
        mysql_host = "10.242.64.1"

        #cmd = 'mysql -uapsara -p123456 -h ' + mysql_host + ' -e "USE data_engine_hivedb_collie; SELECT PARTITIONS.PART_NAME FROM PARTITIONS INNER JOIN TBLS ON PARTITIONS.TBL_ID = TBLS.TBL_ID WHERE TBLS.TBL_NAME = ' + "'" + self.table_left + "';" + '" |grep "="'
        #res = os.popen(cmd).read().strip()
        #print "res1", res
        cmd = self.declient + ' -j -e "show partitions ' + self.table_left + ';"'

        print cmd
        show_part_retryTimes = 0
        while show_part_retryTimes < 3:
            res = json.loads(os.popen(cmd).read())[0]
            if res["Result"] == 'OK':
                res = res['Message']
                break
            elif 'is not a partitioned table' in res['Message']:
                res = res['Message']
                break
            show_part_retryTimes += 1
            sleep(5)
        if show_part_retryTimes==3:
            Exit_Verify("show partitions %s failed." % self.table_left, False)

        print "res2", res
        print res.find("is not a partitioned table")
        if res.find("is not a partitioned table") == -1:
            vec = res.split('\n')
            for i in vec:
                if i != '':
                    tmp = i.split('/')
                    value = ''
                    for j in tmp:
                        value += j.split('=')[1] + ','
                    self.left_partition_values[value[:-1]] = 1
            print 'left partitin value: ' + str(self.left_partition_values.keys())
            
            cmd = cmd.replace(self.table_left, self.table_right)
            print cmd
            #res = os.popen(cmd).read().strip()
            res = json.loads(os.popen(cmd).read())[0]["Message"]
            vec = res.split('\n')
            for i in vec:
                if i != '':
                    tmp = i.split('/')
                    value = ''
                    for j in tmp:
                        value += j.split('=')[1] + ','
                    self.right_partition_values[value[:-1]] = 1
            print 'right partitin value: ' + str(self.right_partition_values.keys())
        #end hack

        self.needCompPart = []

        print "debug info for wangzhao hack: self.left_partition_values.keys():" + str(self.left_partition_values.keys()) + " self.right_partition_values.keys():" + str(self.right_partition_values.keys())

        lk = self.left_partition_values.keys()

        #wangzhao hack for only verify last partition
        '''if self.left_partition_values.has_key("20110917"):
            lk = ["20110917"]
        elif self.left_partition_values.has_key("2011-09-17 00%3A00%3A00"):
            lk = ["2011-09-17 00%3A00%3A00"]
        else:
            tmp_list = ['nodepend_tbidl_auc_item_reply_h', 'nodepend_tbidl_stat_uv_ipv_cate_h']
            if self.table_left in tmp_list or self.table_left in tmp_list:
                lk = ["20110801"]'''
        #wangzhao hack end

        for l in lk:
            if self.right_partition_values.has_key(l):
                self.needCompPart.append(l)
        if len(self.left_partition_values) == 0 and len(self.right_partition_values) != 0 or len(self.left_partition_values) != 0 and len(self.right_partition_values) == 0:
            msg = 'get compared partition value failed'
            Exit_Verify(msg, False)
        print 'comp partition value: ' +str(self.needCompPart)

    def GetCompParam(self):
        print 'hello###############GetCompParam'
        self.verify_param = {}
        if len(self.needCompPart) == 0:
            self.verify_param[self.table_left] = self.table_right
        else:
            for part in self.needCompPart:
                param_left = self.table_left + ';' + part
                param_right = self.table_right + ';' + part
                self.verify_param[param_left] = param_right
        return self.verify_param


def usage():
    print '''\n\
        Usage: \n\
        example:
        ./verify.py table_name table_name <ignore_cols>\n\
        e.g: ./verify.py chai chai_res
             ./verify.py chai chai_res 3,9
    '''


if __name__ == "__main__":
#    if len(sys.argv) != 3 and len(sys.argv) != 4:
#        usage()
#        sys.exit(1)
#    try:
#        verifyConf = None
#        if len(sys.argv) == 3:
#            verifyConf = VerifyConf(sys.argv[1], sys.argv[2])
#        else:
#            verifyConf = VerifyConf(sys.argv[1], sys.argv[2], sys.argv[3])
#        verifier = Verifier(verifyConf)
#        verifier.run()
#    except Exception, e:
#        msg = 'exception: ' + str(e)
#        Exit_Verify(msg, False)

    if len(sys.argv) != 4 and len(sys.argv) != 5:
        msg = 'input param is invalid'
        usage()
        Exit_Verify(msg, False)
    try:
        today = sys.argv[3]
        ignore_cols = None
        if len(sys.argv) == 5:
            ignore_cols = sys.argv[4]
        vt = VerifyTable(sys.argv[1].lower(), sys.argv[2].lower())
        vt.GetPartitionValues()
        verify_pair = vt.GetCompParam()
        print 'Need compare partitions: ' + str(verify_pair)

        keys = verify_pair.keys()
        for key in keys:
            verifyConf = VerifyConf(key, verify_pair[key], today, ignore_cols)
            verifier = Verifier(verifyConf)
            verifier.run()
        Exit_Verify('', True)
    except Exception, e:
        msg = 'unknown exception: ' + str(e)
        Exit_Verify(msg, False)

