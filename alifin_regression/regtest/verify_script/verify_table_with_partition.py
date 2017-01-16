#! /usr/ali/bin/python

import os
import re
import time
import datetime
import string 
import random
import logging
import threading
import traceback
import getopt, sys 
import logging.handlers
import copy
#import ConfigParser
import math
from time import localtime, strftime, sleep
from datetime import date, datetime
import subprocess
from subprocess import *
import simplejson as json
import config as Config

import common

File_Type = {0 : 'text', 1 : 'cfile', 2 : 'rcfile', 3 : 'seqfile'}
Conf_Out_Dir = Config.verify_conf_dir 
RetryTimes = 3 

TimeFormat = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'

cmd = ''
if not os.path.isdir(Config.workingPath):
    cmd += 'mkdir -p ' + Config.workingPath + ';' 
if not os.path.isdir(Config.verify_case_log_path):
    print cmd
    cmd = 'mkdir -p ' + Config.verify_case_log_path + ';' 
if not os.path.isdir(Config.verify_conf_dir):
    cmd = 'mkdir -p ' + Config.verify_conf_dir + ';' 
if cmd != '': 
    os.system(cmd)

def Exit_Verify(msg, type):
    ret_msg = 'yongfeng.chai say: Execute verify_table.py end. \n'
    ret_msg += msg
    if type:
        print ret_msg
        exit(0)
    else:
        ret_msg += '. UNFORTUNATELY!!'
        print ret_msg
        exit(-1)

def DescTable(declient, table_name):
    cmd = declient + ' -j -e "desc ' + table_name + ';"'
    print cmd
    for i in range(0, RetryTimes):
        out = os.popen(cmd).read()
        try:
            param = json.loads(json.loads(out)[0]['Message'])
            return param
        except Exception, e:
            if i == RetryTimes - 1:
                msg = "can't desc table " + table_name + '(' + cmd + ')' + str(e) + str(traceback.format_exc())
                Exit_Verify(msg, False)
            else:
                time.sleep(5)

@common.retry(RetryTimes)
def IsPartitionedTable(declient, table_name):
    return declient.is_partitioned_table(table_name)


class VerifyConf(object):

    def __init__(self, table_l, table_r, ignore_cols = None):

        self.declient_l = Config.de_client_1
        self.declient_r = Config.de_client_2
        self.hadoop = Config.hadoop_home 
        self.verifier = Config.verifier_path 
        self.pu = Config.pu 

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

        #self.today = today
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
            self.out_file = '%s%s-%s.conf' % (Conf_Out_Dir, self.left_table, self.right_table)
        else:
            self.out_file = '%s%s%s-%s%s.conf' % (Conf_Out_Dir, self.left_table, str(self.left_part), self.right_table, str(self.right_part))
            self.out_file = self.out_file.replace("'", '').replace('[', '.').replace(']', '').replace(' ', '_').replace(':', '_').replace(',', '')

class Verifier:

    STORAGE_TYPE = 'cfile'
    NULL_INDICATOR = r'\\N'
    DELIM = '\u0001'
    
    TYPE_SHORT = {
        'string': 's',
        'double': 'd',
        'bigint': 'l',
        'boolean': 'b',
        'datetime': 't',
        }

    def __init__(self, conf):

        self.conf = conf
        self.verify_param = None
        self.odps_l = common.ODPSConsole(self.conf.declient_l)
        self.odps_r = common.ODPSConsole(self.conf.declient_r)

    def run(self):
        try:
            self.gen_verify_param()
            self.run_verify()
        except Exception, e:
            msg = 'unknown exception: ' + str(e) + str(traceback.format_exc())
            Exit_Verify(msg, False)

    def _get_schema_short(self, odps, table):
        desc_result = odps.desc_table(table)
        left_schema = ''
        for col in desc_result['schema']:
            if col in desc_result['partition_columns']:
                continue
            try:
                left_schema += self.TYPE_SHORT[col[1]]
            except KeyError:
                msg = 'unknown column type for left schema: ' + col[0] + '[' + col[1] + ']'
                Exit_Verify(msg, False)
        return [c[0] for c in desc_result['partition_columns']], left_schema

    def gen_verify_param(self):
        out_file = file(self.conf.out_file, 'w')
        left_part_cols, left_schema = self._get_schema_short(self.odps_l, self.conf.left_table)
        l_location = self.odps_l.get_table_location(self.conf.left_table)
        if l_location is None:
            Exit_Verify('Cannot get location of ' + self.conf.left_table, False)
        self.conf.left_dir = l_location + '/'
        if len(self.conf.left_part) != 0:
            for i in range(len(self.conf.left_part)):
                self.conf.left_dir += left_part_cols[i] + '=' + self.conf.left_part[i] + '/'
        out_file.write('left.schema = ' + left_schema + '\n')
        out_file.write('left.dir = ' + self.conf.left_dir.replace(' ', '\\ ') + '\n')
        out_file.write('left.field.delim = ' + self.DELIM + '\n')
        out_file.write('left.null.indicator = ' + self.NULL_INDICATOR + '\n')
        out_file.write('left.use.bin = false' + '\n')
        out_file.write('left.format = ' + self.STORAGE_TYPE + '\n')

        right_part_cols, right_schema = self._get_schema_short(self.odps_r, self.conf.right_table)
        #self.conf.right_dir = param['Location']
        r_location = self.odps_r.get_table_location(self.conf.right_table)
        if r_location is None:
            Exit_Verify('Cannot get location of ' + self.conf.right_table, False)
        self.conf.right_dir = r_location + '/'
        if len(self.conf.right_part) != 0:
            for i in range(len(self.conf.right_part)):
                self.conf.right_dir += right_part_cols[i] + '=' + self.conf.right_part[i] + '/'
        out_file.write('right.dir = ' + self.conf.right_dir.replace(' ', '\\ ') + '\n')
        out_file.write('right.schema = ' + right_schema+ '\n')
        out_file.write('right.field.delim = ' + self.DELIM + '\n')
        out_file.write('right.null.indicator = ' + self.NULL_INDICATOR + '\n')
        out_file.write('right.use.bin = false' + '\n')
        out_file.write('right.format = ' + self.STORAGE_TYPE + '\n')

        #get ignore columns from schema
        if self.conf.ignore_cols.endswith(','):
            self.conf.ignore_cols = self.conf.ignore_cols[:-1]
        for i, col in enumerate(self.odps_l.desc_table(self.conf.left_table)['schema']):
            if col[0] == 'dw_ins_date':
                if self.conf.ignore_cols == '':
                    self.conf.ignore_cols += str(i)
                else:
                    self.conf.ignore_cols += ',' + str(i)
        
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
        out_file.write('verify.mega.bytes.per.reducer = 256' + '\n')
        out_file.write('ignore.columns = ' + self.conf.ignore_cols + '\n')
        out_file.write('verify.convert.datetime.to.string = true' + '\n')
        out_file.write('schema = ' + left_schema + '\n')

        out_file.flush()
        out_file.close()
        print 'get verify config OK: ' + self.conf.out_file

    def run_verify(self):

        hadoop = self.conf.hadoop
        hivejar = self.conf.verifier + "hive-exec-0.7.0.jar"
        verifyjar = self.conf.verifier + "verifier.jar"
        cmd = "%s jar -libjars %s %s com.testyun.Verifier %s 2>&1" % (hadoop, hivejar, verifyjar, self.conf.out_file)
        print cmd

        verify_retryTimes = 0
        while verify_retryTimes < 3:
            pipe = os.popen(cmd)
            returnStr = pipe.read()
            if 'INFO Verifier: Job failed' not in returnStr:
                break
            else:
                verify_retryTimes += 1

        print "return value of verify : #####################\n" + returnStr
        if returnStr.find("Verify fail!") != -1:
            Exit_Verify('Verify failed.', False)


class VerifyTable:
    def __init__(self, tl, tr):
        self.table_left = tl
        self.table_right = tr
        self.left_part_values = [] 
        self.right_part_values = []

        self.declient_l = Config.de_client_1
        self.declient_r = Config.de_client_2
        self.odps_l = common.ODPSConsole(self.declient_l)
        self.odps_r = common.ODPSConsole(self.declient_r)

    def _GetPartitionValues(self, declient, table):
        if IsPartitionedTable(declient, table):
            part_values = declient.get_partition_values(self.table_left)
            part_values.sort()
            return part_values
        return None

    def GetPartitionValues(self):
        # determine whether it's a partitioned table
        self.left_part_values = self._GetPartitionValues(self.odps_l, self.table_left)
        print 'left partition value: ' + str(self.left_part_values)
        self.right_part_values = self._GetPartitionValues(self.odps_r, self.table_right)
        print 'right partitipn value: ' + str(self.right_part_values)
        return self.GetCompParam()

    def GetCompParam(self):
        self.verify_param = {}
        if self.left_part_values is None and self.right_part_values is None:
            #two tables are not partitoned
            self.verify_param[self.table_left] = self.table_right
            return self.verify_param
        if None in (self.left_part_values, self.right_part_values):
            Exit_verify('One table is not partitioned!', False)
            
        if len(self.left_part_values) != len(self.right_part_values):
            Exit_Verify("left part is not equal to right part", False)

        if len(self.left_part_values) >=3 and len(self.right_part_values) >=3:
            l_vals = self.left_part_values[-3:]
            r_vals = self.right_part_values[-3:]
            for i in range(3):
                if len(l_vals[i]) and l_vals[i][0].startswith('2999'):
                    continue
                print l_vals[i]
                param = ''
                for v in l_vals[i]:
                    param += v + ','
                self.verify_param[self.table_left + ';'+ param[:-1]] = self.table_right + ';' + param[:-1]
            return self.verify_param
        elif len(self.left_part_values) == 0:
            print 'Two paths are the same. Do not verify.'
            exit(0)

        for l in self.left_part_values:
            if l in self.right_part_values:
                param  = ''
                for v in l:
                    param += v + ','
                self.verify_param[self.table_left + ';'+ param[:-1]] = self.table_right + ';' + param[:-1]
            else:
                Exit_Verify("left part is not equal to right part", False)
        print 'verify param: ', str(self.verify_param)
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

    if len(sys.argv) != 3 and len(sys.argv) != 4:
        msg = 'input param is invalid'
        usage()
        Exit_Verify(msg, False)
    try:
        ignore_cols = None
        if len(sys.argv) == 4:
            ignore_cols = sys.argv[3]
        verify_pair = {}
        if sys.argv[1].find(';') != -1 or sys.argv[2].find(';') != -1:
            verify_pair[sys.argv[1]] = sys.argv[2]
        else:
            vt = VerifyTable(sys.argv[1], sys.argv[2])
            verify_pair = vt.GetPartitionValues()
        #verify_pair[sys.argv[1]] = sys.argv[2]
        keys = verify_pair.keys()
        for key in keys:
            verifyConf = VerifyConf(key, verify_pair[key], ignore_cols)
            verifier = Verifier(verifyConf)
            verifier.run()
        Exit_Verify('', True)
    except Exception, e:
        msg = 'unknown exception: ' + str(e) + str(traceback.format_exc())
        Exit_Verify(msg, False)

