#! /usr/ali/bin/python

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
import math
from time import localtime, strftime
from datetime import date, datetime
import subprocess
from subprocess import *
import config as Config
from multithreadwrapper import WorkerThread

gMaxJobNum = 20
gInterval = 300
gCurrPath = os.path.dirname(os.path.abspath(__file__))

timeStamp = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime(time.time()))
logFile = Config.verify_summary + '_' + timeStamp

#WriteLogFile = True
#if WriteLogFile:
#    g_LOG = file(logFile, 'w')

cmd = ''
if not os.path.isdir(Config.workingPath):
    cmd += 'mkdir -p ' + Config.workingPath + ';'
if not os.path.isdir(Config.verify_case_log_path):
    cmd = 'mkdir -p ' + Config.verify_case_log_path + ';'
if not os.path.isdir(Config.verify_conf_dir):
    cmd = 'mkdir -p ' + Config.verify_conf_dir + ';'
if cmd != '':
    os.system(cmd)

def WriteLog(log):
    output = '[' + time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))+ ']' + log 
    print output
#    if WriteLogFile:
#        g_LOG.write(output + '\n')
#        g_LOG.flush()

def RunVerify(table_list):      #map<leftTable, rightTable>
    counter = 0
    execDict = {} #like {process_obj : [out_file, table_l, table_r]}
    passDict = {}  #like {table_l : table_r} 
    failDict = {}  #like {table_l : table_r}
    if len(table_list) != 0:
        print 'Verify List: ' + str(table_list)

    while (len(table_list) != 0) or (len(execDict) != 0):
        while (len(execDict) < gMaxJobNum) and (len(table_list) != 0):
            tableLeft = table_list.keys()[0]
            out_file = Config.verify_case_log_path + tableLeft + '.' + table_list[tableLeft] + '.log'
            cmd = 'python ' + gCurrPath + '/verify_table_with_partition.py ' + tableLeft + ' ' + table_list[tableLeft] + ' 1>' + out_file + ' 2>&1'
            WriteLog('Start verify: ' + cmd)
            pobj = subprocess.Popen(cmd, shell = True)
            execDict[pobj] = [out_file, tableLeft, table_list[tableLeft]]
            del table_list[tableLeft]
            #WriteLog('Pass: ' + str(len(passDict)) + ', Fail: ' + str(len(failDict)) + ', Running: ' + str(len(execDict)) + ', Remaining: ' + str(len(table_list)))
            time.sleep(1)

        keys = execDict.keys()
        for pobj in keys:
            time.sleep(1)
            res = pobj.poll()
            if res == None:     #verify still running
                counter += 1
                if counter >= gInterval:
                    WriteLog('Pass: ' + str(len(passDict)) + ', Fail: ' + str(len(failDict)) + ', Running: ' + str(len(execDict)) + ', Remaining: ' + str(len(table_list)) + ', ExecList: ' + str(execDict.values()))
                    counter = 0
                continue

            elif res < 0:               #verify meet exception
                WriteLog('Verify failed , meet exception: ' + str(execDict[pobj]))
                failDict[execDict[pobj][1]] = execDict[pobj][2]
                del execDict[pobj]
            else:                       #verify end, success or failed
                cmd = 'cat ' + execDict[pobj][0]
                res = os.popen(cmd).read()
                if res.find('Verify fail!') != -1 or (res.find('UNFORTUNATELY') != -1) or len(res) == 0:    #verify failed
                    WriteLog('Verify failed : ' + str(execDict[pobj]))
                    failDict[execDict[pobj][1]] = execDict[pobj][2]
                else:
                    WriteLog('Verify success: ' + str(execDict[pobj]))
                    passDict[execDict[pobj][1]] = execDict[pobj][2]
                del execDict[pobj]

    print 20 * '*' + 'END VERIFY, PASS: ' + str(len(passDict)) + ', FAIL: ' + str(len(failDict)) + 20 * '*'
    return (passDict, failDict)

def VerifyTableList(listFile):
    config_file = sys.argv[1]
    fp = file(listFile, 'r')
    lines = fp.readlines()
    fp.close()
    table_list = {}
    for line in lines:
        if not line.startswith('#'):
            vec = line.strip().split(':')
            if len(vec) == 2:
                table_list[vec[0]] = vec[1]
            else:
                WriteLog('Invalid input ' + line.strip())
        else:
            WriteLog('Skip ' + line.strip())
    if os.path.isfile('/tmp/already_verify'):
        fp = file('/tmp/already_verify', 'r')
        lines = fp.readlines()
        fp.close()
        for line in lines:
            vec = line.strip().split(':')
            if table_list.has_key(vec[0]) and table_list[vec[0]] == vec[1]:
                del table_list[vec[0]]
                WriteLog('Remove ' + line.strip())

    return RunVerify(table_list)

def TestFunc(content):
    print "I'm content", content
    time.sleep(5)
    return (content, {})


def CheckStateThenVerify(bizFile, stateFile):
    fp = file(bizFile, 'r')
    lines = fp.readlines()
    fp.close()
    biz_conf = {} #{id : tblName}
    tbl2ID = {}   #{tblName, id}
    for line in lines:
        vec = line.strip().split(':')
        if len(vec) == 2:
            biz_conf[vec[0]] = vec[1]
            tbl2ID[vec[1]] = vec[0]
        else:
            print 'invaild biz config:', line
            exit(-1)
    
    passDict = {}; failDict = {}  #{id : tblName}
    readyList = []  #[id1, id2, id3......]
    execDict = {} #{threadObj : [id1, id2......]}
    counter = 0
    while len(passDict) + len(failDict) < len(bizFile):
        fp = file(stateFile, 'r')
        lines = fp.readlines()
        fp.close()
        for line in lines:
            vec = line.strip().split(':')
            if len(vec) == 2:
                if vec[1] == '0' and not vec[0] in readyList:
                    readyList.append(vec[0])
            else:
                print 'invaild biz config:', line
                exit(-1)

        tmpDict = {} # {tblName : tblName}
        execIds = [] #[id1, id2......]
        for id in readyList:
            if not id in biz_conf.keys():
                print 'biz id is not expected', id
                exit(-1)
            if (not id in passDict.keys()) and (not id in failDict.keys()): #it's not finished
                if not (True in [id in ids for ids in execDict.values()]):  #it's not running
                    tblName = biz_conf[id]
                    tmpDict[biz_conf[id]] = biz_conf[id]
                    execIds.append(id)

        if len(tmpDict) != 0:   #someone is ready for verify
            t = WorkerThread(RunVerify, [tmpDict], RunVerify.__name__) #a new thread for verify them
            #t = WorkerThread(TestFunc, [tmpDict], RunVerify.__name__)
            t.start()
            execDict[t] = execIds

        isUpdateState = False
        for t in execDict.keys():
            if t.isAlive(): #thread is not finish
                continue
            isUpdateState = True
            (passTbls, failTbls) = t.getResult() #thread finished
            for passKey in passTbls.keys():
                passDict[tbl2ID[passKey]] = passKey
            for failKey in failTbls.keys():
                failDict[tbl2ID[failKey]] = failKey
            del execDict[t]
        if isUpdateState or counter >= gInterval:
            print 'PASS:', passDict, '\nFAIL:', failDict, '\nEXEC:', execDict.values()
            if counter >= gInterval:
                counter = 0
        else:
            counter += 5
        time.sleep(5)
    return (passDict, failDict)


if __name__ == "__main__":
    if len(sys.argv) != 2 and len(sys.argv) != 3:
        print 'python verify_table_list.py table_list_file\n\t or python verify_table_list.py biz_conf state_conf'
        sys.exit(-1)

    if len(sys.argv) == 2:
        config_file = sys.argv[1]
        (passDict, failDict) = VerifyTableList(config_file)
        sys.exit(0)

    bizFile = sys.argv[1]
    stateFile = sys.argv[2]
    (passDict, failDict) = CheckStateThenVerify(bizFile, stateFile)
