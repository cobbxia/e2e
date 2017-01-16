import subprocess
import time
import sys
import os
sys.path.append(sys.path[0] + "/../common/")
import config as RegConf

if __name__ == "__main__":


    if len(sys.argv) != 3:
        print 'python verify_1table.py tableName biz_date'
        sys.exit(-1)

    tableName = sys.argv[1]
    today = sys.argv[2]
       
    basetableName = 'basetable%s_%s' % (RegConf.snapshot_date, tableName)
    verify_output_dir = RegConf.verify_log_dir + today + '/'
    if not os.path.exists(verify_output_dir):
        os.system('mkdir ' + verify_output_dir)
    out_file = verify_output_dir + tableName + '.' + basetableName + '.log'
    print out_file
    '''if tableName in RegConf.seq_file_list:
        cmd = 'python ' + RegConf.verify_tool_path + ' ' + tableName + ' ' + basetableName + ' ' + today + ' 0 > ' + out_file
    else:'''
    cmd = 'python ' + RegConf.verify_tool_path + ' ' + tableName + ' ' + basetableName + ' ' + today + ' > ' + out_file
    print cmd
    p = subprocess.Popen(cmd,shell=True,close_fds=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)


    #wait for end
    while True:
        cmd = 'tail -1 ' + out_file + ' |grep "yongfeng.chai say: Execute verify_table.py end."'
        res = os.popen(cmd).read()
        if res.find('yongfeng.chai say') == -1:
            print "Still Running... ..."
            time.sleep(10)
            continue
        print "Verify end."
        break

    stdoutdata,stderrdata = p.communicate()
    print "verify script's stdout:"
    print stdoutdata
    print "verify script's stderr:"
    print stderrdata

    #get result and adjust whether verify success
    cmd = 'cat ' + out_file
    res = os.popen(cmd).read()
    if res.find('Verify fail!') == -1 and res.find('UNFORTUNATELY') == -1 and (res.find('Verify success!') != -1 or res.find('Two paths are the same') != -1):
        print "result: Success!"
        exit(0)
    else: #verify fail
        print "result: Fail!"
        exit(-1)
