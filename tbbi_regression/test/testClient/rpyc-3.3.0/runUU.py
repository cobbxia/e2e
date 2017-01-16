import os,subprocess,json
def do_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return p.returncode, out, err 


#oracle sql:update  phoenix_task_inst set status=6  where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0 and status=5;
def runUU(infile):
    taskids=",".join([line.strip("\n") for line in open(infile,"r")])
    cmd='''curl -v -H "Accept: application/json" -H "Content-type: application/json" -X PUT -d  '{"opCode":"RERUN_MULTI_BY_MANUAL","opSEQ":12345,"opUser":"067605","includeTaskIds":[%s]}' "http://10.189.230.125/engine/2.0/tasks/15824675/fixdata"  ''' % (taskids)
    r=do_cmd(cmd)
    if r[0]!=0:
        print(r[2])
    else:
        print(r[1])
        print(r[2])


runUU("./runUU.txt")
