taskidfile="/home/mingchao.xiamc/addtable/taskid/succ_id.txt"
taskids=",".join([line.strip("\n") for line in open(taskidfile,"r")])
cmd='''curl -v -H "Accept: application/json" -H "Content-type: application/json" -X PUT -d  '{"opCode":"RERUN_MULTI_BY_MANUAL","opSEQ":12345,"opUser":"067605","includeTaskIds":[%s]}' "http://10.189.230.125/engine/2.0/tasks/15824675/fixdata" ''' %(taskids)
open("run_BI_START.sh","w").write("%s" % (cmd))
