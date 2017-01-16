import os
import sys
import pickle

from common import lineiter

basedir = '/apsarabak/wayne.wuw/regtest'
tmpdir = os.path.join(os.path.dirname(basedir), 'tmp')
cubedb_dir = os.path.join(basedir, 'cubedb')

ST_VERIFY_SEMI_PASSED = 'verify passed: invalid string'
ST_VERIFY_PASSED = 'verify passed'
ST_VERIFY_FAILED = 'verify failed'
ST_MOYE_JOB_SUCCESS = 'moye job success'
ST_MOYE_JOB_FAILED = 'moye job failed'
ST_MOYE_JOB_CANCELED = 'moye job canceled'
ST_MOYE_GENC_ERROR = 'genc error'
ST_MOYE_GEN3_ERROR = 'gen3 error'
ST_MOYE_COMPILE_ERROR = 'compile error'
ST_HW_JOB_FAILED = 'hadoop wrapper job failed'
ST_HW_JOB_SUCCESS = 'hadoop wrapper job success'
ST_UNKNOWN = 'unknown'

JOB_SUCCESS = 0
JOB_FAILED = 1
JOB_CANCELED = 2
CYTHON_COMPILE_ERROR = 3
CODEGEN_ERROR = 4
SUBMIT_ERROR = 5
GEN3_ERROR = 6

ST_TABLE = {
    JOB_SUCCESS: ST_MOYE_JOB_SUCCESS,
    JOB_FAILED: ST_MOYE_JOB_FAILED,
    JOB_CANCELED: ST_MOYE_JOB_CANCELED,
    CODEGEN_ERROR: ST_MOYE_GENC_ERROR,
    SUBMIT_ERROR: ST_MOYE_COMPILE_ERROR,
    GEN3_ERROR: ST_MOYE_GEN3_ERROR,
}


def is_passed(st):
    if st in (ST_VERIFY_SEMI_PASSED, ST_VERIFY_PASSED):
        return st


def parse_hwlog(fpath):
    status = ST_UNKNOWN
    lines = lineiter(fpath)
    if 'Time taken' in lines[-2]:
        status = ST_HW_JOB_SUCCESS
    else:
        status = ST_HW_JOB_FAILED
    return status


def parse_log(fpath):
    status = ST_UNKNOWN
    for line in lineiter(fpath):
        if 'MapRedTask: Job Return Status:' in line:
            int_st = int(line[line.rfind(']')+1:])
            status = ST_TABLE[int_st]
    return status


def parse_vlog(fpath):
    status = ST_UNKNOWN
    for line in lineiter(fpath):
        if 'INVALID_STRING' in line:
            status = ST_VERIFY_SEMI_PASSED
            break
        elif 'Verify Success' in line:
            status = ST_VERIFY_PASSED
            break
        else:
            status = ST_VERIFY_FAILED
    return status



def parse(hwlog_dir, log_dir, vlog_dir):
    reports = []

    # 1. load all tasks
    tasks = pickle.load(file(os.path.join(cubedb_dir, 'cubedb_tasks.pickle')))
    reasons = pickle.load(file(os.path.join(cubedb_dir, 'cubedb_reason_table.pick')))

    for key, tbparts in tasks.items():
        taskid = key[0]
        seqid = key[1]
        log_filename = '%s.hql_%s.log' % key

        # hw
        hw_log_path = os.path.join(hwlog_dir, log_filename)
        hw_status = parse_hwlog(hw_log_path)
        if hw_status != ST_HW_JOB_SUCCESS:
            for i in tbparts:
                reports.append((taskid, seqid, i, hw_status, ''))
            continue
        
        # moye
        moye_log_path = os.path.join(log_dir, log_filename)
        moye_status = parse_log(moye_log_path)
        if moye_status != ST_MOYE_JOB_SUCCESS:
            for i in tbparts:
                reports.append((taskid, seqid, i, moye_status, ''))
            continue

        # now both hadoop_wrapper and moye have done the job successfully
        for i in tbparts:
            vlog_fpath = os.path.join(vlog_dir, 
                                      '%s.hql_%s.%s.log' % (taskid, seqid, i))
            v_status = parse_vlog(vlog_fpath)
            reason = ''
            if v_status not in (ST_VERIFY_PASSED, ST_VERIFY_SEMI_PASSED):
                # get the reason
                rkey = (taskid, i)
                # the value of reasons is a two-element tuple
                reason = reasons.get(rkey, ('', reason))[1]
                if reason:
                    v_status = 'has reason'
            reports.append((taskid, seqid, i, v_status, reason))
    return reports


if __name__ == '__main__':
    stamp = sys.argv[1]
    reports = parse(os.path.join(tmpdir, 'hwlogs_%s' % stamp),
                    os.path.join(tmpdir, 'logs_%s' % stamp), 
                    os.path.join(tmpdir, 'vlogs_%s' % stamp)
                    )
    reports.sort(key=lambda x: (x[0], x[1]))

    total_num = len(reports)
    passed_num = 0
    has_reason = 0
    for i in reports:
        if is_passed(i[3]):
            passed_num += 1
        if i[4]:
            has_reason += 1
    
    print '= Cube Regression Report ='
    print ''
    print '* Total num: ', total_num
    print '* Passed num: ', passed_num
    print '* Has reason: ', has_reason
    print ''
    print '|| task id || no. || tb/part || status || notes ||'
    for i in reports:
        print '|| ' + ' || '.join(i) + ' ||'

    pickle.dump(reports, open(os.path.join(cubedb_dir, 'cubedb_%s' % stamp), 'w'))
    
