"""
"""
import json
import common

def log_summary2(log_file):
    counters_json = False
    js = None
    for line in  common.lineiter(log_file):
        # if '__COUNTERS_JSON__' in line:
        #     counters_json = True
        #     continue
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
    if js['MismatchRecords'] != 0 or js['Success'] is False:
        ret['status'] = 'FAIL'
    else:
        ret['status'] = 'PASS'
    return ret
