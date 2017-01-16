import sys
import pickle

import parse_logs

stamp = sys.argv[1]

reports = pickle.load(file('cubedb/cubedb_%s' % stamp))

for i in reports:
    if i[3] in (
        #parse_logs.ST_HW_JOB_FAILED, 
        parse_logs.ST_VERIFY_FAILED,
        parse_logs.ST_MOYE_JOB_FAILED,
        #parse_logs.ST_MOYE_COMPILE_ERROR,
        ):
        print '%s.hql_%s' % (i[0], i[1])
