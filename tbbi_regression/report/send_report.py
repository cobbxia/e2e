#!/usr/ali/bin/python
import sys, os

sys.path.append(sys.path[0] + "/../common/")
from config import * 

if len(sys.argv) != 5:
    print 'Usage: python ' + sys.argv[0] + ' <biz_date> <table_list_file> <verify_log_dir> <comment_file>'
    print '       example: ' + 'python send_report.py 20120308 /apsara/wangzhao/20120308/de/need_verify /apsara/wangzhao/parallel_run/logs/verify_tables_output /tmp/error_tables_0308'
    exit(1)

tool_make_report = verify_tool_dir + 'make_report.py'
tool_format_report = verify_tool_dir + 'format_report.py'
tool_send_mail = verify_tool_dir + 'mail.py'
report_file = '/tmp/daily_report_' + sys.argv[1] + '.html'

cmd = 'python ' + tool_make_report + ' ' + ' '.join(sys.argv[1:]) + ' | python ' + tool_format_report + ' > ' + report_file
print cmd
rtn = os.system(cmd) / 256
if rtn != 0:
    exit(rtn)
cmd = 'python ' + tool_send_mail + ' "[' + sys.argv[1]  + '] AY42 and AY39 Parallel Running Daily Verification Report" < ' + report_file
print cmd
rtn = os.system(cmd) / 256
exit(rtn)
