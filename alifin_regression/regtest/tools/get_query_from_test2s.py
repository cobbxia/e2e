import os
import sys

import imp
import shutil

_cur_dir = os.path.dirname(__file__)
_regtest_dir = os.path.dirname(_cur_dir)
_regtest_dir = os.path.abspath(_regtest_dir)
sys.path.insert(0, os.path.dirname(_regtest_dir))


from regtest import trans_query
from regtest import common


def main():
    input_dir = os.path.abspath(sys.argv[1])
    dest_dir = os.path.abspath(sys.argv[2])
    suffix = sys.argv[3]
    input_queries = []
    visited_select_query = set()
    dump_select = '-s' in sys.argv
    
    for f in os.listdir(input_dir):
        f_name = f
        f = os.path.join(input_dir, f)
        qid = f_name.split('.')[0]
        qkey = 'q%s.sql' % qid
        fp = open(f, 'r')
        test2 = fp.read()
        fp.close()
        prefix = 'query = r"""'
        start_pos = test2.find(prefix) + len(prefix)
        if start_pos == -1:
            continue
        end_pos = test2.find('"""', start_pos)
        sql = test2[start_pos: end_pos] + ";"
        if 'tmp_table_for_select_' in sql:
            if not dump_select:
                continue
            key = trans_query.get_tmp_table_key(sql)
            if key in visited_select_query:
                continue
            visited_select_query.add(key)
            input_queries.append((qkey, sql))
            continue
            
        elif 'ops_monitor_dst' in sql:
            continue
        input_queries.append((qkey, sql))
    trans_query.dump_to_dir(input_queries, dest_dir, suffix)


if __name__ == '__main__':
    main()
    
