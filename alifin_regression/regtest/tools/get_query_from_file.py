import os
import sys

import shutil

_cur_dir = os.path.dirname(__file__)
_regtest_dir = os.path.dirname(_cur_dir)
_regtest_dir = os.path.abspath(_regtest_dir)
sys.path.insert(0, os.path.dirname(_regtest_dir))

from regtest import trans_query
from regtest import common

SPECIAL_CASE = 'odps.idata.useragent'


def preprocess_sql(sql):
    lines = []
    for line in sql.splitlines():
        if SPECIAL_CASE in line:
            head, tail = line.split('=', 1)
            pos = tail.rfind(';')
            lines.append(head + '=' + "'" + tail[:pos] + "'" + tail[pos])
        else:
            lines.append(line)
    return '\n'.join(lines)

            
def get_query_from_file(input_file):
    flatstr = file(input_file).read()
    comps = flatstr.split('|\n')
    queries = []
    extracted_keys = set()
    for i in comps:
        if not i.strip():
            continue
        parts = i.split('|', 2)
        biz_id = parts[1]
        sql = parts[2]
        toks = biz_id.split('_')
        skynet_id = toks[0].strip()
        sql_seq = toks[4].strip()
        key = 'q' + skynet_id + '_' + sql_seq + '.sql'
        if key in extracted_keys:
            # Ignore duplicate biz_id queries
            continue
        extracted_keys.add(key)
        queries.append((key, sql))
    return queries
        
def main():
    created_tables = {}
    input_file = sys.argv[1]
    dest_dir = os.path.abspath(sys.argv[2])
    input_queries = get_query_from_file(input_file)

    def add_project_prefix(name):
        toks = name.split('.')
        assert len(toks) in (1, 2)
        if len(toks) == 1:
            # Add alifin_dw
            return 'alifin_dw.' + name
        else:
            if toks[0] != 'alifin_dw':
                raise trans_query.UnexpectedSQL(name)
            return name

    def add_project_prefix2(name):
        toks = name.split('.')
        assert len(toks) in (1, 2)
        if len(toks) == 2:
            # Remove project prefix, use current project instead
            if toks[0] != 'alifin_dw':
                raise trans_query.UnexpectedSQL(name)
            return toks[1]
        return name

    trans_query.PROCESS_SRC_TABLE = add_project_prefix
    trans_query.PROCESS_DEST_TABLE = add_project_prefix2
    trans_query.dump_to_dir(input_queries, dest_dir, '')


if __name__ == '__main__':
    main()
    
