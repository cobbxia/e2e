import os
import sys

import os
import shutil

_cur_dir = os.path.dirname(__file__)
_regtest_dir = os.path.dirname(_cur_dir)
_regtest_dir = os.path.abspath(_regtest_dir)
sys.path.insert(0, os.path.dirname(_regtest_dir))

from regtest import trans_query
from regtest import common


def get_query_from_dir(input_file):
    files = os.listdir(input_file)
    files = [file(os.path.join(input_file, f)).read() for f in files]
    print 'File num: ', len(files)
    files = set(files)
    print 'Uniq file num:', len(files)
    for i, f in enumerate(files):
        if 'tmp_table_for_select' in f:
            continue
        yield 'q%d.sql' % i, f

def main():
    created_tables = {}
    input_file = sys.argv[1]
    dest_dir = os.path.abspath(sys.argv[2])
    def add_project_prefix(name):
        return 'alifin_odps_dw.' + name
    def add_project_prefix2(name):
        return 'alifin_smoking.' + name
    trans_query.PROCESS_SRC_TABLE = add_project_prefix
    trans_query.PROCESS_DEST_TABLE = add_project_prefix2
    input_queries = get_query_from_dir(input_file)
    trans_query.dump_to_dir(list(input_queries), dest_dir, '')


if __name__ == '__main__':
    main()
    
