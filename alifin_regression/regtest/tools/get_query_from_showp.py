import os
import sys

import shutil

_cur_dir = os.path.dirname(__file__)
_regtest_dir = os.path.dirname(_cur_dir)
_regtest_dir = os.path.abspath(_regtest_dir)
sys.path.insert(0, os.path.dirname(_regtest_dir))

from regtest import trans_query
from regtest import common



def get_query_from_showp(showp_file, dates=None):
    query_lines = []
    in_query = False
    query_id = 0
    date = ''
    visited = set()
    for line in common.lineiter(showp_file):
        if line.startswith('-----') or line.startswith('ID  |') or line.startswith('show p') or line.startswith('Congratulations')
            continue
        
        if line[0:6].isdigit() and line[7:9] == '| ':
            query_id = int(line[0:6])
            if query_lines:
                sql = ' '.join(query_lines) + ';'
                if sql not in visited:
                    visited.add(sql)
                    if dates:
                        if date in dates:
                            yield 'q%s.sql' % query_id, sql
                    else:
                        yield 'q%s.sql' % query_id, sql
                        
            if line[9:13] == 'xyjr':
                # XXX: 6 digits now, may increase in the future
                in_query = True
                query_lines = []
                toks = line.split('|', 5)
                line = toks[-1]
                date = toks[-3].strip()[0:10].replace('-', '')
                query_lines.append(line)
                continue
            else:
                in_query = False

        if in_query:
            query_lines.append(line)

def main():
    created_tables = {}
    input_file = sys.argv[1]
    dest_dir = os.path.abspath(sys.argv[2])
    suffix = sys.argv[3]
    dates = []
    if len(sys.argv) > 4:
        dates= sys.argv[4].split(',')
    input_queries = list(get_query_from_showp(input_file, dates))
    trans_query.dump_to_dir(input_queries, dest_dir, suffix)


if __name__ == '__main__':
    main()
    
