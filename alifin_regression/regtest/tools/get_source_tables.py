import sys
import re

IDENTIFIER= r'[a-zA-Z][a-zA-Z0-9_]*'
TABLE_NAME = r'%s(?:\.%s)?' % ((IDENTIFIER,) * 2)
FROM_TABLE = r'(?:from\s+(%s))|(?:join\s+(%s))' % ((TABLE_NAME, ) * 2)
PT_FROM_TABLE = re.compile(FROM_TABLE, re.I)

def get_source_tables(sql):
    for matchobj in PT_FROM_TABLE.finditer(sql):
        groups = [i for i in matchobj.groups() if i is not None]
        return groups
    return []

sql = file(sys.argv[1]).read()
for i in get_source_tables(sql):
    print i
