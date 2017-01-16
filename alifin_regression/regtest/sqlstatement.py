import sys
import re

p_table_part = re.compile(r'insert\s+overwrite\s+table\s+(\S+)(\s+partition\s*\((.*?)\)\s*)?')

def extract(sql):
    sql = sql.lower()
    matches = p_table_part.findall(sql)
    for m in matches:
        if len(m) < 2:
            yield m[0]
        else:
            yield '%s:%s' % (m[0], m[2])

def main():
    sql = file(sys.argv[1]).read()
    for line in extract(sql):
        print line

if __name__ == '__main__':
    main()


