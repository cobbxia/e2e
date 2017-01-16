import sys
import json


def main():
    report = json.load(file(sys.argv[1]))
    ret = []
    for table in report['table_list']:
        table_name = table['table']
        last_fail_count = table['ay42_rowcount']
        ret.append((table_name, last_fail_count))
    ret.sort()
    for i in ret:
        print i[0], i[1]


if __name__ == '__main__':
    main()
