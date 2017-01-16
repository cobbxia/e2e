import sys

def main():
    from common import ODPSConsole
    odps = ODPSConsole('/apsara/huian/alifin_pre/clt_pre/bin/odpscmd')
    table_list = file(sys.argv[1]).read().splitlines()
    for t in table_list:
        desc = odps.desc_table(t)
        print >>sys.stdout, t + ':' + ','.join([i[0] for i in desc['partition_columns']])
        sys.stdout.flush()

if __name__ == '__main__':
    main()
