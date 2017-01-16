
import re

settings = []


add_p = re.compile('^add ', re.IGNORECASE)
set_p = re.compile('^set ', re.IGNORECASE)
tempfunc_p = re.compile('^create temporary function', re.IGNORECASE)
ct_p = re.compile('^create table', re.IGNORECASE)
asselect_p = re.compile(' as select\s', re.IGNORECASE)

def is_setting(stmt):
    if add_p.match(stmt) or set_p.match(stmt):
        return True
    if tempfunc_p.match(stmt):
        return True
    #for word in ['insert overwrite', 'from']:
        #if not word.startswith(stmt):
            #assert 'unexpected case:', stmt # we shouldn't have things like drop, alter etc.
    return False

def store_settings(stmt):
    settings.append(stmt)

def flush_hql(stmt, out):
    for i in settings:
        print >> out, i
    print >> out, stmt

def flush_hql_only(stmt, out):
    print >> out, stmt[:-1] #throw the ';' away

spaces = re.compile('\s+')

if __name__ == '__main__':
    import sys
    in_name = sys.argv[1]
    stmt = ''
    i = 0
    for line in file(in_name).readlines():
        line = spaces.sub(' ', line.strip())
        if line.endswith(';'):
            stmt += line
            if ct_p.match(stmt):
                if not asselect_p.search(stmt):
                    stmt = '' # throw create table ddl away
                    continue

            #print stmt
            if is_setting(stmt):
                store_settings(stmt)
            else:
                out = file('moye_hql/' + in_name + '_' + str(i), 'w')
                flush_hql(stmt, out)
                out.close()
                out = file('hql_only/' + in_name + '_' + str(i), 'w')
                flush_hql_only(stmt, out)
                out.close()
                i += 1
            stmt = ''
        elif line: #needs to have something in line
            stmt += line + '\n'
