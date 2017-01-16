import os
import sys
import re
import shutil

import common

IDENTIFIER= r'[a-zA-Z][a-zA-Z0-9_]*'
TABLE_NAME = r'%s(?:\.%s)?' % ((IDENTIFIER,) * 2)

INSERT = r'insert\s+((?:overwrite)|(?:into))\s+table\s+(%s)' % TABLE_NAME
CTAS = r'create\s+table\s+([a-zA-Z0-9_]+)\s+as'
FROM_TABLE = r'(?:^(?:(?:from)|(?:join))|\s+(?:(?:from)|(?:join)))\s+(%s)' % TABLE_NAME

PT_INSERT = re.compile(INSERT, re.I)
PT_CTAS = re.compile(CTAS, re.I)
PT_FROM_TABLE = re.compile(FROM_TABLE, re.I)

PT_SET_STMT = re.compile(r'\s*set\s+[a-zA-Z.]+\s*=\w+\s*;', re.I)
# XXX
INSERT2 = r''.join((r'insert',
                    r'\s+',
                    r'(?:(?:overwrite)|(?:into))\s+',
                    r'table\s+',
                    '(' + TABLE_NAME + ')\s+',
                    r'(?:partition\s*\((.*?)\)\s+)?',
                    ))
PT_INSERT2 = re.compile(INSERT2, re.I)

# XXX: Use global variale passing around configurations
SUFFIX = None
PROCESS_DEST_TABLE = lambda x: x + '_' + SUFFIX if SUFFIX else x
PROCESS_SRC_TABLE = lambda x: x
REPLACE_INTO = False


class UnexpectedSQL(Exception):
    pass

def get_created_table_partitions(sql):
    real_sql = sql.strip()
    sql = real_sql.lower()
    if sql.startswith('create'):
        toks = sql.split()
        if toks[1] != 'table':
            raise UnexpectedSQL(sql)
        if toks[2] == 'if':
            return set([(toks[5], None), ])
        else:
            return set([(toks[2], None), ])
    elif sql.startswith('insert') or sql.startswith('from'):
        results = PT_INSERT2.findall(real_sql)
        ret = set()
        for r in results:
            ret.add((r[0], normalize_partspec(r[1])))
        return ret
    else:
        print sql, sql.startswith('insert'), '|'+sql[0:11]+'|', len(sql[:11])
        raise UnexpectedSQL(sql)


def _replace_group(m, group_id, new_str):
    start, end = m.span(group_id)
    start -= m.start()
    end -= m.start()
    result = m.group(0)
    result = result[:start] + new_str + result[end:]
    return result


def extract_settings(sql):
    statements = sql.split(';\n')
    settings = []
    query = None
    for q in statements:
        if q.strip().lower().startswith('set '):
            settings.append(q + ';')
        else:
            if query is None:
                query = q
            else:
                assert False, str(q)+str(query)
    assert query is not None
    return settings, query
    

def _part_tokenize(spec):
    in_str = False
    str_chars = []
    for c in spec:
        if in_str:
            if c == "'":
                in_str = False
                yield ''.join(str_chars)
                str_chars = []
            else:
                str_chars.append(c)
        else:
            if c == "'":
                in_str = True
            elif c in (' ', '\t'):
                continue
            elif c in ('=', ','):
                if str_chars:
                    yield ''.join(str_chars)
                str_chars = []
                yield c
            else:
                str_chars.append(c)
    if str_chars:
        yield ''.join(str_chars)


def normalize_partspec(spec):
    '''                                                                                                                                                 
    DT does **not** support partspec like this:
      "a = 'abc', b=1"  
    It should be normalized to:
      "a='abc',b='1'"
    '''
    from collections import deque
    toks = deque(_part_tokenize(spec))
    parts = []
    # Malformed spec will simply fail, which is fine to be prevented
    # from unexpected behavior. 
    while toks:
        partname = toks.popleft()
        if not toks:
            # dynamic partition
            # return partition name
            parts.append(partname)
            break
        peek = toks.popleft()
        if peek == '=':
            partval = toks.popleft()
            parts.append("%s='%s'" % (partname, partval))
            if toks:
                assert toks.popleft() == ','
        elif peek == ',':
            pass
        else:
            toks.appendleft(peek)
    return ','.join(parts)


def get_partition_vals(partspec):
    ret = []
    if not partspec:
        return ret
    parts = partspec.split(',')
    for part in parts:
        toks = part.split('=')
        if len(toks) < 2:
            continue
        ret.append(toks[1][1:-1])
    return ret


def form_cases(input_queries, created_tables, source_tables={}, error_trans={}):
    queries = {}
    count = 0
    for entry in input_queries:
        if isinstance(entry, tuple):
            qkey = entry[0]
            sql = entry[1]
        else:
            qkey = 'q%s.sql' % count
            count += 1
            sql = entry
        # # Retrieve set options.
        # sets, sql = extract_settings(sql)
        # qsql = sql
        # queries[qkey] = '\n'.join(sets) + '\n' + qsql
        suffix = qkey.split('.')[0]
        new_sqls = ['-- Table schema Initialization']
        def replace_tab_name(m):
            """Also replace insert into.
            """
            kv = m.groups()
            result = m.group(0)
            toks = result.split()
            if REPLACE_INTO is True and toks[1] == 'into':
                toks[1] = 'overwrite'
            assert toks[3] == kv[0]
            if kv[1] == None:
                kv = (kv[0], '')
            else:
                kv = (kv[0], normalize_partspec(kv[1]))
            new_tabn = PROCESS_DEST_TABLE(toks[3])
            new_tabn += '_' + suffix

            # Initialize table schema
            new_sqls.append('drop table if exists %s;' % new_tabn)
            new_sqls.append('create table %s like %s;' % (new_tabn, PROCESS_SRC_TABLE(toks[3])))
            created_tables.setdefault(qkey, set()).add((new_tabn, kv[1]))
            toks[3] = new_tabn
            return ' '.join(toks) + ' '

        def replace_src_name(m):
            result = m.group(0)
            toks = result.split()
            new_tabn = PROCESS_SRC_TABLE(toks[1])
            source_tables.setdefault(qkey, set()).add(new_tabn)
            toks[1] = new_tabn
            return ' ' + ' '.join(toks)

        try:
            sql = PT_INSERT2.sub(replace_tab_name, sql)
            sql = PT_FROM_TABLE.sub(replace_src_name, sql)
            new_sqls.append('-- DML start --')
            sql = '\n'.join(new_sqls) + sql
        except UnexpectedSQL:
            error_trans[qkey] = sql
            created_tables.pop(qkey)
        else:
            queries[qkey] = sql
    return queries


def flush_out_queries(queries, dpath):
    for qfile, sql in queries.items():
        odps_file = open(os.path.join(dpath, qfile), 'w')
        odps_file.write(sql)
        odps_file.close()
    

def dump_to_dir(input_queries, dest_dir, suffix=''):
    # Directory intializing.
    global SUFFIX
    SUFFIX = suffix
    try:
        print 'Removing %s ...' % dest_dir
        shutil.rmtree(dest_dir)
    except (OSError, IOError):
        print 'Remove failed.'
    os.mkdir(dest_dir)
    odps_dest_dir = os.path.join(dest_dir, 'queries')
    odps_bad_dir = os.path.join(dest_dir, 'bad_queries')
    os.mkdir(odps_dest_dir)
    os.mkdir(odps_bad_dir)
    print 'Input queries: %s' % len(input_queries)

    # Query transformation.
    created_tables = {}
    source_tables = {}
    bad_queries = {}
    queries = form_cases(input_queries, created_tables, source_tables, bad_queries)
    print 'Output queries: %s' % len(queries)
    print 'Error queries: %s' % len(bad_queries)

    # Flush out queries.
    flush_out_queries(queries, odps_dest_dir)
    flush_out_queries(bad_queries, odps_bad_dir)

    # Verify plan
    tab_file = open(os.path.join(dest_dir, 'verify_tables'), 'w')
    tables = set()
    for qfile, tabparts in created_tables.items():
        tabs = []
        for tabpart in tabparts:
            tables.add(tabpart[0])
            part = ','.join(get_partition_vals(tabpart[1]))
            if part:
                item = '%s:%s' % (tabpart[0], part)
            else:
                item = tabpart[0]
            tabs.append(item)
        tab_file.write("%s\t%s\n" % (qfile, '\t'.join(tabs)))
    tab_file.close()

    # Generate source table list.
    flat_source = set()
    for k, v in source_tables.items():
        flat_source.update(v)
    fp = open(os.path.join(dest_dir, 'source_tables'), 'w')
    for i in flat_source:
        print >>fp, i
    fp.close()

    # Copy misc. stuff
    _cur_dir = os.path.dirname(__file__)
    shutil.copy(os.path.join(_cur_dir, 'tools', 'config.py'), dest_dir)


def get_tmp_table_key(sql):
    """Get a normalized sql for hash key.
    XXX: Do not use the return value of this function as modeling query.
    """
    sql = sql.strip().lower().replace('\n', ' ')
    assert 'tmp_table_for_select' in sql
    # create table tmp_table_for_select_xxx as select
    toks = sql.split()
    sql = ' '.join(toks[3:])
    return sql
    
    

def main():
    created_tables = {}
    input_file = sys.argv[1]
    dest_dir = os.path.abspath(sys.argv[2])
    dates = []
    if len(sys.argv) > 3:
        dates = sys.argv[3].split(',')
    try:
        shutil.rmtree(dest_dir)
    except:
        pass
    os.mkdir(dest_dir)
    odps_dest_dir = os.path.join(dest_dir, 'odps')
    verify_list_dir = os.path.join(dest_dir, 'verify')
    os.mkdir(odps_dest_dir)
    os.mkdir(verify_list_dir)
    error_file = open(os.path.join(dest_dir, 'error.list'), 'w')

    input_queries = []
    for i, line in get_query_from_showp(input_file, dates):
        if 'tmp_table_for_select_' in line:
            continue
        input_queries.append(('q%s.sql' % i, line))
    
    queries, error_list = form_cases(input_queries, created_tables)

    # flush out the queries
    for qfile, sql in queries.items():
        odps_file = open(os.path.join(odps_dest_dir, qfile), 'w')
        odps_file.write(sql)
        odps_file.close()

    error_file.write('\n'.join(error_list))
    error_file.close()
        
    tables = set()
    tab_file = open(os.path.join(verify_list_dir, 'verify_tables'), 'w')
    for qfile, tabparts in created_tables.items():
        tabs = []
        for tabpart in tabparts:
            tables.add(tabpart[0])
            part = ','.join(get_partition_vals(tabpart[1]))
            if part:
                item = '%s;%s' % (tabpart[0], part)
            else:
                item = tabpart[0]
            tabs.append(item)
        tab_file.write("%s\t%s\n" % (qfile, '\t'.join(tabs)))
    tab_file.close()



if __name__ == '__main__':
    main()
    
