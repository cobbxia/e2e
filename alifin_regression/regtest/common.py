"""
"""

CLT='/apsara/huian/clt/bin/odpscmd'

import os
import sys
import re
import shlex
import shutil
import subprocess


try:
    import json
except ImportError:
    import simplejson as json


def lineiter(pfile):
    if isinstance(pfile, basestring):
        file_path = pfile
        try:
            fobj = file(file_path)
        except IOError:
            return []
    else:
        fobj = pfile

    ret = []
    for line in fobj:
        line = line.strip()
        if line:
            ret.append(line)
    return ret


def linefiles(pfiles):
    ret = []
    for p in pfiles:
        ret.extend(lineiter(p))
    return ret


def import_from_path(path):
    """
    import the module specified by file path.
    """
    if not os.path.exists(path):
        raise ImportError
    mod_dir, mod_fname = os.path.split(path)
    mtch = re.match(r'^([a-zA-Z_]\w*)\.py$', mod_fname)
    if not mtch:
        raise ImportError
    mod_name = mtch.group(1)
    sys.path.insert(0, mod_dir)
    mod = __import__(mod_name, globals(), locals(), [], -1)
    sys.path.pop(0)
    return mod


def cmd(args):
    r = subprocess.Popen(shlex.split(args),
                         shell=False,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         )
    out, err = r.communicate()
    r.wait()
    return r.returncode, out, err


## declient ##

class SQLExecutionError(Exception):
    pass

    
class DEClient(object):
    
    ## copied from sqlvariant.h ##
    VT_INTEGER = 0
    VT_DOUBLE = 1
    VT_BOOLEAN = 2
    VT_STRING = 3
    VT_DATETIME = 4
    VT_NULL = 5
    VT_BLOB = 6
    ####

    
    def __init__(self, cmd):
        """
        cmd includes username and password
        """
        self.cmd = cmd
        self.err = None
        self._desc_cache = {}


    def execute(self, sql, json_mode=True):
        """Execute sql through piped client.
        Returns client's stdout as string. If the cmd fails returns None.
        """
        extra_args = []
        if json_mode:
            extra_args.append('-j')
        extra_args.extend(['-e', sql])
        p = subprocess.Popen(shlex.split(self.cmd) + extra_args, 
                             shell=False, 
                             stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             #stdin=subprocess.PIPE
                             )
        out, err = p.communicate()
        self.err = err
        self.out = out
        if p.wait() != 0:
            return None
        return out

    def get_table_meta(self, table):
        out = self.execute("get_table_meta %s;" % table)
        if out is None:
            raise SQLExecutionError(self.err + '\n' + self.out)
        # assert out[:4] == 'OK: ', out      # failure has already been examed
        # out = out[4:]
        js = json.loads(out)[0]['Message']
        js = json.loads(js)
        js['Meta'] = json.loads(js['Meta'])
        return js
        
    def get_table_schema(self, table):
        js = self.get_table_meta(table)
        schema = js['Meta']['Schema']
        d = {}
        for i in schema:
            d[i['Index']] = (i['Name'], i['Type'])
        return [item[1] for item in sorted(d.items(), key=lambda x: x[0])]

    def _parse_partition_values(self, out):
        part_values = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            values = []
            for kv in line.split('/'):
                k, v = kv.split('=')
                values.append(v)
            part_values.append(values)
        return part_values
        
    def get_partition_values(self, table):
        out = self.execute('show partitions %s;' % table, json_mode=True)
        if out is None:
            raise SQLExecutionError(self.err + '\n' + self.out)
        out = json.loads(out)
        out = out[0]['Message']
        return self._parse_partition_values(out)

    def desc_table(self, table):
        result = self._desc_cache.get(table, {})
        if result:
            return result

        # Parse the desc output
        schema = []
        partition_columns = []
        out = self.execute("desc %s;" % table, json_mode=False)
        if out is None:
            raise SQLExecutionError(self.err + '\n' + self.out)

        schema = []
        in_field = False
        start = False
        bounder = 0
        for line in out.splitlines():
            if in_field:
                in_field = False
                start = True
                continue
            line = line.strip()
            if start:
                if line.startswith('+-----'):
                    bounder += 1
                    continue
                if bounder == 1:
                    if not line.startswith('| Partition Columns:'):
                        break
                    else:
                        continue
                elif bounder == 3:
                    break
                fields = line.split('|')
                fields = [ f.strip() for f in fields[1:-1]]
                if bounder == 2:
                    partition_columns.append((fields[0], fields[1]))
                else:
                    schema.append((fields[0], fields[1]))
                continue
            if line.startswith('| Field'):
                in_field = True
                continue
        result['schema'] = schema
        result['partition_columns'] = partition_columns
        self._desc_cache[table] = result
        return result

    def is_partitioned_table(self, table):
        result = self.desc_table(table)
        return len(result['partition_columns']) != 0

    def show_tables(self, pattern=''):
        result = self.execute('show tables %s;' % pattern, False)
        for line in result.splitlines():
            line = line.strip()
            if line:
                yield line

class ODPSConsole(DEClient):
    PT = re.compile(r'location:(pangu://.*),\s*inputFormat:com.testyun.apsara.serde.CFile')

    def get_table_schema(self, table):
        result = self.desc_table(table)
        return result['schema']

    def get_partition_values(self, table):
        out = self.execute('show partitions %s;' % table, json_mode=False)
        return self._parse_partition_values(out)

    def get_table_location(self, table):
        out = self.execute('set apsara.moye.restriction=false; desc extended %s;' % table, json_mode=False)
        match = self.PT.search(out, re.M)
        if match:
            location = match.group(1)
            return location
        return None


def get_timestamp():
    from datetime import datetime
    return datetime.now().strftime('_%Y_%m_%d_%H_%M_%s')


def truncate_dir(dir_path):
    try:
        print dir_path
        shutil.rmtree(dir_path)
    except IOError:
        pass
    except OSError:
        pass
    os.mkdir(dir_path)
    return dir_path


def retry(times):
    def dec(f):
        def func(*args, **kwargs):
            for i in range(times):
                try:
                    return f(*args, **kwargs)
                except:
                    pass
                print 'Retry %s/%s' % (i, times)
        return func
    return dec


def show_create_table(odps, table_name):
    desc = odps.desc_table(table)
    # schema = odps.get_table_schema(table)
    print 'drop table if exists ' + table + ';'
    print 'create table ' + table + ' ('
    print ',\n'.join(['%s %s' % i for i in list(desc['schema'])])
    print ')'
    if desc['partition_columns']:
        print 'partitioned by (%s)' % (','.join(['%s %s' % i for i in desc['partition_columns']]))
    print ';'


if __name__ == '__main__':
    import sys
    odps = ODPSConsole(CLT)
    table = sys.argv[1]
    show_create_table(odps, table)
