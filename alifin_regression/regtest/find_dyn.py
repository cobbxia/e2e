import os
from common import lineiter

dest_dir = '/tmp/find_dyn'
all_queries = lineiter('/apsarabak/wayne.wuw/regtest/extended_24xx.list')

mysql_cmd = "mysql -ucube -pq -h172.24.146.24 -Dcube_ammdb_schema -e"
Q_TPL = '''"select part_name from e2e_qry_tgt where version_id=38 and taskid=%s and table_name='%s' and part_name is not NULL"'''

os.system('rm -rf %s; mkdir %s' % (dest_dir, dest_dir))

dy_queries = set()

for q in all_queries:
    taskid = q[:5]
    p_mhql = os.path.join('/apsarabak/wayne.wuw/regtest/moye_hql', q)
    p_tbpart = os.path.join('/apsarabak/wayne.wuw/regtest/table_part/', q)
    assert os.path.exists(p_mhql)
    assert os.path.exists(os.path.join('/apsarabak/wayne.wuw/regtest/hive_hql', q))
    assert os.path.exists(p_tbpart)
    tbparts_list = lineiter(p_tbpart)

    entries = []
    has_dyn_part = False
    for line in tbparts_list:
        tokens = line.split(':')
        if len(tokens) == 1:
            # no partition, skip
            continue
        assert len(tokens) == 2
        parts = tokens[1].split(',')

        for p in parts:
            if '=' not in p:
                found_parts = lineiter(os.popen(mysql_cmd+ (Q_TPL % (taskid, tokens[0]))))
                assert len(found_parts) > 1
                for f_part in found_parts[1:]:
                    entries.append('%s:%s' % (tokens[0], f_part.replace('/', ',')))
                has_dyn_part = True

    if has_dyn_part:
        assert len(tbparts_list) == 1
        with open(os.path.join(dest_dir, q), 'w') as fp:
            for i in entries:
                fp.write(i+'\n')

