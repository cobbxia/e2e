

def normalize_sql(sql, bizdate):
    sql = sql.strip()
    sql = sql.replace(bizdate, '$bizdate')
    return sql


def specialize_sql(sql, bizdate):
    sql = sql.strip()
    sql = sql.replace('$bizdate', bizdate)
    return sql


def build_set(sqls):
    ret = set()
    for i in sqls:
        ret.add(normalize_sql(i))
    return re



if __name__ == '__main__':
    pass
