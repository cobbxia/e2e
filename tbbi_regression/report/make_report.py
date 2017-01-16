# -*- coding: utf-8 -*-
import os
import re
import sys
import simplejson as json

biz_date = sys.argv[1]
table_list_file = sys.argv[2]
v_log_dir = sys.argv[3]
comment_file = sys.argv[4]
table_list = []
for l in open(table_list_file):
    table_list.append(l[:-1])
comments = {}
for l in open(comment_file):
    g = l.split('\t')
    if len(g) > 1:
        comments[g[0]] = g[1]

content = {}
content['title'] = 'Ay42 and aY39 ParAllEl RunNing <br/> DaiLy VeriFicAtIOn RepOrt'
content['date'] = biz_date
content['description'] = '阔四二，穷三九，对比但凭心和手。星月落，竭虑博，半分差错，整宿重拖，弱、弱、弱。 <br/> 觉不够，人渐瘦，花容憔悴香汗臭。梦谁做，霸气多，六千忒驳，一键回车，过、过、过。'

summary = {}
summary['Status'] = 'N/A'
summary['Table Count'] = str(len(table_list))

pass_count = 0
fail_count = 0

tl = []
for t in table_list:
    comment = ''
    result = ''
    ay42_rc = ''
    ay39_rc = ''
    unmatch = ''
    v_log = v_log_dir + '/' + t + '.basetable' + biz_date + '_' + t + '.log'
    if not os.path.isfile(v_log):
        comment = 'Verify log not found!'
        result = 'FAIL'

    ptn1 = re.compile('.*Verifier.*LEFT_RECORDS=(\d+).*')
    ptn2 = re.compile('.*Verifier.*RIGHT_RECORDS=(\d+).*')
    ptn3 = re.compile('.*Verifier.*UNMATCH_RECORDS=(\d+).*')
    for line in open(v_log):
        m = ptn1.match(line)
        if m:
            ay42_rc = int(m.groups()[0])
        m = ptn2.match(line)
        if m:
            ay39_rc = int(m.groups()[0])
        m = ptn3.match(line)
        if m:
            unmatch = int(m.groups()[0])
        if line.startswith('Verify success!'):
            result = 'PASS'
        if line.startswith('Verify fail!'):
            result = 'FAIL'
        if line.startswith('Two paths are the same. Do not verify.'):
            result = 'PASS'
            comment = 'Two paths are the same. Do not verify.'
    if unmatch == '' and ay42_rc != '':
        unmatch = 0
    if result == 'PASS':
        pass_count += 1
    if result == 'FAIL':
        fail_count += 1
    if comments.has_key(t):
        comment = comments[t]
    tl.append({'table':t, 'ay42_rowcount':ay42_rc, 'ay39_rowcount':ay39_rc, 'unmatched':unmatch, 'comment':comment, 'result':result})

def rk(x):
    if x['result'] == 'FAIL':
        return 1
    if x['result'] == 'PASS':
        return 3
    return 2

def cmp(x, y):
    return rk(x) - rk(y)

tl.sort(cmp)

content['table_list'] = tl

summary['Verify Passed'] = pass_count
summary['Verify Failed'] = fail_count
content['summary'] = summary

json.dump(content, sys.stdout, indent=2)
