from pyh import *
import sys
import simplejson as json

reload(sys)
sys.setdefaultencoding('UTF-8')

content = json.load(sys.stdin)

page = PyH()

tb = page << table(align="center", style="width:100%")
tb << tr(td(content['title'], style="font:20pt Arial;color:green;font-weight:bold;vertical-align:bottom;font-variant:small-caps") + td() + td("Business Date: " + content["date"], style="vertic
al-align:bottom;font:12pt Arial;text-align: right;vertical-align: bottom"))

tb = page << table(align="center", style="width:100%")
tb << tr(td('<hr style="height:1px;border:none;border-top:1px solid #000"/>'))


tb = page << table(align="center", style="width:100%;font:12pt Arial")
tb << tr(td(content['description']))

page << br()

def tdd(x):
    y = '&nbsp;' if x == '' else x
    return td(y, style="border: 1pt black solid;font:11pt Arial")

# Generate modeling details
tb = page << table(align="center", style="width:100%;font:12pt Arial;border: 1pt black solid", cellpadding="5", cellspacing="0")
tb << tr(td('Summary', colspan="2"), style="font:14pt Arial;background-color: green;color:white")
for k, v in content['info'].items():
    tb << tr(tdd(k) + tdd(v))
if 'jobs' in content:
    tb << tr(td('Modeling Stats', colspan="2"), style="background-color:gray;color:white")
    pre_job = content['jobs']['pre']
    tb << tr(tdd('Total Query Number') + tdd(pre_job['Total Queries']))
    tb << tr(tdd('Successed Query Number') + tdd(pre_job['Success']))
    tb << tr(tdd('Failed Query Number') + tdd(pre_job['Fail']))
tb << tr(td('Verifying Stats', colspan="2"), style="background-color:gray;color:white")


for key in ['Table Count', 'Verify Passed', 'Verify Failed', 'Verify Not Run']:
    tb << tr(tdd(key) + tdd(content['summary'][key]))
page << br()

tb = page << table(align="center", style="width:100%;font:12pt Arial;border: 1pt black solid", cellpadding="5", cellspacing="0")
tb << tr(td('Verify Details', colspan="6"), style="font:14pt Arial;background-color: green;color:white")
tb << tr(tdd('Table Name') + tdd('PRD Row Count') + tdd('PRE Row Count') + tdd('Unmatched') + tdd('Comments') + tdd('Verify Result'), style="background-color:gray;color:white")

def small(x):
    return div(x, style="font:8pt")

def with_color(x):
    if x == 'PASS':
        return div(x, style="color:green")
    if x == 'FAIL':
        return div(x, style="color:red;font-weight:bold")
    else:
        return x

for t in content['table_list']:
    tb << tr(tdd(t['table']) + tdd(t['ay42_rowcount']) + tdd(t['ay39_rowcount']) + tdd(t['unmatched']) + tdd(small(t['comment'])) + tdd(with_color(t['result'])))

page << br()

tb = page << table(align="center", style="width:100%")
tb << tr(td('<hr style="height:1px;border:none;border-top:1px solid #000"/>'))

tb = page << table(align="center", style="width:100%")
tb << tr(td('Parallel working group: wayne.wuw', style="font:8pt Arial"))

page.printOut()
