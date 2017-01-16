import urllib
import urllib2

#url_prefix = 'http://10.101.214.141:8000'
url_prefix = 'http://k.test-inc.com/watchmen/api'

def add_case(name, module, group, owner=None, case_time=None, log_url=None, comment=None):

    url = '%s/add_case' % url_prefix
    print(url)

    postdata = dict(name=name, module=module, group=group)
    print(postdata)

    if comment:
        postdata.update({'comment':comment})

    if owner:
        postdata.update({'owner':owner})

    if case_time:
        postdata.update({'case_time':case_time})

    if log_url:
        postdata.update({'log_url':log_url})

    postdata = urllib.urlencode(postdata)

    print(postdata)
    request = urllib2.Request(url, postdata)

    response = urllib2.urlopen(request)
    print(response.read())
    print response.code

def add_report(module, group, case_time=None, link=None, status=None, latency=None, comment=None, total_case=0, failure_case=0):

    url = '%s/add_report' % url_prefix
    print(url)

    postdata = dict(module=module, group=group)
    print(postdata)

    if case_time:
        postdata.update({'case_time':case_time})

    if link:
        postdata.update({'link':link})

    if status:
        postdata.update({'status':str(status)})

    if latency:
        postdata.update({'latency':latency})

    if comment:
        postdata.update({'comment':comment})

    if total_case:
        postdata.update({'total_case':total_case})

    if failure_case:
        postdata.update({'failure_case':failure_case})

    postdata = urllib.urlencode(postdata)

    print(postdata)
    request = urllib2.Request(url, postdata)

    response = urllib2.urlopen(request)
    print(response.read())

    print response.code

def add_build(name, group, info=None, comment=None):

    url = '%s/add_build' % url_prefix
    print(url)

    postdata = dict(name=name, group=group)
    print(postdata)

    if info:
        postdata.update({'info':info})

    if comment:
        postdata.update({'comment':comment})

    postdata = urllib.urlencode(postdata)
    print(postdata)

    request = urllib2.Request(url, postdata)

    response = urllib2.urlopen(request)
    print(response.read())

    print response.code

def split_mail(module, group, case_time):

    url = '%s/split_mail' % url_prefix
    print(url)

    postdata = dict(module=module, group=group, case_time=case_time)

    postdata = urllib.urlencode(postdata)

    print(postdata)
    request = urllib2.Request(url, postdata)

    response = urllib2.urlopen(request)

    print response.code


if __name__ == '__main__':

    print(add_case('groupby_limit_2.q', 'ServiceMode', 'trunk'))
    print(add_report('ServiceMode', 'trunk', '2015-12-13 14:11:12', 'http://wocao', 'True', '23', 'cm'))

    #split_mail('MrTask', 'sandbox', '14-03-27 18:55:40')
    print(add_build('sp123', 'sandbox', info='info', comment='comment'))
