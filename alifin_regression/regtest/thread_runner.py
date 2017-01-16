import os
import sys
import re
import subprocess
import threading
import Queue
import shlex

def worker_thread(queue):
    while True:
        cmd = queue.get()
        LOG.info('In threading: ' + cmd)
        stdin, stdout, stderr = os.popen3(cmd)
        out = stdout.read()
        err = stderr.read()
        LOG.info('out: ' + out)
        LOG.info('err: ' + err)
        stdin.close()
        stdout.close()
        stderr.close()
        queue.task_done()

def run_parallel(items, para):
    queue = Queue.Queue()
    for i in items:
        queue.put(i)
    for i in range(para):
        t = threading.Thread(target=worker_thread, args=(queue,))
        t.daemon = True
        t.setDaemon(True)
        t.start()
    queue.join()

