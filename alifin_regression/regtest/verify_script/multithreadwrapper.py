#!/bin/env python

import os
import sys
import signal
import threading
from time import ctime, sleep
import random

class Watcher:
    def __init__(self):
        self.child = os.fork()
        if self.child == 0:
            return
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except KeyboardInterrupt:
            print "KeyBoard Interrupt"
            self.kill()
        sys.exit()

    def kill(self):
        try:
            os.kill(self.child, signal.SIGKILL)
        except OSError: pass


class WorkerThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
        self.result = []

    def getResult(self):
        return self.result

    def run(self):
        self.result = apply(self.func, self.args)


class MultiThreadWrapper:
    def __init__(self, threshold):
        if (threshold <= 0):
            self.__threshold = 1
        else:
            self.__threshold = threshold
        self.__nextkey = 0
        self.__threads = {}
        Watcher()
    
    def isReady(self):
        if (len(self.__threads) < self.__threshold):
            return 1
        else:
            return 0

    def reset(self, threshold):
        if (threshold <= 0):
            self.__threshold = 1
        else:
            self.__threshold = threshold
        for key in self.__threads:
            self.__threads[key].join()
        self.__threads.clear()
        self.__nextkey = 0

    def getThreadIdList(self):
        return self.__threads.keys()

    def wait4End(self):
        for key in self.__threads:
            self.__threads[key].join()

    def removeFinishedThread(self):
        uselesskeys = []
        for key in self.__threads:
            if (not self.__threads[key].isAlive()):
                uselesskeys.append(key)
        for key1 in uselesskeys:
                self.__threads.pop(key1)
        
        if (len(self.__threads) == self.__threshold):
            threadsnum = len(self.__threads)
            idx = random.randint(0, threadsnum - 1)
            keys = self.__threads.keys()
            id = keys[idx]
            t = self.__threads.pop(id)
            t.join()

    def getResult(self, id):
        ret = []
        if (id in self.__threads):
            t = self.__threads.pop(id)
            t.join()
            ret = t.getResult()

        return ret

    def run(self, func, args, name = ''):
        if (not self.isReady()):
            return 1    #please wait thread finish

        t = WorkerThread(func, args, func.__name__)
        self.__threads[self.__nextkey] = t
        self.__threads[self.__nextkey].start()
        self.__nextkey += 1


def trialthread(threadid, result):
    print 'thread:', threadid
    if threadid == 0:
        sleep(2)
    print threadid, result
    return result

if __name__ == '__main__':
    
    mtw = MultiThreadWrapper(2)
    mtw.run(trialthread, (0, 'hello'))
    mtw.run(trialthread, (1, 'world'))
    idlist = mtw.getThreadIdList()
    for id in idlist:
        print mtw.getResult(id)


        

    









