#!/bin/env python

#This file provide
#1. logging with a specific file
#2. TODO:support multi thread logging
#3. TODO:support pg logging

import os
import sys
import string
import time

class Logger:
    def __init__(self, logpath, logfile, logtype, prefix, isaddtimestamp):
        self.isaddtimestamp = isaddtimestamp
        if isaddtimestamp:
            self.reset(logpath, logfile + str(time.time()), logtype, prefix)
        else:
            self.reset(logpath, logfile, logtype, prefix)

    def write(self, lines):
        if not self.__isready:
            return False
        if self.isaddtimestamp: 
            lines = "[" + str(time.ctime()) + "]  " + self.__prefix + lines + '\n'     
        else:
            lines = self.__prefix + lines + '\n'     
        self.__logfobj.writelines(lines) #TODO: Handle exception here
        self.__logfobj.flush()
        return True

    def finish(self):
        self.__logfobj.close()
        self.__logfilepath = ''
        self.__isready = False
        
    def reset(self, logpath, logfile, logtype, prefix):
        if not os.path.isdir(logpath):#TODO:check path format
            os.system("mkdir -p %s" % logpath)
        self.__logfilepath = os.path.join(logpath, logfile)
        self.__logfobj = open(self.__logfilepath, 'w')
        self.__isready = True
        self.__prefix = prefix

if __name__ == '__main__':
    print 'Unit Test'
    print 'Test Start'
    logger = Logger('/tmp', 'test.log', 'local', 'Unit Test:')
    logger.write('hello!\nworld!\nI am logging!\n')
    logger.finish()
    print 'Test End'

