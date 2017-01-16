#!/usr/bin/env python

import os
import sys
from common import lineiter


#######
# configurations

PREFIX = 'x'
DEST_DIR = '/apsarabak/wayne.wuw/tmp'

#######

if __name__ == '__main__':
    iplist = lineiter(sys.argv[1])
    big_files = lineiter(sys.argv[2])
    small_name = sys.argv[3]
    filenum = len(iplist)
    bigfile_num = len(big_files)
    avg_per_file = bigfile_num / filenum
    
    total_files = []
    j = 0
    for i in range(filenum):
        x = big_files[j: j + avg_per_file]
        j = j + avg_per_file
        total_files.append(x)

    remain_num = bigfile_num - filenum * avg_per_file
    for i, offset in enumerate(range(filenum*avg_per_file, bigfile_num)):
        total_files[i].append(big_files[offset])
        
    file_names = []
    for i, l in enumerate(total_files):
        f_name = '/tmp/%s%d' % (PREFIX, i)
        file_names.append(f_name)
        f = open(f_name, 'w')
        for j in l:
            print >>f, j
        f.close()

    # deliver sliced lines to each machine
    assert len(total_files) == len(iplist)
    for i, f in enumerate(file_names):
        dest_f = os.path.join(DEST_DIR, small_name)
        os.system('scp %s %s:%s' % (f, iplist[i], dest_f))
        
