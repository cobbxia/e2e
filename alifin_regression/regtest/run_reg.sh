#!/usr/bin/env sh
. .bashrc
. /home/admin/cube/env.sh
cd /home/admin/cxu/cube

pssh -h gateway.list -t -1 " cd /home/admin/cxu/cube && python2.6 CompileDriver.py to_run.list" >> /tmp/reg.log 2>&1
