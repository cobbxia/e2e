#!/usr/bin/env sh

. ${0%/*}/global.sh

tmpdir=`dirname $basedir`/tmp
echo $tmpdir

nohup pssh -h $basedir/deploy/gateway.list -P -t -1 " python2.6 $basedir/CompileDriver.py $tmpdir/$1 " < /dev/null &
