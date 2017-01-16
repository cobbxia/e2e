#!/bin/sh

. ${0%/*}/global.sh

tmpdir=`dirname $basedir`/tmp
stamp=`date +%y%m%d%H`

#cat $base_dir/finished.log >> $base_dir/finished.log.$stamp
#cat $base_dir/failed.log >> $base_dir/failed.log.$stamp
mv $tmpdir/logs $tmpdir/logs_$stamp
mv $tmpdir/vlogs $tmpdir/vlogs_$stamp
mv $tmpdir/hwlogs $tmpdir/hwlogs_$stamp

mkdir $tmpdir/logs
mkdir $tmpdir/vlogs
mkdir $tmpdir/hwlogs
