#!/usr/bin/env bash

WORK_DIR=./workdir
LOG_DIR=./logs

log_file=$1
log_file_list=`cat $log_file`

failed_rule_file=$2
rules=`cat $failed_rule_file`

rm -rf $WORK_DIR/xlogs
mkdir $WORK_DIR/xlogs

for file in $log_file_list ; do
    for r in $rules ; do
        grep $r $LOG_DIR/$file
        if [ $? == 0 ] ; then
            mv $LOG_DIR/$file $WORK_DIR/xlogs
        fi
    done
done
