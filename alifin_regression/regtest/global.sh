#!/usr/bin/env sh

export basedir=$(cd `dirname $BASH_SOURCE` && pwd)
export tmpdir=`dirname $basedir`/tmp
export HIVE_HOME=/apsara/ganjiang/moye-regtest
export HIVE_CONF_DIR=$basedir/deploy/hiveconf
export HADOOP_HOME=/home/hadoop/hadoop-current
export HADOOP_CONF_DIR=$basedir/deploy/hadoopconf
alias moye=/apsara/ganjiang/moye-regtest/bin/hive
alias hive=/apsara/ganjiang/router-111020/bin/hive

export PATH=$HADOOP_HOME/bin:$PATH
export LD_LIBRARY_PATH=/apsara/lib64:$LD_LIBRARY_PATH