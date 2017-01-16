#!/usr/bin/env sh

. ${0%/*}/global.sh

echo " prsync -h $basedir/deploy/gateway.list -azr $basedir /apsarabak/wayne.wuw < /dev/null"
prsync -h $basedir/deploy/gateway.list -azr $basedir /apsarabak/wayne.wuw < /dev/null
