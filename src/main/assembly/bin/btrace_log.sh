#!/bin/sh
APP_NAME='dble'
basepath=$(cd `dirname $0`; pwd)/../..
basepath=`(cd "$basepath"; pwd)`

if [ "$1" -gt 0 ] 2>/dev/null ;then
    echo "recording btrace log for $1 seconds"
else
    echo 'Usage: btrace_log.sh [statistical time]'
    exit 1
fi

input=$1
export JAVA_HOME=$basepath/jdk
pid=`(ps -ef | grep "lib/$APP_NAME" | grep -v grep|awk '{print $2}')`
echo "su  actiontech-ushard -s /bin/bash timeout $1   $basepath/btrace/bin/btrace $pid $basepath/btrace/bin/BTraceCostTime.java > /$basepath/core/traceCost.log"
timeout $1 su actiontech-ushard -s /bin/bash $basepath/btrace/bin/btrace $pid $basepath/btrace/bin/BTraceCostTime.java > /$basepath/core/traceCost.log