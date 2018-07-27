#!/bin/sh
APP_NAME='dble'
CORE_JAVA_HOME=$JAVA_HOME

if [ "$1" -gt 0 ] 2>/dev/null ;then
    echo "recording btrace log for $1 seconds"
else
    echo 'Usage: btrace_log.sh [statistical time]'
    exit 1
fi


#check CORE_JAVA_HOME & java
noJavaHome=false
if [ -z "$CORE_JAVA_HOME" ] ; then
    noJavaHome=true
fi
if [ ! -e "$CORE_JAVA_HOME/bin/java" ] ; then
    noJavaHome=true
fi
if $noJavaHome ; then
    echo
    echo "Error: CORE_JAVA_HOME environment variable is not set."
    echo
    exit 1
fi

input=$1
export JAVA_HOME=$CORE_JAVA_HOME
BASEDIR=`dirname "$0"`/../..
BASEDIR=`(cd "$BASEDIR"; pwd)`
pid=`(ps -ef | grep "lib/$APP_NAME" | grep -v grep|awk '{print $2}')`
echo "su  actiontech-ushard -s /bin/bash timeout $1   $BASEDIR/btrace/bin/btrace $pid $BASEDIR/btrace/bin/BTraceCostTime.java > /$BASEDIR/core/traceCost.log"
timeout $1 su actiontech-ushard -s /bin/bash $BASEDIR/btrace/bin/btrace $pid $BASEDIR/btrace/bin/BTraceCostTime.java > /$BASEDIR/core/traceCost.log