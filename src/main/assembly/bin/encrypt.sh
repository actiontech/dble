#!/bin/sh
CORE_JAVA_HOME=$JAVA_HOME
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

#set HOME
CURR_DIR=`pwd`
cd `dirname "$0"`/..
APP_HOME=`pwd`
cd $CURR_DIR
if [ -z "$APP_HOME" ] ; then
    echo
    echo "Error: APP_HOME environment variable is not defined correctly."
    echo
    exit 1
fi 

#============run encrypt
input=$1
PASSWORD=${input/password=/""}
RUN_CMD="$CORE_JAVA_HOME/bin/java -cp $APP_HOME/lib/dble*.jar com.actiontech.dble.util.DecryptUtil $PASSWORD"
echo "$CORE_JAVA_HOME/bin/java -cp $APP_HOME/lib/dble*.jar com.actiontech.dble.util.DecryptUtil password=******"
eval $RUN_CMD
EXIT_STATUS=$?
exit $EXIT_STATUS
#==============================================================================
