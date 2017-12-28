#!/bin/sh

#check JAVA_HOME & java
noJavaHome=false
if [ -z "$JAVA_HOME" ] ; then
    noJavaHome=true
fi
if [ ! -e "$JAVA_HOME/bin/java" ] ; then
    noJavaHome=true
fi
if $noJavaHome ; then
    echo
    echo "Error: JAVA_HOME environment variable is not set."
    echo
    exit 1
fi

#set HOME
CURR_DIR=`pwd`
cd `dirname "$0"`/..
DBLE_HOME=`pwd`
cd $CURR_DIR
if [ -z "$DBLE_HOME" ] ; then
    echo
    echo "Error: DBLE_HOME environment variable is not defined correctly."
    echo
    exit 1
fi 

#============run encrypt
RUN_CMD="$JAVA_HOME/bin/java -cp $DBLE_HOME/lib/dble*.jar com.actiontech.dble.util.DecryptUtil $@" 
echo $RUN_CMD
eval $RUN_CMD
#==============================================================================
