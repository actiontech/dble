#!/bin/bash

echo "check JAVA_HOME & java"
JAVA_CMD="$2"/bin/java
MAIN_CLASS=com.actiontech.dble.cluster.general.xmltoKv.XmltoCluster
if [ ! -n "$2" ]; then
    JAVA_CMD=$JAVA_HOME/bin/java
    if [ ! -d "$JAVA_HOME" ]; then
        echo ---------------------------------------------------
        echo WARN: JAVA_HOME environment variable is not set.
        echo ---------------------------------------------------
        JAVA_CMD=java
    fi
fi

echo "---------set HOME_DIR------------"
CURR_DIR=`pwd`
cd ..
DBLE_HOME="$1"
if [ ! -n "$1" ]; then
    DBLE_HOME=`pwd`
fi
cd $CURR_DIR
$JAVA_CMD -Xms256M -Xmx1G  -DhomePath=$DBLE_HOME -cp "$DBLE_HOME/conf:$DBLE_HOME/lib/*" $MAIN_CLASS
if [ $? -eq 0 ]; then
    echo "--------finish with success -------"
    exit 0
  else
    echo "--------finish with error---------"
    exit 1
fi
