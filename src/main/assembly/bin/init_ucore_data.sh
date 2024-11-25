#!/bin/bash
cd "$(dirname "$0")"
echo "check JAVA_HOME & java"
JAVA_CMD="$2"/bin/java
MAIN_CLASS=com.oceanbase.obsharding_d.cluster.general.xmltoKv.XmltoCluster
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
OBsharding-D_HOME="$1"
if [ ! -n "$1" ]; then
    OBsharding-D_HOME=`pwd`
fi
cd $CURR_DIR
$JAVA_CMD -Xms256M -Xmx1G  -DhomePath=$OBsharding-D_HOME -cp "$OBsharding-D_HOME/conf:$OBsharding-D_HOME/lib/*" $MAIN_CLASS
if [ $? -eq 0 ]; then
    echo "--------finish with success -------"
    exit 0
  else
    echo "--------finish with error---------"
    exit 1
fi
