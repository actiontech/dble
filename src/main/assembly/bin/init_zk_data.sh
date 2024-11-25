#!/bin/bash
cd "$(dirname "$0")"
echo "check JAVA_HOME & java"
JAVA_CMD=$JAVA_HOME/bin/java
MAIN_CLASS=com.oceanbase.obsharding_d.cluster.zkprocess.xmltozk.XmltoZkMain
if [ ! -d "$JAVA_HOME" ]; then
    echo ---------------------------------------------------
    echo WARN: JAVA_HOME environment variable is not set. 
    echo ---------------------------------------------------
    JAVA_CMD=java
fi

echo "---------set HOME_DIR------------"
CURR_DIR=`pwd`
cd ..
OBsharding-D_HOME=`pwd`
cd $CURR_DIR
$JAVA_CMD -Xms256M -Xmx1G  -DhomePath=$OBsharding-D_HOME -cp "$OBsharding-D_HOME/conf:$OBsharding-D_HOME/lib/*" $MAIN_CLASS
echo "---------finished------------"
