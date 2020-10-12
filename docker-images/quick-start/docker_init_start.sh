#!/bin/sh

echo "dble init&start in docker"

if [[ $1 == "startMgr" ]]; then
    echo "start mgr ..."
    sh /opt/dble/bin/start_mgr.sh
fi

sh /opt/dble/bin/dble start
sh /opt/dble/bin/wait-for-it.sh 127.0.0.1:8066
mysql -P9066 -u man1 -h 127.0.0.1 -p654321 -e "create database @@shardingNode ='dn1,dn2,dn3,dn4,dn5,dn6'"
mysql -P8066 -u root -h 127.0.0.1 -p123456 -e "source /opt/dble/conf/template_table.sql" testdb

echo "dble init finish"

/bin/bash