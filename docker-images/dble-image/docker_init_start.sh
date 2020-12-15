#!/bin/sh

echo "dble init&start in docker"

if [ -n "$MASTERS" ] && [ -n "$SLAVES" ] &&  [ -n "$MYSQL_REPLICATION_USER" ] && [ -n "$MYSQL_REPLICATION_PASSWORD" ]; then
      # master:
      masters=(${MASTERS//,/ })
      for M in ${masters[@]}
      do
      mysql -h$M -p3306 -uroot -p123456 \
        -e "CHANGE MASTER TO MASTER_USER='$MYSQL_REPLICATION_USER', MASTER_PASSWORD ='$MYSQL_REPLICATION_PASSWORD' for channel 'group_replication_recovery' ;" \
        -e "SET @@GLOBAL.group_replication_bootstrap_group=1;START GROUP_REPLICATION;SET @@GLOBAL.group_replication_bootstrap_group=0;"
      done

      # slave:
      slaves=(${SLAVES//,/ })
      for S in ${slaves[@]}
      do
      mysql -h$S -p3306 -uroot -p123456 \
        -e "CHANGE MASTER TO MASTER_USER='$MYSQL_REPLICATION_USER', MASTER_PASSWORD ='$MYSQL_REPLICATION_PASSWORD' for channel 'group_replication_recovery' ;" \
        -e "set global group_replication_allow_local_disjoint_gtids_join=ON;START GROUP_REPLICATION;"
      done
fi

cp -n /opt/dble/extend.conf.d/* /opt/dble/conf/

sh /opt/dble/bin/dble start
sh /opt/dble/bin/wait-for-it.sh 127.0.0.1:8066
mysql -P9066 -u man1 -h 127.0.0.1 -p654321 -e "create database @@shardingNode ='dn1,dn2,dn3,dn4,dn5,dn6'"
mysql -P8066 -u root -h 127.0.0.1 -p123456 -e "source /opt/dble/conf/template_table.sql" testdb

echo "dble init finish"

/bin/bash