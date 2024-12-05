#!/bin/sh

if [ -n "$MASTERS" ] && [ -n "$SLAVES" ] &&  [ -n "$MYSQL_REPLICATION_USER" ] && [ -n "$MYSQL_REPLICATION_PASSWORD" ]; then
      echo "OBsharding-D init mysql mgr"
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

echo "OBsharding-D init&start in docker"
sh /opt/OBsharding-D/bin/OBsharding-D start
sh /opt/OBsharding-D/bin/wait-for-it.sh 127.0.0.1:8066

echo "init shardingNode and execute template_table.sql"
mysql -P9066 -u man1 -h 127.0.0.1 -p654321 -e "create database @@shardingNode ='dn1,dn2,dn3,dn4,dn5,dn6'"
mysql -P8066 -u root -h 127.0.0.1 -p123456 -e "source /opt/OBsharding-D/conf/template_table.sql" testdb

echo "OBsharding-D init finish"

/bin/bash