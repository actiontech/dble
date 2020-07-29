#!/bin/bash

ip_master_arr=(172.18.0.2 172.18.0.5)
ip_slave_arr=(172.18.0.3 172.18.0.4 172.18.0.6 172.18.0.7)

# master:
for M in ${ip_master_arr[@]}
do
mysql -h$M -p3306 -uroot -p123456 \
  -e "SET @@GLOBAL.group_replication_bootstrap_group=1;" \
  -e "drop user if exists repl;" \
  -e "create user repl@'%';" \
  -e "GRANT REPLICATION SLAVE ON *.* TO repl@'%';" \
  -e "flush privileges;" \
  -e "change master to master_user='root' for channel 'group_replication_recovery';" \
  -e "START GROUP_REPLICATION;" \
  -e "SET @@GLOBAL.group_replication_bootstrap_group=0;"
done

# slave:
for S in ${ip_slave_arr[@]}
do
mysql -h$S -p3306 -uroot -p123456 \
  -e "change master to master_user='repl' for channel 'group_replication_recovery';" \
  -e "set global group_replication_allow_local_disjoint_gtids_join=ON;" \
  -e "START GROUP_REPLICATION;"
done





