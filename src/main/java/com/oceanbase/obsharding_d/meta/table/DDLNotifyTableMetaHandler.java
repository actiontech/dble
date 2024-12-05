/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta.table;

import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.DDLTraceHelper;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2018/2/5.
 */
public class DDLNotifyTableMetaHandler extends AbstractTableMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableMetaHandler.class);
    private Lock lock;
    private Condition done;
    private boolean extracting = false;
    private volatile boolean metaInited = false;
    private boolean isCreateSql = false;
    private ShardingService currShardingService;

    public DDLNotifyTableMetaHandler(String schema, String tableName, List<String> shardingNodes, Set<String> selfNode, boolean isCreateSql, ShardingService currShardingService) {
        super(schema, tableName, shardingNodes, selfNode, false);
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
        this.isCreateSql = isCreateSql;
        this.currShardingService = currShardingService;
    }

    @Override
    public void countdown() {

    }

    @Override
    public void execute() {
        DDLTraceHelper.log2(currShardingService, DDLTraceHelper.Stage.update_table_metadata, "Start execute sql{show create table} in the shardingNodes[" + Strings.join(shardingNodes, ',') + "] to get table[" + tableName + "]'s information");
        super.execute();
        this.waitDone();
    }

    @Override
    public void handlerTableByNode(boolean isSucc, String tableName0, String shardingNode) {
        DDLTraceHelper.log2(currShardingService, DDLTraceHelper.Stage.update_table_metadata, "In shardingNode[" + shardingNode + "], fetching " + (isSucc ? "success" : "fail"));
    }

    @Override
    public void handlerTable(TableMeta tableMeta) {
        if (tableMeta != null) {
            ProxyMeta.getInstance().getTmManager().addTable(schema, tableMeta, isCreateSql);
            metaInited = true;
            DDLTraceHelper.log2(currShardingService, DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.succ, "Successful to update table[" + schema + "." + tableMeta.getTableName() + "]'s metadata");
        } else {
            DDLTraceHelper.log2(currShardingService, DDLTraceHelper.Stage.update_table_metadata, DDLTraceHelper.Status.fail, "Failed to update table[" + schema + "." + tableName + "]'s metadata");
        }
        signalDone();
    }


    private void signalDone() {
        lock.lock();
        try {
            extracting = true;
            done.signal();
        } finally {
            lock.unlock();
        }
    }

    private void waitDone() {
        lock.lock();
        try {
            while (!extracting) {
                done.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("InterruptedException", e);
        } finally {
            lock.unlock();
        }
    }

    public boolean isMetaInited() {
        return metaInited;
    }

}
