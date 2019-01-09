/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.meta.table.old.AbstractTableMetaHandler;
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

    public DDLNotifyTableMetaHandler(String schema, String tableName, List<String> dataNodes, Set<String> selfNode) {
        super(schema, tableName, dataNodes, selfNode);
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    @Override
    public void countdown() {

    }

    @Override
    public void execute() {
        super.execute();
        this.waitDone();
    }

    @Override
    public void handlerTable(StructureMeta.TableMeta tableMeta) {
        if (tableMeta != null) {
            DbleServer.getInstance().getTmManager().addTable(schema, tableMeta);
            metaInited = true;
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
