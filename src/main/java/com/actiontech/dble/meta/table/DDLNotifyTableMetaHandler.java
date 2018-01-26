package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.meta.protocol.StructureMeta;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2018/2/5.
 */
public class DDLNotifyTableMetaHandler extends AbstractTableMetaHandler {

    private Lock lock;
    private Condition done;
    private boolean extracting = false;

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
        DbleServer.getInstance().getTmManager().addTable(schema, tableMeta);
        signalDone();
        DbleServer.getInstance().getTmManager().removeMetaLock(schema, tableMeta.getTableName());
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

    public void waitDone() {
        lock.lock();
        try {
            while (!extracting) {
                done.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
