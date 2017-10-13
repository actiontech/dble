/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VarsExtractorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(VarsExtractorHandler.class);
    private static final String[] MYSQL_SHOW_VARIABLES_COLS = new String[]{
            "Variable_name",
            "Value"};
    private static final String MYSQL_SHOW_VARIABLES = "show variables";
    private AtomicBoolean extracting;
    private Lock lock;
    private Condition done;

    public VarsExtractorHandler() {
        this.extracting = new AtomicBoolean(false);
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public void execute() {
        Map<String, PhysicalDBNode> dataNodes = DbleServer.getInstance().getConfig().getDataNodes();
        for (Map.Entry<String, PhysicalDBNode> entry : dataNodes.entrySet()) {
            if (extracting.get()) {
                break;
            }

            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_VARIABLES_COLS, new MysqlVarsListener(this));
            PhysicalDBNode dn = entry.getValue();
            SQLJob sqlJob = new SQLJob(MYSQL_SHOW_VARIABLES, dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
            sqlJob.run();
        }

        waitDone();
    }

    public void handleVars(Map<String, String> vars) {
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            SystemVariables.getSysVars().setDefaultValue(key, value);
        }
        signalDone();

        return;
    }

    public boolean isExtracting() {
        return extracting.compareAndSet(false, true);
    }

    private void signalDone() {
        lock.lock();
        try {
            done.signal();
        } finally {
            lock.unlock();
        }
    }

    private void waitDone() {
        lock.lock();
        try {
            while (!extracting.get()) {
                done.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("wait variables  grapping done " + e);
        } finally {
            lock.unlock();
        }
    }
}
