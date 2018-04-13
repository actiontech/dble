/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VarsExtractorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(VarsExtractorHandler.class);
    private static final String[] MYSQL_SHOW_VARIABLES_COLS = new String[]{
            "Variable_name",
            "Value"};
    private static final String MYSQL_SHOW_VARIABLES = "show variables";
    private boolean extracting;
    private Lock lock;
    private Condition done;
    private Map<String, PhysicalDBNode> dataNodes;
    private SystemVariables systemVariables = new SystemVariables();
    public VarsExtractorHandler(Map<String, PhysicalDBNode> dataNodes) {
        this.dataNodes = dataNodes;
        this.extracting = false;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public SystemVariables execute() {
        Map.Entry<String, PhysicalDBNode> entry = dataNodes.entrySet().iterator().next();

        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_VARIABLES_COLS, new MysqlVarsListener(this));
        PhysicalDBNode dn = entry.getValue();
        SQLJob sqlJob = new SQLJob(MYSQL_SHOW_VARIABLES, dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
        sqlJob.run();

        waitDone();
        return systemVariables;
    }

    public void handleVars(Map<String, String> vars) {
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            systemVariables.setDefaultValue(key, value);
        }
        signalDone();
    }


    public void signalDone() {
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
            LOGGER.info("wait variables  grapping done " + e);
        } finally {
            lock.unlock();
        }
    }
}
