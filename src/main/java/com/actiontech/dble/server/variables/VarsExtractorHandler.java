/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
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
    private Map<String, AbstractPhysicalDBPool> dataHosts;
    private volatile SystemVariables systemVariables = null;
    private PhysicalDatasource usedDataource = null;

    public VarsExtractorHandler(Map<String, AbstractPhysicalDBPool> dataHosts) {
        this.dataHosts = dataHosts;
        this.extracting = false;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public SystemVariables execute() {
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_VARIABLES_COLS, new MysqlVarsListener(this));
        PhysicalDatasource ds = getPhysicalDatasource();
        this.usedDataource = ds;
        if (ds != null) {
            OneTimeConnJob sqlJob = new OneTimeConnJob(MYSQL_SHOW_VARIABLES, null, resultHandler, ds);
            sqlJob.run();
            waitDone();
        } else {
            LOGGER.warn("No Data host is alive, server can not get 'show variables' result");
        }
        return systemVariables;
    }

    private PhysicalDatasource getPhysicalDatasource() {
        PhysicalDatasource ds = null;
        for (AbstractPhysicalDBPool dbPool : dataHosts.values()) {
            PhysicalDatasource dsTest = dbPool.getSource();
            if (dsTest.isTestConnSuccess()) {
                ds = dsTest;
            }
            if (ds != null) {
                break;
            }
        }
        if (ds == null) {
            for (AbstractPhysicalDBPool dbPool : dataHosts.values()) {
                for (PhysicalDatasource dsTest : dbPool.getReadSources()) {
                    if (dsTest.isTestConnSuccess()) {
                        ds = dsTest;
                        break;
                    }
                }
                if (ds != null) {
                    break;
                }
            }
        }
        return ds;
    }

    void handleVars(Map<String, String> vars) {
        systemVariables = new SystemVariables();
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            systemVariables.setDefaultValue(key, value);
        }
        signalDone();
    }


    void signalDone() {
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

    public PhysicalDatasource getUsedDataource() {
        return usedDataource;
    }

    public void setUsedDataource(PhysicalDatasource usedDataource) {
        this.usedDataource = usedDataource;
    }
}
