/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.OneTimeConnJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class VarsExtractorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(VarsExtractorHandler.class);
    private static final String[] MYSQL_SHOW_VARIABLES_COLS = new String[]{
            "Variable_name",
            "Value"};
    private static final String MYSQL_SHOW_VARIABLES = "show variables";
    private boolean extracting;
    private Lock lock;
    private Condition done;
    private Map<String, PhysicalDbGroup> dbGroups;
    private volatile SystemVariables systemVariables = null;
    private boolean isExistClickHouse = false;

    public VarsExtractorHandler(Map<String, PhysicalDbGroup> dbGroups) {
        this.dbGroups = dbGroups;
        this.extracting = false;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public SystemVariables execute() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-system-variables-from-backend");
        try {
            /**
             * get system Variables only from available Mysql instances (does not contain ClickHouse instance);
             * if there is only ClickHouse in the instances, just fake a copy of the system parameters, see tryInitVars().
             */
            PhysicalDbInstance ds = getMysqlPhysicalDbInstance();
            if (ds != null) {
                OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_VARIABLES_COLS, new MysqlVarsListener(this));
                OneTimeConnJob sqlJob = new OneTimeConnJob(MYSQL_SHOW_VARIABLES, null, resultHandler, ds);
                sqlJob.run();
                waitDone();
            } else {
                tryInitVars();
                LOGGER.warn("No Mysql dbInstance is alive, server can not get 'show variables' result");
            }
            return systemVariables;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private PhysicalDbInstance getMysqlPhysicalDbInstance() {
        if (dbGroups == null || dbGroups.isEmpty()) {
            return null;
        }
        PhysicalDbInstance ds = null;

        List<PhysicalDbGroup> dbGroupList = dbGroups.values().stream().collect(Collectors.toList());
        for (PhysicalDbGroup dbGroup : dbGroupList) {
            PhysicalDbInstance dsTest = dbGroup.getWriteDbInstance();
            if (dsTest.isTestConnSuccess()) {
                if (dsTest.getConfig().getDataBaseType() == DataBaseType.MYSQL) {
                    ds = dsTest;
                    break;
                } else {
                    isExistClickHouse = true;
                }
            }
        }
        if (ds == null) {
            outLoop:
            for (PhysicalDbGroup dbGroup : dbGroupList) {
                for (PhysicalDbInstance dsTest : dbGroup.getDbInstances(false)) {
                    if (dsTest.isTestConnSuccess()) {
                        if (dsTest.getConfig().getDataBaseType() == DataBaseType.MYSQL) {
                            ds = dsTest;
                            break outLoop;
                        } else {
                            isExistClickHouse = true;
                        }
                    }
                }
            }
        }
        return ds;
    }

    void handleVars(Map<String, String> vars) {
        systemVariables = new SystemVariables();
        StringBuilder sb = new StringBuilder("system default value:");
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            sb.append("key:").append(key).append(",value:").append(value).append(",");
            systemVariables.setDefaultValue(key, value);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sb.toString());
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

    private void tryInitVars() {
        if (isExistClickHouse) {
            systemVariables = new SystemVariables();
            systemVariables.setDefaultValue("lower_case_table_names", "0");
        }
    }
}
