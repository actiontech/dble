/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.util;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.DbGroups;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.oceanbase.obsharding_d.config.converter.DBConverter;
import com.oceanbase.obsharding_d.singleton.HaConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2019/10/23.
 */
public class DbXmlWriteJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbGroup.class);
    private final Set<PhysicalDbGroup> changeSet;
    private final DbGroups dbGroups;
    private volatile boolean finish = false;
    private volatile String errorMessage = null;
    private final ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private final int reloadIndex;

    public DbXmlWriteJob(Set<PhysicalDbGroup> changeSet, DbGroups dbGroups, int reloadIndex) {
        this.changeSet = changeSet;
        this.dbGroups = dbGroups;
        this.reloadIndex = reloadIndex;
    }

    @Override
    public void run() {
        try {
            List<DBGroup> dbGroupList = dbGroups.getDbGroup();
            Map<String, DBGroup> dbGroupBeanMap = ClusterLogic.forHA().changeFromListToMap(dbGroupList);
            for (PhysicalDbGroup dbGroup : changeSet) {
                DBGroup dbGroupBean = dbGroupBeanMap.get(dbGroup.getGroupName());
                if (dbGroupBean != null) {
                    changeHostInfo(dbGroupBean, dbGroup);
                } else {
                    HaConfigManager.getInstance().warn("dbGroup " + dbGroup.getGroupName() + " is not found");
                }
            }
            HaConfigManager.getInstance().write(dbGroups, reloadIndex);
            OBsharding_DServer.getInstance().getConfig().setDbConfig(DBConverter.dbBeanToJson(dbGroups));
        } catch (Exception e) {
            errorMessage = e.getMessage();
            HaConfigManager.getInstance().info("get error from SchemaWriteJob", e);
        } finally {
            HaConfigManager.getInstance().finishAndNext();
            this.signalAll();
        }
    }


    private void changeHostInfo(DBGroup dbGroupBean, PhysicalDbGroup dbGroup) {
        Map<String, PhysicalDbInstance> physicalDbInstances = dbGroup.getAllDbInstanceMap();
        for (DBInstance dbInstanceBean : dbGroupBean.getDbInstance()) {
            PhysicalDbInstance dbInstance = physicalDbInstances.get(dbInstanceBean.getName());
            if (dbInstance != null) {
                dbInstanceBean.setPrimary(!dbInstance.isReadInstance());
                dbInstanceBean.setDisabled(dbInstance.isDisabled() ? "true" : null);
            } else {
                LOGGER.warn("dbInstance " + dbInstanceBean.getName() + " is not found");
            }
        }
    }


    public void waitForWritingDone() {
        lock.lock();
        try {
            while (!finish) {
                condition.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("unexpected error:", e);
        } finally {
            lock.unlock();
        }

        if (errorMessage != null) {
            LOGGER.info("get result errorMessage = " + errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    public void signalAll() {
        lock.lock();
        try {
            finish = true;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
