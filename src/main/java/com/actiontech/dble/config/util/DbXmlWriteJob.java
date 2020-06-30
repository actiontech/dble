package com.actiontech.dble.config.util;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.singleton.HaConfigManager;
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
            Map<String, DBGroup> dbGroupBeanMap = ClusterLogic.changeFromListToMap(dbGroupList);
            for (PhysicalDbGroup dbGroup : changeSet) {
                DBGroup dbGroupBean = dbGroupBeanMap.get(dbGroup.getGroupName());
                if (dbGroupBean != null) {
                    changeHostInfo(dbGroupBean, dbGroup);
                } else {
                    LOGGER.warn("dbGroup " + dbGroup.getGroupName() + " is not found");
                }
            }
            HaConfigManager.getInstance().write(dbGroups, reloadIndex);
        } catch (Exception e) {
            errorMessage = e.getMessage();
            HaConfigManager.getInstance().log("get error from SchemaWriteJob", e);
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
