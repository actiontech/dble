/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response.ha;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.HaInfo;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.services.manager.handler.PacketResult;
import com.oceanbase.obsharding_d.singleton.HaConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;


/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHaEnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupHaEnable.class);

    private DbGroupHaEnable() {
    }

    public static void execute(Matcher enable, PacketResult packetResult) {
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.HA);
        String dbGroupName = enable.group(1);
        String dbInstanceName = enable.group(3);
        //check the dbGroup is exists

        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            HaConfigManager.getInstance().info("added configLock");
            PhysicalDbGroup dbGroup = OBsharding_DServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
            if (dbGroup == null) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("dbGroup " + dbGroupName + " do not exists");
                return;
            }


            if (!dbGroup.checkInstanceExist(dbInstanceName)) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Some of the dbInstanceName in command in " + dbGroup.getGroupName() + " do not exists");
                return;
            }

            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.LOCAL_CHANGE, HaInfo.HaStartType.LOCAL_COMMAND, enable.group(0));
            if (ClusterConfig.getInstance().isClusterEnable()) {
                if (ClusterConfig.getInstance().isNeedSyncHa()) {
                    if (!enableWithCluster(id, dbGroup, dbInstanceName, packetResult)) {
                        return;
                    }
                } else {
                    try {
                        RawJson result = dbGroup.enableHosts(dbInstanceName, true);
                        //only update for the status
                        clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(dbGroup.getGroupName()), result);
                        // eg: all mysql instance is disabled when OBsharding-D starts up, then enable a mysql instance
                        OBsharding_DServer.getInstance().pullVarAndMeta(dbGroup);
                    } catch (Exception e) {
                        HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("enable dataHost with error, use show @@dataSource to check latest status. Error:" + e.getMessage());
                        return;
                    }
                }
            } else {
                try {
                    RawJson result = dbGroup.enableHosts(dbInstanceName, true);
                    HaConfigManager.getInstance().haFinish(id, null, result);
                    // eg: all mysql instance is disabled when OBsharding-D starts up, then enable a mysql instance
                    OBsharding_DServer.getInstance().pullVarAndMeta(dbGroup);
                } catch (Exception e) {
                    HaConfigManager.getInstance().haFinish(id, e.getMessage(), null);
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("enable dbGroup with error, use show @@dbInstance to check latest status. Error:" + e.getMessage());
                    LOGGER.warn("enable dbGroup with error, use show @@dbInstance to check latest status. Error:", e);
                    return;
                }
            }
        } finally {
            lock.readLock().unlock();
            HaConfigManager.getInstance().info("released configLock");
        }
    }

    private static boolean enableWithCluster(int id, PhysicalDbGroup dh, String subHostName, PacketResult packetResult) {
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.HA);
        //get the lock from ucore
        DistributeLock distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getHaLockPath(dh.getGroupName()),
                new HaInfo(dh.getGroupName(),
                        SystemConfig.getInstance().getInstanceName(),
                        HaInfo.HaType.ENABLE,
                        HaInfo.HaStatus.INIT
                )
        );

        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is changing the dbGroup, please try again later.");
            return false;
        }
        HaConfigManager.getInstance().info("added distributeLock, path: " + ClusterMetaUtil.getHaLockPath(dh.getGroupName()));
        try {
            RawJson result = dh.enableHosts(subHostName, false);
            //only update for the status
            clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(dh.getGroupName()), result);
            HaConfigManager.getInstance().haFinish(id, null, result);
            // eg: all mysql instance is disabled when OBsharding-D starts up, then enable a mysql instance
            OBsharding_DServer.getInstance().pullVarAndMeta(dh);
        } catch (Exception e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
            return false;
        } finally {
            distributeLock.release();
            HaConfigManager.getInstance().info("released distributeLock");
        }
        return true;
    }
}
