/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.logic;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.*;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.services.manager.response.ReloadConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.JSON_LIST;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class HAClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(HAClusterLogic.class);

    HAClusterLogic() {
        super(ClusterOperation.HA);
    }

    public void dbGroupChangeEvent(String dbGroupName, RawJson value) {
        int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, "");
        PhysicalDbGroup physicalDBPool = DbleServer.getInstance().getConfig().getDbGroups().get(dbGroupName);
        if (null != physicalDBPool) {
            physicalDBPool.changeIntoLatestStatus(value);
        }
        HaConfigManager.getInstance().haFinish(id, null, value);
    }


    public void dbGroupResponseEvent(HaInfo info, String dbGroupName) throws Exception {
        //dbGroup_locks events,we only try to response to the DISABLE,ignore others
        if (info.getLockType() == HaInfo.HaType.DISABLE &&
                !info.getStartId().equals(SystemConfig.getInstance().getInstanceName()) &&
                info.getStatus() == HaInfo.HaStatus.SUCCESS) {
            try {
                //start the log
                int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, HaInfo.HaStage.RESPONSE_NOTIFY.toString());
                //try to get the latest status of the dbGroup
                RawJson latestStatus = clusterHelper.getPathValue(ClusterMetaUtil.getHaStatusPath(info.getDbGroupName())).map(ClusterValue::getData).orElse(null);
                //find out the target dbGroup and change it into latest status
                PhysicalDbGroup dbGroup = DbleServer.getInstance().getConfig().getDbGroups().get(info.getDbGroupName());
                dbGroup.changeIntoLatestStatus(latestStatus);
                //response the event ,only disable event has response
                clusterHelper.createSelfTempNode(ClusterPathUtil.getHaResponsePath(dbGroupName), FeedBackType.SUCCESS);
                //ha manager writeOut finish log
                HaConfigManager.getInstance().haFinish(id, null, latestStatus);
            } catch (Exception e) {
                //response the event ,only disable event has response
                clusterHelper.createSelfTempNode(ClusterPathUtil.getHaResponsePath(dbGroupName), FeedBackType.ofError(e.getMessage()));
            }
        }
    }


    public void syncDbGroupStatusToCluster() throws Exception {
        LOGGER.info("syncDbGroupStatusToCluster start");
        HaConfigManager.getInstance().init(false);
        Map<String, RawJson> map = HaConfigManager.getInstance().getSourceJsonList();
        for (Map.Entry<String, RawJson> entry : map.entrySet()) {
            clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(entry.getKey()), entry.getValue());
        }
        LOGGER.info("syncDbGroupStatusToCluster success");
    }

    public void syncDbGroupStatusToCluster(ReloadConfig.ReloadResult reloadResult) throws Exception {
        LOGGER.info("syncDbGroupStatusToCluster start");
        HaConfigManager.getInstance().init(true);
        Map<String, RawJson> dbGroupStatusMap = HaConfigManager.getInstance().getSourceJsonList();

        Map<String, PhysicalDbGroup> recycleHostMap = reloadResult.getRecycleHostMap();
        if (recycleHostMap != null) {
            for (Map.Entry<String, PhysicalDbGroup> groupEntry : recycleHostMap.entrySet()) {
                String dbGroupName = groupEntry.getKey();
                LOGGER.debug("delete dbGroup_status:{}", dbGroupName);
                clusterHelper.cleanKV(ClusterMetaUtil.getHaStatusPath(dbGroupName));
            }
        }
        Map<String, PhysicalDbGroup> addOrChangeHostMap = reloadResult.getAddOrChangeHostMap();
        if (addOrChangeHostMap != null) {
            for (Map.Entry<String, PhysicalDbGroup> groupEntry : addOrChangeHostMap.entrySet()) {
                RawJson dbGroupStatusJson = dbGroupStatusMap.get(groupEntry.getKey());
                LOGGER.debug("add dbGroup_status:{}---{}", groupEntry.getKey(), dbGroupStatusJson);
                clusterHelper.setKV(ClusterMetaUtil.getHaStatusPath(groupEntry.getKey()), dbGroupStatusJson);
            }
        }
        LOGGER.info("syncDbGroupStatusToCluster success");
    }

    void syncHaStatusFromCluster(Gson gson, DbGroups dbs, List<DBGroup> dbGroupList) {
        try {
            List<ClusterEntry<RawJson>> statusKVList = this.getKVBeanOfChildPath(ClusterChildMetaUtil.getHaStatusPath());
            if (statusKVList.size() > 0) {
                Map<String, DBGroup> dbGroupMap = this.changeFromListToMap(dbGroupList);
                for (ClusterEntry<RawJson> kv : statusKVList) {
                    String[] path = kv.getKey().split(ClusterPathUtil.SEPARATOR);
                    String dbGroupName = path[path.length - 1];
                    DBGroup dbGroup = dbGroupMap.get(dbGroupName);
                    changStatusByJson(gson, dbs, dbGroupList, dbGroupName, dbGroup, kv.getValue().getData());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("syncHaStatusFromCluster error :", e);
        }
    }

    public Map<String, DBGroup> changeFromListToMap(List<DBGroup> dbGroupList) {
        Map<String, DBGroup> dbGroupMap = new HashMap<>(dbGroupList.size());
        for (DBGroup dbGroup : dbGroupList) {
            dbGroupMap.put(dbGroup.getName(), dbGroup);
        }
        return dbGroupMap;
    }


    private static void changeDbGroupByStatus(DBGroup dbGroup, List<DbInstanceStatus> statusList) {
        Map<String, DbInstanceStatus> statusMap = new HashMap<>(statusList.size());
        for (DbInstanceStatus status : statusList) {
            statusMap.put(status.getName(), status);
        }
        for (DBInstance instance : dbGroup.getDbInstance()) {
            DbInstanceStatus status = statusMap.get(instance.getName());
            if (null != status) {
                instance.setPrimary(status.isPrimary());
                instance.setDisabled(status.isDisable() ? "true" : "false");
            }
        }
    }

    private static void changStatusByJson(Gson gson, DbGroups dbs, List<DBGroup> dbGroupList, String dbGroupName, DBGroup dbGroup, RawJson data) {
        if (dbGroup != null) {
            JsonObject jsonStatusObject = data.getJsonObject();
            JsonElement instanceJson = jsonStatusObject.get(JSON_LIST);
            if (instanceJson != null) {
                List<DbInstanceStatus> list = gson.fromJson(instanceJson.toString(),
                        new TypeToken<List<DbInstanceStatus>>() {
                        }.getType());
                dbs.setDbGroup(dbGroupList);
                changeDbGroupByStatus(dbGroup, list);
            }
        } else {
            LOGGER.warn("dbGroup " + dbGroupName + " is not found");
        }
    }


}
