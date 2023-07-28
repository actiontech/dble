/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.services.manager.response.ChangeItem;
import com.actiontech.dble.services.manager.response.ChangeItemType;
import com.actiontech.dble.services.manager.response.ChangeType;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.CollectionUtil;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public final class ConfigUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtil.class);

    private ConfigUtil() {
    }

    public static String filter(String text) {
        return filter(text, System.getProperties());
    }

    public static String filter(String text, Properties properties) {
        StringBuilder s = new StringBuilder();
        int cur = 0;
        int textLen = text.length();
        int propStart;
        int propStop;
        String propName;
        String propValue;
        for (; cur < textLen; cur = propStop + 1) {
            propStart = text.indexOf("${", cur);
            if (propStart < 0) {
                break;
            }
            s.append(text, cur, propStart);
            propStop = text.indexOf("}", propStart);
            if (propStop < 0) {
                throw new ConfigException("Unterminated property: " + text.substring(propStart));
            }
            propName = text.substring(propStart + 2, propStop);
            propValue = properties.getProperty(propName);
            if (propValue == null) {
                s.append("${").append(propName).append('}');
            } else {
                s.append(propValue);
            }
        }
        return s.append(text.substring(cur)).toString();
    }

    public static void setSchemasForPool(Map<String, PhysicalDbGroup> dbGroupMap, Map<String, ShardingNode> shardingNodeMap) {
        for (PhysicalDbGroup dbGroup : dbGroupMap.values()) {
            dbGroup.setSchemas(getShardingNodeSchemasOfDbGroup(dbGroup.getGroupName(), shardingNodeMap));
        }
    }

    private static ArrayList<String> getShardingNodeSchemasOfDbGroup(String dbGroup, Map<String, ShardingNode> shardingNodeMap) {
        ArrayList<String> schemaList = new ArrayList<>(30);
        for (ShardingNode dn : shardingNodeMap.values()) {
            if (dn.getDbGroup() != null && dn.getDbGroup().getGroupName().equals(dbGroup)) {
                schemaList.add(dn.getDatabase());
            }
        }
        return schemaList;
    }

    public static boolean isAllDbInstancesChange(List<ChangeItem> changeItemList) {
        if (changeItemList.size() == 0) return false;

        Map<String, PhysicalDbInstance> oldDbInstanceMaps = new HashMap<>();
        DbleServer.getInstance().getConfig().getDbGroups()
                .values().stream().forEach(group -> group.getAllDbInstanceMap()
                .values().stream().forEach(db -> {
                    oldDbInstanceMaps.put(genDataSourceKey(group.getGroupName(), db.getName()), db);
                }));

        if (CollectionUtil.isEmpty(oldDbInstanceMaps)) return false;

        for (ChangeItem changeItem : changeItemList) {
            switch (changeItem.getItemType()) {
                case PHYSICAL_DB_GROUP:
                    if (changeItem.getType() == ChangeType.DELETE) {
                        ((PhysicalDbGroup) changeItem.getItem()).getAllDbInstanceMap().values().stream().forEach(db -> {
                            oldDbInstanceMaps.remove(genDataSourceKey(db.getDbGroupConfig().getName(), db.getName()));
                        });
                    }
                    break;
                case PHYSICAL_DB_INSTANCE:
                    if ((changeItem.getType() == ChangeType.UPDATE || changeItem.getType() == ChangeType.DELETE)) {
                        PhysicalDbInstance db = ((PhysicalDbInstance) changeItem.getItem());
                        oldDbInstanceMaps.remove(genDataSourceKey(db.getDbGroupConfig().getName(), db.getName()));
                    }
                    break;
                default:
                    break;
            }
        }
        oldDbInstanceMaps.values().removeIf(db -> db.isDisabled() || !db.isTestConnSuccess() || db.isFakeNode());
        return CollectionUtil.isEmpty(oldDbInstanceMaps);
    }

    public static List<String> getAndSyncKeyVariables(Map<String, PhysicalDbGroup> dbGroups, boolean needSync) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("sync-key-variables");
        try {
            Set<PhysicalDbInstance> mysqlDbInstances = new HashSet<>();
            Set<PhysicalDbInstance> ckDbInstances = new HashSet<>();
            for (Map.Entry<String, PhysicalDbGroup> entry : dbGroups.entrySet()) {
                for (PhysicalDbInstance ds : entry.getValue().getDbInstances(true)) {
                    if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                        continue;
                    }
                    if (ds.getConfig().getDataBaseType() == DataBaseType.MYSQL) {
                        mysqlDbInstances.add(ds);
                    } else {
                        ckDbInstances.add(ds);
                    }
                }
            }

            List<String> syncKeyVariables = Lists.newArrayList();
            List<String> clickHouseSyncKeyVariables = getClickHouseSyncKeyVariables(ckDbInstances, true, needSync, !CollectionUtil.isEmpty(mysqlDbInstances));
            syncKeyVariables.addAll(clickHouseSyncKeyVariables);
            List<String> mysqlSyncKeyVariables = getMysqlSyncKeyVariables(mysqlDbInstances, true, needSync, !CollectionUtil.isEmpty(ckDbInstances));
            syncKeyVariables.addAll(mysqlSyncKeyVariables);
            return syncKeyVariables;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public static List<String> getAndSyncKeyVariables(List<ChangeItem> changeItemList, boolean needSync) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("sync-key-variables");
        try {
            Set<PhysicalDbInstance> mysqlDbInstances = new HashSet<>();
            Set<PhysicalDbInstance> ckDbInstances = new HashSet<>();

            for (ChangeItem changeItem : changeItemList) {
                // add dbInstance or add dbGroup or (update dbInstance and need testConn)
                if (((changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE || changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_GROUP) &&
                        changeItem.getType() == ChangeType.ADD) ||
                        (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE && changeItem.getType() == ChangeType.UPDATE && changeItem.isAffectTestConn())) {

                    // filter not available
                    if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE) {
                        PhysicalDbInstance ds = (PhysicalDbInstance) changeItem.getItem();
                        if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                            continue;
                        }
                        if (ds.getConfig().getDataBaseType() == DataBaseType.MYSQL) {
                            mysqlDbInstances.add(ds);
                        } else {
                            ckDbInstances.add(ds);
                        }
                    } else if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_GROUP) {
                        PhysicalDbGroup dbGroup = (PhysicalDbGroup) changeItem.getItem();
                        for (PhysicalDbInstance ds : dbGroup.getAllDbInstanceMap().values()) {
                            if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                                continue;
                            }
                            if (ds.getConfig().getDataBaseType() == DataBaseType.MYSQL) {
                                mysqlDbInstances.add(ds);
                            } else {
                                ckDbInstances.add(ds);
                            }
                        }
                    }
                }
            }

            List<String> syncKeyVariables = Lists.newArrayList();
            List<String> clickHouseSyncKeyVariables = getClickHouseSyncKeyVariables(ckDbInstances, false, needSync, !CollectionUtil.isEmpty(mysqlDbInstances));
            syncKeyVariables.addAll(clickHouseSyncKeyVariables);
            List<String> mysqlSyncKeyVariables = getMysqlSyncKeyVariables(mysqlDbInstances, false, needSync, !CollectionUtil.isEmpty(ckDbInstances));
            syncKeyVariables.addAll(mysqlSyncKeyVariables);
            return syncKeyVariables;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private static List<String> getMysqlSyncKeyVariables(Set<PhysicalDbInstance> mysqlDbInstances, boolean isAllChange, boolean needSync, boolean existClickHouse) throws InterruptedException, ExecutionException, IOException {
        if (mysqlDbInstances.size() == 0)
            return new ArrayList<>();
        Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(mysqlDbInstances.size());
        getAndSyncKeyVariablesForDataSources(mysqlDbInstances, keyVariablesTaskMap, needSync);
        return diffMysqlKeyVariables(keyVariablesTaskMap, mysqlDbInstances, isAllChange, existClickHouse);
    }

    private static List<String> getClickHouseSyncKeyVariables(Set<PhysicalDbInstance> ckDbInstances, boolean isAllChange, boolean needSync, boolean existMysql) throws InterruptedException, ExecutionException, IOException {
        if (ckDbInstances.size() == 0)
            return new ArrayList<>();
        Boolean lowerCase = (isAllChange && !existMysql) ? null : DbleServer.getInstance().getConfig().isLowerCase();
        if (lowerCase != null && lowerCase) {
            StringBuilder sb = new StringBuilder();
            sb.append("The configuration add Clickhouse. Since clickhouse is not case sensitive, so the values of lower_case_table_names for previous dbInstances must be 0.");
            throw new IOException(sb.toString());
        }

        Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(ckDbInstances.size());
        getAndSyncKeyVariablesForDataSources(ckDbInstances, keyVariablesTaskMap, needSync);
        return diffClickHouseKeyVariables(keyVariablesTaskMap, ckDbInstances);
    }

    private static List<String> diffMysqlKeyVariables(Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap,
                                                      Set<PhysicalDbInstance> dbInstanceList,
                                                      boolean isAllChange,
                                                      boolean existClickHouse) throws InterruptedException, ExecutionException, IOException { // Mysql
        List<String> msgList = new ArrayList<>();
        if (CollectionUtil.isEmpty(keyVariablesTaskMap))
            return msgList;

        String msg;
        Boolean lowerCase = isAllChange ? null : DbleServer.getInstance().getConfig().isLowerCase();
        boolean reInitLowerCase = false;
        Set<String> leftGroup = new HashSet<>();
        Set<String> rightGroup = new HashSet<>();
        int minNodePacketSize = Integer.MAX_VALUE;
        int minVersion = VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion());
        for (Map.Entry<VariableMapKey, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
            VariableMapKey variableMapKey = entry.getKey();
            Future<KeyVariables> future = entry.getValue();
            KeyVariables keyVariables = future.get();
            if (keyVariables != null) {
                // lowerCase
                if (lowerCase == null) {
                    reInitLowerCase = true;
                    lowerCase = keyVariables.isLowerCase();
                    leftGroup.add(variableMapKey.getDataSourceName());
                } else if (keyVariables.isLowerCase() == lowerCase) {
                    leftGroup.add(variableMapKey.getDataSourceName());
                } else {
                    rightGroup.add(variableMapKey.getDataSourceName());
                }

                minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());

                // version
                Integer majorVersion = VersionUtil.getMajorVersionWithoutDefaultValue(keyVariables.getVersion());
                if (majorVersion == null) {
                    LOGGER.warn("the backend mysql server version  [{}] is unrecognized, we will treat as default official  mysql version 5.*. ", keyVariables.getVersion());
                    majorVersion = 5;
                }
                minVersion = Math.min(minVersion, majorVersion);

                // The back_log value indicates how many requests can be stacked during this short time before MySQL momentarily stops answering new requests
                PhysicalDbInstance instance = variableMapKey.getDbInstance();
                int minCon = instance.getConfig().getMinCon();
                int backLog = keyVariables.getBackLog();
                if (backLog < minCon) {
                    msg = "dbGroup[" + instance.getDbGroup().getGroupName() + "," + instance.getName() + "] the value of back_log may too small, current value is " + backLog + ", recommended value is " + minCon;
                    msgList.add(msg);
                    LOGGER.warn(msg);
                }
            }
        }

        // maxPacketSize
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msgList.add(msg);
            LOGGER.warn(msg);
        }

        // version
        checkVersion(minVersion, "mysql");

        // if all dbInstance's lower case are not equal, throw exception
        if (rightGroup.size() != 0) {
            StringBuilder sb = new StringBuilder();
            if (existClickHouse) {
                sb.append("The configuration contains Clickhouse. Since clickhouse is not case sensitive, so the values of lower_case_table_names for dbInstances must be 0. ");
            } else {
                sb.append("The values of lower_case_table_names for dbInstances are different. ");
            }
            if (reInitLowerCase) {
                sb.append("These dbInstances's [");
                sb.append(Strings.join(leftGroup, ','));
                sb.append("] value is");
                sb.append(lowerCase ? " not 0" : " 0");
                sb.append(". And these dbInstances's [");
                sb.append(Strings.join(rightGroup, ','));
                sb.append("] value is");
                sb.append(lowerCase ? " 0" : " not 0");
                sb.append(".");
            } else {
                sb.append("These previous dbInstances's value is");
                sb.append(lowerCase ? " not 0" : " 0");
                sb.append(".but these dbInstances's [");
                sb.append(Strings.join(rightGroup, ','));
                sb.append("] value is");
                sb.append(lowerCase ? " 0" : " not 0");
                sb.append(".");
            }
            throw new IOException(sb.toString());
        }

        if (existClickHouse && (lowerCase != null && lowerCase)) {
            throw new IOException("The configuration contains Clickhouse. Since clickhouse is not case sensitive, so the values of lower_case_table_names for all dbInstances must be 0. Current all dbInstances are 1.");
        }
        dbInstanceList.forEach(dbInstance -> dbInstance.setNeedSkipHeartTest(true));
        DbleTempConfig.getInstance().setLowerCase(lowerCase);
        return msgList;
    }

    private static List<String> diffClickHouseKeyVariables(Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap,
                                                           Set<PhysicalDbInstance> dbInstanceList) throws InterruptedException, ExecutionException, IOException { // Mysql
        List<String> msgList = new ArrayList<>();
        if (CollectionUtil.isEmpty(keyVariablesTaskMap))
            return msgList;

        String msg;
        final boolean lowerCaseA = false; // clickhouse is not case sensitive
        int minNodePacketSize = Integer.MAX_VALUE;
        int minVersion = VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion());
        for (Map.Entry<VariableMapKey, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
            Future<KeyVariables> future = entry.getValue();
            KeyVariables keyVariables = future.get();
            if (keyVariables != null) {
                minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());
                Integer majorVersion = VersionUtil.getMajorVersionWithoutDefaultValue(keyVariables.getVersion());
                if (majorVersion == null) {
                    LOGGER.warn("the backend clickhouse server version  [{}] is unrecognized, we will treat as default official  clickhouse version 5.*. ", keyVariables.getVersion());
                    majorVersion = 5;
                }
                minVersion = Math.min(minVersion, majorVersion);
            }
        }
        // maxPacketSize
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msgList.add(msg);
            LOGGER.warn(msg);
        }
        // version
        checkVersion(minVersion, "clickHouse");

        dbInstanceList.forEach(dbInstance -> dbInstance.setNeedSkipHeartTest(true));
        DbleTempConfig.getInstance().setLowerCase(lowerCaseA);
        return msgList;

    }

    private static void checkVersion(int currentMinVersion, String databaseType) throws ConfigException {
        String fakeMySQLVersion = SystemConfig.getInstance().getFakeMySQLVersion();
        if (currentMinVersion < VersionUtil.getMajorVersion(fakeMySQLVersion)) {
            throw new ConfigException("the dble version[=" + fakeMySQLVersion + "] cannot be higher than the minimum version of the backend " + databaseType + " node,pls check the backend " + databaseType + " node.");
        }
    }

    private static void getAndSyncKeyVariablesForDataSources(Set<PhysicalDbInstance> availableDbInstances,
                                                             Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap,
                                                             boolean needSync) throws InterruptedException {

        ExecutorService service = Executors.newFixedThreadPool(availableDbInstances.size());
        for (PhysicalDbInstance ds : availableDbInstances) {
            getKeyVariablesForDataSource(service, ds, ds.getDbGroupConfig().getName(), keyVariablesTaskMap, needSync);
        }
        service.shutdown();
        int i = 0;
        while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
            if (LOGGER.isDebugEnabled()) {
                if (i == 0) {
                    LOGGER.info("wait to get all dbInstances's get key variable");
                }
                i++;
                if (i == 100) { //log every 10 seconds
                    i = 0;
                }
            }
        }
    }

    private static void getKeyVariablesForDataSource(ExecutorService service, PhysicalDbInstance ds, String hostName, Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap, boolean needSync) {
        VariableMapKey key = new VariableMapKey(genDataSourceKey(hostName, ds.getName()), ds);
        GetAndSyncDbInstanceKeyVariables task = new GetAndSyncDbInstanceKeyVariables(ds, needSync);
        Future<KeyVariables> future = service.submit(task);
        keyVariablesTaskMap.put(key, future);
    }

    private static String genDataSourceKey(String hostName, String dsName) {
        return hostName + ":" + dsName;
    }

    protected static class VariableMapKey {
        private final String dataSourceName;
        private final PhysicalDbInstance dbInstance;

        protected VariableMapKey(String dataSourceName, PhysicalDbInstance dbInstance) {
            this.dataSourceName = dataSourceName;
            this.dbInstance = dbInstance;
        }

        protected String getDataSourceName() {
            return dataSourceName;
        }

        protected PhysicalDbInstance getDbInstance() {
            return dbInstance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VariableMapKey that = (VariableMapKey) o;
            return dataSourceName.equals(that.dataSourceName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataSourceName);
        }
    }
}
