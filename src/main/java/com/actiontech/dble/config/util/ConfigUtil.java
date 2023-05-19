/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.ConfigInitializer;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.MysqlVersion;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.services.manager.response.ChangeItem;
import com.actiontech.dble.services.manager.response.ChangeItemType;
import com.actiontech.dble.services.manager.response.ChangeType;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

    public static String getAndSyncKeyVariables(List<ChangeItem> changeItemList, boolean needSync) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("sync-key-variables");
        try {
            String msg = null;
            List<ChangeItem> needCheckItemList = changeItemList.stream()
                    //add dbInstance or add dbGroup or (update dbInstance and need testConn)
                    .filter(changeItem -> ((changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE || changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_GROUP) &&
                            changeItem.getType() == ChangeType.ADD) ||
                            (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE && changeItem.getType() == ChangeType.UPDATE && changeItem.isAffectTestConn()))
                    .collect(Collectors.toList());
            if (changeItemList.size() == 0 || needCheckItemList.isEmpty()) {
                //with no dbGroups, do not check the variables
                return null;
            }
            Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap = Maps.newHashMap();
            List<PhysicalDbInstance> dbInstanceList = Lists.newArrayList();
            getAndSyncKeyVariablesForDataSources(needCheckItemList, keyVariablesTaskMap, needSync, dbInstanceList);

            Set<String> diffGroup = new HashSet<>();
            int minNodePacketSize = Integer.MAX_VALUE;
            Boolean lowerCase = DbleServer.getInstance().getConfig().isLowerCase();
            for (Map.Entry<VariableMapKey, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
                VariableMapKey variableMapKey = entry.getKey();
                Future<KeyVariables> future = entry.getValue();
                KeyVariables keyVariables = future.get();
                if (keyVariables != null) {
                    if (dbInstanceList.size() == 1 || lowerCase == null) {
                        lowerCase = keyVariables.isLowerCase();
                    } else if (keyVariables.isLowerCase() != lowerCase && dbInstanceList.size() > 1) {
                        diffGroup.add(variableMapKey.getDataSourceName());
                    }
                    minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());
                }
            }
            if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
                SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
                msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
                LOGGER.warn(msg);
            }

            if (diffGroup.size() != 0) {
                // if all datasoure's lower case are not equal, throw exception
                StringBuilder sb = new StringBuilder("The values of lower_case_table_names for backend MySQLs are different.");
                sb.append("These previous MySQL's value is");
                sb.append(DbleServer.getInstance().getConfig().isLowerCase() ? " not 0" : " 0");
                sb.append(".but these MySQL's [");
                sb.append(Strings.join(diffGroup, ','));
                sb.append("] value is");
                sb.append(DbleServer.getInstance().getConfig().isLowerCase() ? " 0" : " not 0");
                sb.append(".");
                throw new IOException(sb.toString());
            }
            dbInstanceList.forEach(dbInstance -> dbInstance.setNeedSkipHeartTest(true));
            DbleTempConfig.getInstance().setLowerCase(lowerCase);
            return msg;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public static List<String> getAndSyncKeyVariables(Map<String, PhysicalDbGroup> dbGroups, boolean needSync) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("sync-key-variables");
        try {
            Map<String, PhysicalDbGroup> mysqlDbGroups = new HashMap<>();
            Map<String, PhysicalDbGroup> clickHouseDbGroups = new HashMap<>();
            dbGroups.forEach((k, v) -> {
                if (v.getDbGroupConfig().instanceDatabaseType() == DataBaseType.MYSQL) {
                    mysqlDbGroups.put(k, v);
                } else {
                    clickHouseDbGroups.put(k, v);
                }
            });

            List<String> syncKeyVariables = Lists.newArrayList();
            List<String> mysqlSyncKeyVariables = getMysqlSyncKeyVariables(mysqlDbGroups, needSync);
            Optional.ofNullable(mysqlSyncKeyVariables).ifPresent(syncKeyVariables::addAll);
            List<String> clickHouseSyncKeyVariables = getClickHouseSyncKeyVariables(clickHouseDbGroups, needSync);
            Optional.ofNullable(clickHouseSyncKeyVariables).ifPresent(syncKeyVariables::addAll);
            return syncKeyVariables;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    @Nullable
    private static List<String> getMysqlSyncKeyVariables(Map<String, PhysicalDbGroup> dbGroups, boolean needSync) throws InterruptedException, ExecutionException, IOException {
        String msg = null;
        List<String> list = new ArrayList<>();
        if (dbGroups.size() == 0) {
            //with no dbGroups, do not check the variables
            return list;
        }
        Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(dbGroups.size());
        List<PhysicalDbInstance> dbInstanceList = Lists.newArrayList();
        getAndSyncKeyVariablesForDataSources(dbGroups, keyVariablesTaskMap, needSync, dbInstanceList);

        boolean lowerCase = false;
        boolean isFirst = true;
        Set<String> firstGroup = new HashSet<>();
        Set<String> secondGroup = new HashSet<>();
        int minNodePacketSize = Integer.MAX_VALUE;
        for (Map.Entry<VariableMapKey, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
            VariableMapKey variableMapKey = entry.getKey();
            Future<KeyVariables> future = entry.getValue();
            KeyVariables keyVariables = future.get();
            if (keyVariables != null) {
                if (isFirst) {
                    lowerCase = keyVariables.isLowerCase();
                    isFirst = false;
                    firstGroup.add(variableMapKey.getDataSourceName());
                } else if (keyVariables.isLowerCase() != lowerCase) {
                    secondGroup.add(variableMapKey.getDataSourceName());
                }
                minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());
                PhysicalDbInstance instance = variableMapKey.getDbInstance();

                // The back_log value indicates how many requests can be stacked during this short time before MySQL momentarily stops answering new requests
                int minCon = instance.getConfig().getMinCon();
                int backLog = keyVariables.getBackLog();
                if (backLog < minCon) {
                    msg = "dbGroup[" + instance.getDbGroup().getGroupName() + "," + instance.getName() + "] the value of back_log may too small, current value is " + backLog + ", recommended value is " + minCon;
                    list.add(msg);
                }
            }
        }
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            list.add(msg);
            LOGGER.warn(msg);
        }
        if (secondGroup.size() != 0) {
            // if all datasoure's lower case are not equal, throw exception
            StringBuilder sb = new StringBuilder("The values of lower_case_table_names for backend MySQLs are different.");
            String firstGroupValue;
            String secondGroupValue;
            if (lowerCase) {
                firstGroupValue = " not 0 :";
                secondGroupValue = " 0 :";
            } else {
                firstGroupValue = " 0 :";
                secondGroupValue = " not 0 :";
            }
            sb.append("These MySQL's value is");
            sb.append(firstGroupValue);
            sb.append(Strings.join(firstGroup, ','));
            sb.append(".And these MySQL's value is");
            sb.append(secondGroupValue);
            sb.append(Strings.join(secondGroup, ','));
            sb.append(".");
            throw new IOException(sb.toString());
        }
        dbInstanceList.forEach(dbInstance -> dbInstance.setNeedSkipHeartTest(true));
        DbleTempConfig.getInstance().setLowerCase(lowerCase);
        return list;
    }

    public static boolean checkMysqlVersion(String version, PhysicalDbInstance instance, boolean isThrowException) {
        String type = instance.getDbGroupConfig().instanceDatabaseType().toString();
        Integer majorVersion = VersionUtil.getMajorVersionWithoutDefaultValue(version);
        if (majorVersion == null) {
            LOGGER.warn("the backend {} server version  [{}] is unrecognized, we will treat as default official  {} version 5.*. ", type, type, version);
            majorVersion = 5;
        }
        if (!instance.getDbGroup().isRwSplitUseless()) {
            //rw-split
            return checkVersionWithRwSplit(version, instance, isThrowException, type);
        } else if (!instance.getDbGroup().isShardingUseless() || !instance.getDbGroup().isAnalysisUseless()) {
            //sharding or analysis
            boolean isMatch = majorVersion >= VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion());
            if (!isMatch && isThrowException) {
                throw new ConfigException("this dbInstance[=" + instance.getConfig().getUrl() + "]'s version[=" + version + "] cannot be lower than the dble version[=" + SystemConfig.getInstance().getFakeMySQLVersion() + "],pls check the backend " + type + " node.");
            }
            return isMatch;
        }
        //not referenced
        return true;
    }

    /**
     * check dble-mysql version<p>
     * 1.transaction_isolation/transaction_read_only(com.mysql.jdbc.ConnectionImpl.getTransactionIsolation): 5.7.20 <= version <= 8.0.0 || version >= 8.0.3  <p>
     * 2.query_cache(com.mysql.jdbc.ConnectionImpl.loadServerVariables): version < 8.0.3 <p>
     * dble and mysql versions meet the above requirements, such as:<p>
     * ✔: dble-version：5.7.20 mysql-version：8.0.0<p>
     * ✘: dble-version：5.7.20 mysql-version：8.0.3<p>
     * ✔: dble-version：8.0.3 mysql-version：8.0.23<p>
     * ✘: dble-version：8.0.3 mysql-version：8.0.1<p>
     * ✔: dble-version：5.7.15 mysql-version：8.0.1<p>
     * ✘: dble-version：5.7.15 mysql-version：5.7.25
     */
    private static boolean checkVersionWithRwSplit(String version, PhysicalDbInstance instance, boolean isThrowException, String type) {
        if (StringUtil.isBlank(version)) return false;
        LOGGER.debug("check version: dble-version[{}], mysql-version[{}]", SystemConfig.getInstance().getFakeMySQLVersion(), version);
        MysqlVersion mysqlVersion = VersionUtil.parseVersion(version);
        MysqlVersion dbleVersion = SystemConfig.getInstance().getMysqlVersion();
        boolean mysqlFlag0 = VersionUtil.versionMeetsMinimum(5, 7, 20, mysqlVersion) && !VersionUtil.versionMeetsMinimum(8, 0, 0, mysqlVersion);
        boolean mysqlFlag1 = VersionUtil.versionMeetsMinimum(8, 0, 3, mysqlVersion);
        boolean dbleFlag0 = VersionUtil.versionMeetsMinimum(5, 7, 20, dbleVersion) && !VersionUtil.versionMeetsMinimum(8, 0, 0, dbleVersion);
        boolean dbleFlag1 = VersionUtil.versionMeetsMinimum(8, 0, 3, dbleVersion);
        boolean isMatch = mysqlFlag0 == dbleFlag0 && mysqlFlag1 == dbleFlag1;
        if (!isMatch && isThrowException) {
            throw new ConfigException("the dble version[=" + SystemConfig.getInstance().getFakeMySQLVersion() + "] and " + type + "[" + instance.getConfig().getUrl() + "] version[=" + version + "] not match, Please check the version.");
        }
        return isMatch;
    }

    @Nullable
    private static List<String> getClickHouseSyncKeyVariables(Map<String, PhysicalDbGroup> dbGroups, boolean needSync) throws InterruptedException, ExecutionException, IOException {
        String msg = null;
        List<String> list = new ArrayList<>();
        if (dbGroups.size() == 0) {
            //with no dbGroups, do not check the variables
            return list;
        }
        Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(dbGroups.size());
        List<PhysicalDbInstance> dbInstanceList = Lists.newArrayList();
        getAndSyncKeyVariablesForDataSources(dbGroups, keyVariablesTaskMap, needSync, dbInstanceList);

        boolean lowerCase = false;
        boolean isFirst = true;
        Set<String> firstGroup = new HashSet<>();
        Set<String> secondGroup = new HashSet<>();
        int minNodePacketSize = Integer.MAX_VALUE;
        for (Map.Entry<VariableMapKey, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
            VariableMapKey variableMapKey = entry.getKey();
            Future<KeyVariables> future = entry.getValue();
            KeyVariables keyVariables = future.get();
            if (keyVariables != null) {
                if (isFirst) {
                    lowerCase = keyVariables.isLowerCase();
                    isFirst = false;
                    firstGroup.add(variableMapKey.getDataSourceName());
                } else if (keyVariables.isLowerCase() != lowerCase) {
                    secondGroup.add(variableMapKey.getDataSourceName());
                }
                minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());
            }
        }
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            list.add(msg);
            LOGGER.warn(msg);
        }
        if (secondGroup.size() != 0) {
            // if all datasoure's lower case are not equal, throw exception
            StringBuilder sb = new StringBuilder("The values of lower_case_table_names for backend clickHouse are different.");
            String firstGroupValue;
            String secondGroupValue;
            if (lowerCase) {
                firstGroupValue = " not 0 :";
                secondGroupValue = " 0 :";
            } else {
                firstGroupValue = " 0 :";
                secondGroupValue = " not 0 :";
            }
            sb.append("These clickHouse's value is");
            sb.append(firstGroupValue);
            sb.append(Strings.join(firstGroup, ','));
            sb.append(".And these clickHouse's value is");
            sb.append(secondGroupValue);
            sb.append(Strings.join(secondGroup, ','));
            sb.append(".");
            throw new IOException(sb.toString());
        }
        dbInstanceList.forEach(dbInstance -> dbInstance.setNeedSkipHeartTest(true));
        DbleTempConfig.getInstance().setLowerCase(lowerCase);
        return list;
    }


    private static void getAndSyncKeyVariablesForDataSources(List<ChangeItem> changeItemList, Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap,
                                                             boolean needSync, List<PhysicalDbInstance> dbInstanceList) throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(changeItemList.size());
        for (ChangeItem changeItem : changeItemList) {
            Object item = changeItem.getItem();
            if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE) {
                PhysicalDbInstance ds = (PhysicalDbInstance) item;
                if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                    continue;
                }
                getKeyVariablesForDataSource(service, ds, ds.getDbGroupConfig().getName(), keyVariablesTaskMap, needSync);
                dbInstanceList.add(ds);
            } else if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_GROUP) {
                PhysicalDbGroup dbGroup = (PhysicalDbGroup) item;
                for (PhysicalDbInstance ds : dbGroup.getAllDbInstanceMap().values()) {
                    if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                        continue;
                    }
                    getKeyVariablesForDataSource(service, ds, ds.getDbGroupConfig().getName(), keyVariablesTaskMap, needSync);
                    dbInstanceList.add(ds);
                }
            }
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

    private static void getAndSyncKeyVariablesForDataSources(Map<String, PhysicalDbGroup> dbGroups, Map<VariableMapKey, Future<KeyVariables>> keyVariablesTaskMap,
                                                             boolean needSync, List<PhysicalDbInstance> dbInstanceList) throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(dbGroups.size());
        for (Map.Entry<String, PhysicalDbGroup> entry : dbGroups.entrySet()) {
            String hostName = entry.getKey();
            PhysicalDbGroup pool = entry.getValue();

            for (PhysicalDbInstance ds : pool.getDbInstances(true)) {
                if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                    continue;
                }
                getKeyVariablesForDataSource(service, ds, hostName, keyVariablesTaskMap, needSync);
                dbInstanceList.add(ds);
            }
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

    public static void checkDbleAndMysqlVersion(List<ChangeItem> changeItemList, ConfigInitializer newConfigLoader) {
        List<ChangeItem> needCheckVersionList = changeItemList.stream()
                //add dbInstance/dbGroup/rwSplitUser/shardingNode or (update dbInstance and need testConn) or (update rwSplitUser and affectEntryDbGroup=true) or update shardingNode
                .filter(changeItem -> (changeItem.getType() == ChangeType.ADD) ||
                        (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE && changeItem.getType() == ChangeType.UPDATE && changeItem.isAffectTestConn()) ||
                        (changeItem.getItemType() == ChangeItemType.USERNAME && changeItem.getType() == ChangeType.UPDATE && changeItem.isAffectEntryDbGroup()) ||
                        (changeItem.getItemType() == ChangeItemType.SHARDING_NODE && changeItem.getType() == ChangeType.UPDATE))
                .collect(Collectors.toList());

        if (changeItemList.size() == 0 || needCheckVersionList.isEmpty()) {
            //with no item, do not check the version
            return;
        }
        for (ChangeItem changeItem : needCheckVersionList) {
            Object item = changeItem.getItem();
            if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_INSTANCE) {
                PhysicalDbInstance ds = (PhysicalDbInstance) item;
                if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                    continue;
                }
                //check mysql version
                checkMysqlVersion(ds.getDsVersion(), ds, true);
            } else if (changeItem.getItemType() == ChangeItemType.PHYSICAL_DB_GROUP) {
                PhysicalDbGroup dbGroup = (PhysicalDbGroup) item;
                checkDbGroupVersion(dbGroup);
            } else if (changeItem.getItemType() == ChangeItemType.SHARDING_NODE) {
                ShardingNode shardingNode = (ShardingNode) item;
                PhysicalDbGroup dbGroup = shardingNode.getDbGroup();
                checkDbGroupVersion(dbGroup);
            } else if (changeItem.getItemType() == ChangeItemType.USERNAME) {
                UserConfig userConfig = newConfigLoader.getUsers().get(item);
                if (userConfig instanceof RwSplitUserConfig) {
                    RwSplitUserConfig rwSplitUserConfig = (RwSplitUserConfig) userConfig;
                    PhysicalDbGroup dbGroup = newConfigLoader.getDbGroups().get(rwSplitUserConfig.getDbGroup());
                    checkDbGroupVersion(dbGroup);
                }
            }
        }
    }

    public static void checkDbleAndMysqlVersion(Map<String, PhysicalDbGroup> newDbGroups) {
        if (newDbGroups.isEmpty()) {
            return;
        }
        for (PhysicalDbGroup dbGroup : newDbGroups.values()) {
            checkDbGroupVersion(dbGroup);
        }
    }

    public static void checkDbGroupVersion(PhysicalDbGroup dbGroup) {
        for (PhysicalDbInstance ds : dbGroup.getAllDbInstanceMap().values()) {
            if (ds.isDisabled() || !ds.isTestConnSuccess() || ds.isFakeNode()) {
                continue;
            }
            //check mysql version
            checkMysqlVersion(ds.getDsVersion(), ds, true);
        }
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
