/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config.util;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.VersionUtil;
import com.oceanbase.obsharding_d.config.ConfigInitializer;
import com.oceanbase.obsharding_d.config.OBsharding_DTempConfig;
import com.oceanbase.obsharding_d.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.oceanbase.obsharding_d.config.helper.KeyVariables;
import com.oceanbase.obsharding_d.config.model.MysqlVersion;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.db.type.DataBaseType;
import com.oceanbase.obsharding_d.config.model.user.RwSplitUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.services.manager.response.ChangeItem;
import com.oceanbase.obsharding_d.services.manager.response.ChangeItemType;
import com.oceanbase.obsharding_d.services.manager.response.ChangeType;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.util.Strings;
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

    public static boolean isAllDbInstancesChange(List<ChangeItem> changeItemList) {
        if (changeItemList.size() == 0) return false;

        Map<String, PhysicalDbInstance> oldDbInstanceMaps = new HashMap<>();
        OBsharding_DServer.getInstance().getConfig().getDbGroups()
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

    private static List<String> getClickHouseSyncKeyVariables(Set<PhysicalDbInstance> ckDbInstances, boolean isAllChange, boolean needSync, boolean existMysql) throws Exception {
        if (ckDbInstances.size() == 0)
            return new ArrayList<>();
        Boolean lowerCase = (isAllChange && !existMysql) ? null : OBsharding_DServer.getInstance().getConfig().isLowerCase();
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
        Boolean lowerCase = isAllChange ? null : OBsharding_DServer.getInstance().getConfig().isLowerCase();
        boolean reInitLowerCase = false;
        Set<String> leftGroup = new HashSet<>();
        Set<String> rightGroup = new HashSet<>();
        int minNodePacketSize = Integer.MAX_VALUE;
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
            msg = "OBsharding-D's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msgList.add(msg);
            LOGGER.warn(msg);
        }

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
        OBsharding_DTempConfig.getInstance().setLowerCase(lowerCase);
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
        for (Map.Entry<VariableMapKey, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
            Future<KeyVariables> future = entry.getValue();
            KeyVariables keyVariables = future.get();
            if (keyVariables != null) {
                minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());
            }
        }
        // maxPacketSize
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "OBsharding-D's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msgList.add(msg);
            LOGGER.warn(msg);
        }
        dbInstanceList.forEach(dbInstance -> dbInstance.setNeedSkipHeartTest(true));
        OBsharding_DTempConfig.getInstance().setLowerCase(lowerCaseA);
        return msgList;
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
                throw new ConfigException("this dbInstance[=" + instance.getConfig().getUrl() + "]'s version[=" + version + "] cannot be lower than the OBsharding-D version[=" + SystemConfig.getInstance().getFakeMySQLVersion() + "],pls check the backend " + type + " node.");
            }
            return isMatch;
        }
        //not referenced
        return true;
    }

    /**
     * check OBsharding-D-mysql version<p>
     * 1.transaction_isolation/transaction_read_only(com.mysql.jdbc.ConnectionImpl.getTransactionIsolation): 5.7.20 <= version <= 8.0.0 || version >= 8.0.3  <p>
     * 2.query_cache(com.mysql.jdbc.ConnectionImpl.loadServerVariables): version < 8.0.3 <p>
     * OBsharding-D and mysql versions meet the above requirements, such as:<p>
     * ✔: OBsharding-D-version：5.7.20 mysql-version：8.0.0<p>
     * ✘: OBsharding-D-version：5.7.20 mysql-version：8.0.3<p>
     * ✔: OBsharding-D-version：8.0.3 mysql-version：8.0.23<p>
     * ✘: OBsharding-D-version：8.0.3 mysql-version：8.0.1<p>
     * ✔: OBsharding-D-version：5.7.15 mysql-version：8.0.1<p>
     * ✘: OBsharding-D-version：5.7.15 mysql-version：5.7.25
     */
    private static boolean checkVersionWithRwSplit(String version, PhysicalDbInstance instance, boolean isThrowException, String type) {
        if (StringUtil.isBlank(version)) return false;
        LOGGER.debug("check version: OBsharding-D-version[{}], mysql-version[{}]", SystemConfig.getInstance().getFakeMySQLVersion(), version);
        MysqlVersion mysqlVersion = VersionUtil.parseVersion(version);
        MysqlVersion OBsharding_DVersion = SystemConfig.getInstance().getMysqlVersion();
        boolean mysqlFlag0 = VersionUtil.versionMeetsMinimum(5, 7, 20, mysqlVersion) && !VersionUtil.versionMeetsMinimum(8, 0, 0, mysqlVersion);
        boolean mysqlFlag1 = VersionUtil.versionMeetsMinimum(8, 0, 3, mysqlVersion);
        boolean OBsharding_DFlag0 = VersionUtil.versionMeetsMinimum(5, 7, 20, OBsharding_DVersion) && !VersionUtil.versionMeetsMinimum(8, 0, 0, OBsharding_DVersion);
        boolean OBsharding_DFlag1 = VersionUtil.versionMeetsMinimum(8, 0, 3, OBsharding_DVersion);
        boolean isMatch = mysqlFlag0 == OBsharding_DFlag0 && mysqlFlag1 == OBsharding_DFlag1;
        if (!isMatch && isThrowException) {
            throw new ConfigException("the OBsharding-D version[=" + SystemConfig.getInstance().getFakeMySQLVersion() + "] and " + type + "[" + instance.getConfig().getUrl() + "] version[=" + version + "] not match, Please check the version.");
        }
        return isMatch;
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

    public static void checkOBsharding_DAndMysqlVersion(List<ChangeItem> changeItemList, ConfigInitializer newConfigLoader) {
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

    public static void checkOBsharding_DAndMysqlVersion(Map<String, PhysicalDbGroup> newDbGroups) {
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
