/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.util;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.CollectionUtil;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.Nullable;
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

    private static String[] getShardingNodeSchemasOfDbGroup(String dbGroup, Map<String, ShardingNode> shardingNodeMap) {
        ArrayList<String> schemaList = new ArrayList<>(30);
        for (ShardingNode dn : shardingNodeMap.values()) {
            if (dn.getDbGroup() != null && dn.getDbGroup().getGroupName().equals(dbGroup)) {
                schemaList.add(dn.getDatabase());
            }
        }
        return schemaList.toArray(new String[schemaList.size()]);
    }

    public static String getAndSyncKeyVariables(Map<String, PhysicalDbGroup> dbGroups, boolean needSync) throws Exception {
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
            StringBuilder sb = new StringBuilder();
            sb.append(getClickHouseSyncKeyVariables(ckDbInstances, needSync));
            sb.append(getMysqlSyncKeyVariables(mysqlDbInstances, needSync, !CollectionUtil.isEmpty(ckDbInstances)));
            return sb.toString();
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    @Nullable
    private static String getMysqlSyncKeyVariables(Set<PhysicalDbInstance> mysqlDbInstances, boolean needSync, boolean existClickHouse) throws InterruptedException, ExecutionException, IOException {
        if (mysqlDbInstances.size() == 0)
            return null;
        String msg = null;
        Map<String, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(mysqlDbInstances.size());
        getAndSyncKeyVariablesForDataSources(mysqlDbInstances, keyVariablesTaskMap, needSync);

        Boolean lowerCase = null;
        Set<String> leftGroup = new HashSet<>();
        Set<String> rightGroup = new HashSet<>();
        int minNodePacketSize = Integer.MAX_VALUE;
        int minVersion = VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion());
        for (Map.Entry<String, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
            String dataSourceName = entry.getKey();
            Future<KeyVariables> future = entry.getValue();
            KeyVariables keyVariables = future.get();
            if (keyVariables != null) {
                // lowerCase
                if (lowerCase == null) {
                    lowerCase = keyVariables.isLowerCase();
                    leftGroup.add(dataSourceName);
                } else if (keyVariables.isLowerCase() == lowerCase) {
                    leftGroup.add(dataSourceName);
                } else {
                    rightGroup.add(dataSourceName);
                }
                minNodePacketSize = Math.min(minNodePacketSize, keyVariables.getMaxPacketSize());
                Integer majorVersion = VersionUtil.getMajorVersionWithoutDefaultValue(keyVariables.getVersion());
                if (majorVersion == null) {
                    LOGGER.warn("the backend mysql server version  [{}] is unrecognized, we will treat as default official  mysql version 5.*. ", keyVariables.getVersion());
                    majorVersion = 5;
                }
                minVersion = Math.min(minVersion, majorVersion);
            }
        }
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            LOGGER.warn(msg);
        }
        if (minVersion < VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion())) {
            throw new ConfigException("the dble version[=" + SystemConfig.getInstance().getFakeMySQLVersion() + "] cannot be higher than the minimum version of the backend mysql node,pls check the backend mysql node.");
        }
        if (rightGroup.size() != 0) {
            // if all dbInstances's lower case are not equal, throw exception
            StringBuilder sb = new StringBuilder();
            if (existClickHouse) {
                sb.append("The configuration contains Clickhouse. Since clickhouse is not case sensitive, so the values of lower_case_table_names for dbInstances must be 0. ");
            } else {
                sb.append("The values of lower_case_table_names for dbInstances are different. ");
            }
            sb.append("These dbInstances's [");
            sb.append(Strings.join(leftGroup, ','));
            sb.append("] value is");
            sb.append(lowerCase ? " not 0" : " 0");
            sb.append(". And these dbInstances's [");
            sb.append(Strings.join(rightGroup, ','));
            sb.append("] value is");
            sb.append(lowerCase ? " 0" : " not 0");
            sb.append(".");
            throw new IOException(sb.toString());
        }

        if (existClickHouse && (lowerCase != null && lowerCase)) {
            throw new IOException("The configuration contains Clickhouse. Since clickhouse is not case sensitive, so the values of lower_case_table_names for all dbInstances must be 0. Current all dbInstances are 1.");
        }
        return msg;
    }

    @Nullable
    private static String getClickHouseSyncKeyVariables(Set<PhysicalDbInstance> ckDbInstances, boolean needSync) throws InterruptedException, ExecutionException, IOException {
        if (ckDbInstances.size() == 0)
            return null;
        String msg = null;
        Map<String, Future<KeyVariables>> keyVariablesTaskMap = new HashMap<>(ckDbInstances.size());
        getAndSyncKeyVariablesForDataSources(ckDbInstances, keyVariablesTaskMap, needSync);

        // clickhouse is not case sensitive, so ignore
        int minNodePacketSize = Integer.MAX_VALUE;
        int minVersion = VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion());
        for (Map.Entry<String, Future<KeyVariables>> entry : keyVariablesTaskMap.entrySet()) {
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
        if (minNodePacketSize < SystemConfig.getInstance().getMaxPacketSize() + KeyVariables.MARGIN_PACKET_SIZE) {
            SystemConfig.getInstance().setMaxPacketSize(minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            msg = "dble's maxPacketSize will be set to (the min of all dbGroup's max_allowed_packet) - " + KeyVariables.MARGIN_PACKET_SIZE + ":" + (minNodePacketSize - KeyVariables.MARGIN_PACKET_SIZE);
            LOGGER.warn(msg);
        }
        if (minVersion < VersionUtil.getMajorVersion(SystemConfig.getInstance().getFakeMySQLVersion())) {
            throw new ConfigException("the dble version[=" + SystemConfig.getInstance().getFakeMySQLVersion() + "] cannot be higher than the minimum version of the backend clickHouse node,pls check the backend clickHouse node.");
        }
        return msg;
    }


    private static void getAndSyncKeyVariablesForDataSources(Set<PhysicalDbInstance> availableDbInstances,
                                                             Map<String, Future<KeyVariables>> keyVariablesTaskMap,
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
                    LOGGER.debug("wait to get all dbInstances's get key variable");
                }
                i++;
                if (i == 100) { //log every 10 seconds
                    i = 0;
                }
            }
        }
    }

    private static void getKeyVariablesForDataSource(ExecutorService service, PhysicalDbInstance ds, String hostName, Map<String, Future<KeyVariables>> keyVariablesTaskMap, boolean needSync) {
        String dataSourceName = genDataSourceKey(hostName, ds.getName());
        GetAndSyncDbInstanceKeyVariables task = new GetAndSyncDbInstanceKeyVariables(ds, needSync);
        Future<KeyVariables> future = service.submit(task);
        keyVariablesTaskMap.put(dataSourceName, future);
    }

    private static String genDataSourceKey(String hostName, String dsName) {
        return hostName + ":" + dsName;
    }
}
