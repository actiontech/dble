/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.Shardings;
import com.oceanbase.obsharding_d.config.converter.DBConverter;
import com.oceanbase.obsharding_d.config.converter.SequenceConverter;
import com.oceanbase.obsharding_d.config.converter.ShardingConverter;
import com.oceanbase.obsharding_d.config.converter.UserConverter;
import com.oceanbase.obsharding_d.config.helper.TestSchemasTask;
import com.oceanbase.obsharding_d.config.helper.TestTask;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.db.type.DataBaseType;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ERTable;
import com.oceanbase.obsharding_d.config.model.user.AnalysisUserConfig;
import com.oceanbase.obsharding_d.config.model.user.RwSplitUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.meta.ReloadLogHelper;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;
import com.oceanbase.obsharding_d.route.function.AbstractPartitionAlgorithm;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.services.manager.response.ChangeItem;
import com.oceanbase.obsharding_d.services.manager.response.ChangeItemType;
import com.oceanbase.obsharding_d.services.manager.response.ChangeType;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.xml.bind.UnmarshalException;
import java.util.*;

/**
 * @author mycat
 */
public class ConfigInitializer implements ProblemReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);

    private volatile Map<UserName, UserConfig> users;
    private volatile Map<String, SchemaConfig> schemas = Maps.newHashMap();
    private volatile Map<String, ShardingNode> shardingNodes = Maps.newHashMap();
    private volatile Map<String, PhysicalDbGroup> dbGroups;
    private volatile Map<ERTable, Set<ERTable>> erRelations = Maps.newHashMap();
    private volatile Map<String, Set<ERTable>> funcNodeERMap = Maps.newHashMap();
    private volatile boolean fullyConfigured = false;
    private volatile Map<String, Properties> blacklistConfig;
    private volatile Map<String, AbstractPartitionAlgorithm> functions = Maps.newHashMap();
    private RawJson dbConfig;
    private RawJson shardingConfig;
    private RawJson userConfig;
    private RawJson sequenceConfig;

    private final List<ErrorInfo> errorInfos = new ArrayList<>();

    /**
     * load by xml-config
     */
    public ConfigInitializer() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("load-config-file");
        try {
            //sync json
            UserConverter userConverter = new UserConverter();
            this.userConfig = userConverter.userXmlToJson();
            this.dbConfig = DBConverter.dbXmlToJson();
            if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT) {
                this.sequenceConfig = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_FILE_NAME);
            } else if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL) {
                this.sequenceConfig = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_DB_FILE_NAME);
            }
            if (userConverter.isContainsShardingUser()) {
                this.shardingConfig = new ShardingConverter().shardingXmlToJson();
            } else {
                this.shardingConfig = new ShardingConverter().shardingBeanToJson(new Shardings());
            }
            init(this.userConfig, this.dbConfig, this.shardingConfig, this.sequenceConfig, false);
        } catch (Exception e) {
            if (e instanceof UnmarshalException) {
                throw new ConfigException(e.getMessage() == null ? e.toString() : e.getMessage(), ((UnmarshalException) e).getLinkedException());
            }
            throw new ConfigException(e.getMessage() == null ? e.toString() : e.getMessage(), e);
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    /**
     * load by json-config
     *
     * @param userConfig
     * @param dbConfig
     * @param shardingConfig
     * @param sequenceConfig
     */
    public ConfigInitializer(RawJson userConfig, RawJson dbConfig, RawJson shardingConfig, RawJson sequenceConfig) {
        init(userConfig, dbConfig, shardingConfig, sequenceConfig, true);
    }

    private void init(RawJson userJson, RawJson dbJson, @Nullable RawJson shardingJson, @Nullable RawJson sequenceJson, boolean syncHaStatus) {
        if (userJson == null) {
            throw new IllegalArgumentException("Config for init not ready yet. user config is null");
        }
        if (shardingJson == null) {
            LOGGER.info("sharding config is null");
        }
        if (dbJson == null) {
            LOGGER.warn("Config for init not ready yet. db config is null");
        }
        LOGGER.info("OBsharding-D config is [user]:{},[db]:{},[sharding]:{}", userJson, dbJson, shardingJson);
        //user
        UserConverter userConverter = new UserConverter();
        userConverter.userJsonToMap(userJson, this);
        this.users = userConverter.getUserConfigMap();
        this.blacklistConfig = userConverter.getBlackListConfigMap();
        this.userConfig = userJson;

        //db
        DBConverter dbConverter = new DBConverter();
        if (dbJson != null) {
            dbConverter.dbJsonToMap(dbJson, this, syncHaStatus);
            this.dbGroups = dbConverter.getDbGroupMap();
            this.dbConfig = dbJson;
        } else {
            this.dbGroups = Maps.newLinkedHashMap();
            this.dbConfig = null;
        }

        //sharding
        if (userConverter.isContainsShardingUser()) {
            ShardingConverter shardingConverter = new ShardingConverter();
            shardingConverter.shardingJsonToMap(shardingJson, dbConverter.getDbGroupMap(), sequenceJson, this);
            this.schemas = shardingConverter.getSchemaConfigMap();
            this.erRelations = shardingConverter.getErRelations();
            this.funcNodeERMap = shardingConverter.getFuncNodeERMap();
            this.shardingNodes = shardingConverter.getShardingNodeMap();
            this.functions = shardingConverter.getFunctionMap();
        }
        this.shardingConfig = shardingJson;

        this.sequenceConfig = sequenceJson;
        checkRwSplitDbGroup();
        checkAnalysisDbGroup();
        checkWriteDbInstance();
    }

    private void checkAnalysisDbGroup() {
        // include Analysis dbGroup
        AnalysisUserConfig analysisUserConfig;
        PhysicalDbGroup group;
        for (UserConfig config : this.users.values()) {
            if (config instanceof AnalysisUserConfig) {
                analysisUserConfig = (AnalysisUserConfig) config;
                group = this.dbGroups.get(analysisUserConfig.getDbGroup());
                if (group == null) {
                    throw new ConfigException("The user's group[" + analysisUserConfig.getName() + "." + analysisUserConfig.getDbGroup() + "] for analysisUser isn't configured in db.xml.");
                } else if (group.getDbGroupConfig().instanceDatabaseType() != DataBaseType.CLICKHOUSE) {
                    throw new ConfigException("The group[" + analysisUserConfig.getName() + "." + analysisUserConfig.getDbGroup() + "] all dbInstance database type must be " + DataBaseType.CLICKHOUSE);
                } else {
                    group.setAnalysisUseless(false);
                }
            }
        }
    }


    @Override
    public void warn(String problem) {
        this.errorInfos.add(new ErrorInfo("Xml", "WARNING", problem));
        LOGGER.warn(problem);
    }

    @Override
    public void error(String problem) {
        throw new ConfigException(problem);
    }

    @Override
    public void notice(String problem) {
        this.errorInfos.add(new ErrorInfo("Xml", "NOTICE", problem));
        LOGGER.info(problem);
    }

    private void checkWriteDbInstance() {
        if (this.dbGroups == null || this.dbGroups.isEmpty()) {
            return;
        }
        //Mark all dbInstance whether they are fake or not
        for (PhysicalDbGroup dbGroup : this.dbGroups.values()) {
            if (dbGroup.isUseless()) {
                LOGGER.info("dbGroup " + dbGroup.getGroupName() + " is useless,server will create heartbeat,not create pool");
            }

            for (PhysicalDbInstance dbInstance : dbGroup.getDbInstances(true)) {
                if (checkDbInstanceFake(dbInstance)) {
                    dbInstance.setFakeNode(true);
                } else if (!dbInstance.isDisabled()) {
                    this.fullyConfigured = true;
                }
            }
        }
    }

    private boolean checkDbInstanceFake(PhysicalDbInstance source) {
        return ("localhost".equalsIgnoreCase(source.getConfig().getIp()) || "127.0.0.1".equals(source.getConfig().getIp()) ||
                "0:0:0:0:0:0:0:1".equals(source.getConfig().getIp()) || "::1".equals(source.getConfig().getIp())) &&
                (source.getConfig().getPort() == SystemConfig.getInstance().getServerPort() || source.getConfig().getPort() == SystemConfig.getInstance().getManagerPort());
    }

    private void checkRwSplitDbGroup() {
        // include rwSplit dbGroup
        RwSplitUserConfig rwSplitUserConfig;
        PhysicalDbGroup group;
        for (UserConfig config : this.users.values()) {
            if (config instanceof RwSplitUserConfig) {
                rwSplitUserConfig = (RwSplitUserConfig) config;
                group = this.dbGroups.get(rwSplitUserConfig.getDbGroup());
                if (group == null) {
                    throw new ConfigException("The user's group[" + rwSplitUserConfig.getName() + "." + rwSplitUserConfig.getDbGroup() + "] for rwSplit isn't configured in db.xml.");
                } else if (!group.isShardingUseless()) {
                    throw new ConfigException("The group[" + rwSplitUserConfig.getName() + "." + rwSplitUserConfig.getDbGroup() + "] has been used by sharding node, can't be used by rwSplit.");
                } else if (group.getDbGroupConfig().instanceDatabaseType() != DataBaseType.MYSQL) {
                    throw new ConfigException("The group[" + rwSplitUserConfig.getName() + "." + rwSplitUserConfig.getDbGroup() + "] all dbInstance database type must be " + DataBaseType.MYSQL);
                } else {
                    group.setRwSplitUseless(false);
                }
            }
        }
    }

    public void testConnection(List<ChangeItem> changeItemList) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("test-connection");
        try {
            Map<String, List<Pair<String, String>>> hostSchemaMap = genDbInstanceSchemaMap();
            Set<String> errDbInstanceNames = new HashSet<>();
            boolean isAllDbInstanceConnected = true;
            // check whether dbInstance is connected
            String dbGroupName;
            PhysicalDbGroup dbGroup;
            Set<String> dbGroupTested = Sets.newHashSet();

            for (ChangeItem changeItem : changeItemList) {
                ChangeType type = changeItem.getType();
                Object item = changeItem.getItem();
                ChangeItemType itemType = changeItem.getItemType();
                switch (type) {
                    case ADD:
                        if (itemType == ChangeItemType.PHYSICAL_DB_GROUP) {
                            //test dbGroup
                            dbGroup = (PhysicalDbGroup) item;

                            boolean isDbInstanceConnected = testDbGroup(dbGroup, hostSchemaMap, dbGroupTested, errDbInstanceNames);
                            if (!isDbInstanceConnected) {
                                isAllDbInstanceConnected = false;
                            }
                        } else if (itemType == ChangeItemType.PHYSICAL_DB_INSTANCE) {
                            PhysicalDbInstance ds = (PhysicalDbInstance) item;
                            dbGroupName = ds.getDbGroupConfig().getName();
                            // sharding group
                            List<Pair<String, String>> schemaList = checkDbInstanceMaxConn(hostSchemaMap, ds);
                            //test dbInstance
                            boolean testResult = checkAndTestDbInstance(ds, dbGroupName, schemaList);
                            if (!testResult) {
                                isAllDbInstanceConnected = false;
                                errDbInstanceNames.add("dbInstance[" + dbGroupName + "." + ds.getName() + "]");
                            }
                        } else if (itemType == ChangeItemType.SHARDING_NODE) {
                            ShardingNode shardingNode = (ShardingNode) item;
                            dbGroup = shardingNode.getDbGroup();

                            boolean isDbInstanceConnected = testDbGroup(dbGroup, hostSchemaMap, dbGroupTested, errDbInstanceNames);
                            if (!isDbInstanceConnected) {
                                isAllDbInstanceConnected = false;
                            }
                        }
                        break;
                    case UPDATE:
                        if (itemType == ChangeItemType.PHYSICAL_DB_INSTANCE && changeItem.isAffectTestConn()) {
                            PhysicalDbInstance ds = (PhysicalDbInstance) item;
                            dbGroupName = ds.getDbGroupConfig().getName();
                            // sharding group
                            List<Pair<String, String>> schemaList = checkDbInstanceMaxConn(hostSchemaMap, ds);
                            //test dbInstance
                            //test dbInstance
                            boolean testResult = checkAndTestDbInstance(ds, dbGroupName, schemaList);
                            if (!testResult) {
                                isAllDbInstanceConnected = false;
                                errDbInstanceNames.add("dbInstance[" + dbGroupName + "." + ds.getName() + "]");
                            }
                        } else if (itemType == ChangeItemType.SHARDING_NODE) {
                            ShardingNode shardingNode = (ShardingNode) item;
                            dbGroup = shardingNode.getDbGroup();

                            boolean isDbInstanceConnected = testDbGroup(dbGroup, hostSchemaMap, dbGroupTested, errDbInstanceNames);
                            if (!isDbInstanceConnected) {
                                isAllDbInstanceConnected = false;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            if (!isAllDbInstanceConnected) {
                StringBuilder sb = new StringBuilder("SelfCheck### there are some dbInstance connection failed, pls check these dbInstance:");
                for (String key : errDbInstanceNames) {
                    sb.append("{");
                    sb.append(key);
                    sb.append("},");
                }
                throw new ConfigException(sb.toString());
            }

        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }


    public void testConnection() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("test-connection");
        try {
            Map<String, List<Pair<String, String>>> hostSchemaMap = genDbInstanceSchemaMap();
            Set<String> errDbInstanceNames = new HashSet<>();
            boolean isAllDbInstanceConnected = true;
            // check whether dbInstance is connected
            String dbGroupName;
            PhysicalDbGroup dbGroup;
            for (Map.Entry<String, PhysicalDbGroup> entry : this.dbGroups.entrySet()) {
                dbGroup = entry.getValue();
                dbGroupName = entry.getKey();

                // sharding group
                List<Pair<String, String>> schemaList = null;
                if (hostSchemaMap.containsKey(dbGroupName)) {
                    schemaList = hostSchemaMap.get(entry.getKey());
                    checkMaxCon(dbGroup, schemaList.size());
                }

                for (PhysicalDbInstance ds : dbGroup.getDbInstances(true)) {
                    if (ds.getConfig().isDisabled()) {
                        errorInfos.add(new ErrorInfo("Backend", "WARNING", "dbGroup[" + dbGroupName + "," + ds.getName() + "] is disabled"));
                        LOGGER.info("dbGroup[" + ds.getDbGroupConfig().getName() + "] is disabled,just mark testing failed and skip it");
                        ds.setTestConnSuccess(false);
                        continue;
                    } else if (ds.isFakeNode()) {
                        errorInfos.add(new ErrorInfo("Backend", "WARNING", "dbGroup[" + dbGroupName + "," + ds.getName() + "] is fake Node"));
                        LOGGER.info("dbGroup[" + ds.getDbGroupConfig().getName() + "] is disabled,just mark testing failed and skip it");
                        ds.setTestConnSuccess(false);
                        continue;
                    }
                    if (!testDbInstance(dbGroupName, ds, schemaList)) {
                        isAllDbInstanceConnected = false;
                        errDbInstanceNames.add("dbInstance[" + dbGroupName + "." + ds.getName() + "]");
                    }
                }
            }

            if (!isAllDbInstanceConnected) {
                StringBuilder sb = new StringBuilder("SelfCheck### there are some dbInstance connection failed, pls check these dbInstance:");
                for (String key : errDbInstanceNames) {
                    sb.append("{");
                    sb.append(key);
                    sb.append("},");
                }
                throw new ConfigException(sb.toString());
            }

        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private boolean testDbGroup(PhysicalDbGroup dbGroup, Map<String, List<Pair<String, String>>> hostSchemaMap, Set<String> dbGroupTested, Set<String> errDbInstanceNames) {
        String dbGroupName = dbGroup.getGroupName();
        boolean isAllDbInstanceConnected = true;
        if (dbGroupTested.add(dbGroupName)) {
            // sharding group
            List<Pair<String, String>> schemaList = checkDbGroupMaxConn(hostSchemaMap, dbGroup);

            for (PhysicalDbInstance ds : dbGroup.getDbInstances(true)) {
                //test dbInstance
                boolean testResult = checkAndTestDbInstance(ds, dbGroupName, schemaList);
                if (!testResult) {
                    isAllDbInstanceConnected = false;
                    errDbInstanceNames.add("dbInstance[" + dbGroupName + "." + ds.getName() + "]");
                }
            }
        }
        return isAllDbInstanceConnected;
    }

    private List<Pair<String, String>> checkDbInstanceMaxConn(Map<String, List<Pair<String, String>>> hostSchemaMap, PhysicalDbInstance ds) {
        List<Pair<String, String>> schemaList = null;
        if (hostSchemaMap.containsKey(ds.getDbGroupConfig().getName())) {
            schemaList = hostSchemaMap.get(ds.getDbGroupConfig().getName());
            checkMaxCon(ds, schemaList.size());
        }
        return schemaList;
    }

    private List<Pair<String, String>> checkDbGroupMaxConn(Map<String, List<Pair<String, String>>> hostSchemaMap, PhysicalDbGroup dbGroup) {
        List<Pair<String, String>> schemaList = null;
        if (hostSchemaMap.containsKey(dbGroup.getGroupName())) {
            schemaList = hostSchemaMap.get(dbGroup.getGroupName());
            checkMaxCon(dbGroup, schemaList.size());
        }
        return schemaList;
    }

    private boolean checkAndTestDbInstance(PhysicalDbInstance ds, String dbGroupName, List<Pair<String, String>> schemaList) {
        if (ds.getConfig().isDisabled()) {
            errorInfos.add(new ErrorInfo("Backend", "WARNING", "dbGroup[" + dbGroupName + "," + ds.getName() + "] is disabled"));
            LOGGER.info("dbGroup[" + ds.getDbGroupConfig().getName() + "] is disabled,just mark testing failed and skip it");
            ds.setTestConnSuccess(false);
            return true;
        } else if (ds.isFakeNode()) {
            errorInfos.add(new ErrorInfo("Backend", "WARNING", "dbGroup[" + dbGroupName + "," + ds.getName() + "] is fake Node"));
            LOGGER.info("dbGroup[" + ds.getDbGroupConfig().getName() + "] is disabled,just mark testing failed and skip it");
            ds.setTestConnSuccess(false);
            return true;
        }
        return testDbInstance(dbGroupName, ds, schemaList);
    }


    private void checkMaxCon(PhysicalDbGroup pool, int schemasCount) {
        for (PhysicalDbInstance dbInstance : pool.getDbInstances(true)) {
            checkMaxCon(dbInstance, schemasCount);
        }
    }

    private void checkMaxCon(PhysicalDbInstance dbInstance, int schemasCount) {
        if (dbInstance.getConfig().getMaxCon() < Math.max(schemasCount + 1, dbInstance.getConfig().getMinCon())) {
            errorInfos.add(new ErrorInfo("Xml", "NOTICE", "dbGroup[" + dbInstance.getDbGroupConfig().getName() + "." + dbInstance.getConfig().getInstanceName() + "] maxCon too little,would be change to " +
                    Math.max(schemasCount + 1, dbInstance.getConfig().getMinCon())));
        }

        if (Math.max(schemasCount + 1, dbInstance.getConfig().getMinCon()) != dbInstance.getConfig().getMinCon()) {
            errorInfos.add(new ErrorInfo("Xml", "NOTICE", "dbGroup[" + dbInstance.getDbGroupConfig().getName() + "] minCon too little, OBsharding-D would init dbGroup" +
                    " with " + (schemasCount + 1) + " connections"));
        }
    }

    private boolean testDbInstance(String dbGroupName, PhysicalDbInstance ds, List<Pair<String, String>> schemaList) {
        boolean isConnectivity = true;
        String dbInstanceKey = "dbInstance[" + dbGroupName + "." + ds.getName() + "]";
        try {
            BoolPtr isDSConnectedPtr = new BoolPtr(false);
            TestTask testDsTask = new TestTask(ds, isDSConnectedPtr);
            testDsTask.start();
            testDsTask.join(3000);
            boolean isDbInstanceConnected = isDSConnectedPtr.get();
            ds.setTestConnSuccess(isDbInstanceConnected);
            if (!isDbInstanceConnected) {
                errorInfos.add(new ErrorInfo("Backend", "WARNING", "Can't connect to [" + dbInstanceKey + "]"));
                LOGGER.warn("SelfCheck### can't connect to [" + dbInstanceKey + "]");
                isConnectivity = false;
            } else if (schemaList != null) {
                TestSchemasTask testSchemaTask = new TestSchemasTask(shardingNodes, ds, schemaList, !ds.isReadInstance());
                testSchemaTask.start();
                testSchemaTask.join(3000);
            } else {
                LOGGER.info("SelfCheck### connect to [" + dbInstanceKey + "] successfully.");
            }
        } catch (InterruptedException e) {
            errorInfos.add(new ErrorInfo("Backend", "WARNING", "Can't connect to [" + dbInstanceKey + "]"));
            LOGGER.warn("SelfCheck### can't connect to [" + dbInstanceKey + "]");
            isConnectivity = false;
        } finally {
            ReloadLogHelper.debug("test connection dbInstance:{},is connect:{},schemaList:{}", ds, isConnectivity, schemaList);
        }
        return isConnectivity;
    }

    private Map<String, List<Pair<String, String>>> genDbInstanceSchemaMap() {
        Map<String, List<Pair<String, String>>> dbInstanceSchemaMap = new HashMap<>(16);
        if (shardingNodes != null) {
            for (ShardingNode shardingNode : shardingNodes.values()) {
                List<Pair<String, String>> nodes = dbInstanceSchemaMap.computeIfAbsent(shardingNode.getDbGroupName(), k -> new ArrayList<>(8));
                nodes.add(new Pair<>(shardingNode.getName(), shardingNode.getDatabase()));
            }
        }
        return dbInstanceSchemaMap;
    }


    public Map<UserName, UserConfig> getUsers() {
        return users;
    }

    public Map<String, Properties> getBlacklistConfig() {
        return blacklistConfig;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, AbstractPartitionAlgorithm> getFunctions() {
        return functions;
    }

    public Map<String, ShardingNode> getShardingNodes() {
        return shardingNodes;
    }

    public Map<String, PhysicalDbGroup> getDbGroups() {
        return this.dbGroups;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    public Map<String, Set<ERTable>> getFuncNodeERMap() {
        return funcNodeERMap;
    }

    public boolean isFullyConfigured() {
        return fullyConfigured;
    }

    public List<ErrorInfo> getErrorInfos() {
        return errorInfos;
    }

    public RawJson getDbConfig() {
        return dbConfig;
    }

    public RawJson getShardingConfig() {
        return shardingConfig;
    }

    public RawJson getUserConfig() {
        return userConfig;
    }

    public RawJson getSequenceConfig() {
        return sequenceConfig;
    }
}
