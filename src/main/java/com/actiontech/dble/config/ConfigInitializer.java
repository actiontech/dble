/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.config.converter.DBConverter;
import com.actiontech.dble.config.converter.SequenceConverter;
import com.actiontech.dble.config.converter.ShardingConverter;
import com.actiontech.dble.config.converter.UserConverter;
import com.actiontech.dble.config.helper.TestSchemasTask;
import com.actiontech.dble.config.helper.TestTask;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private volatile boolean fullyConfigured = false;
    private volatile Map<String, Properties> blacklistConfig;
    private volatile Map<String, AbstractPartitionAlgorithm> functions = Maps.newHashMap();
    private String dbConfig;
    private String shardingConfig;
    private String userConfig;
    private String sequenceConfig;

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
                throw new ConfigException(((UnmarshalException) e).getLinkedException());
            }
            throw new ConfigException(e);
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
    public ConfigInitializer(String userConfig, String dbConfig, String shardingConfig, String sequenceConfig) {
        init(userConfig, dbConfig, shardingConfig, sequenceConfig, true);
    }

    private void init(String userJson, String dbJson, String shardingJson, String sequenceJson, boolean syncHaStatus) {
        if (StringUtil.isBlank(userJson) || StringUtil.isBlank(dbJson)) {
            throw new ConfigException("the configuration file is missing or the content is empty,pls check the file/zk/ucore configuration");
        }
        LOGGER.info("dble config is [user]:{},[db]:{},[sharding]:{}", userJson, dbJson, shardingJson);
        //user
        UserConverter userConverter = new UserConverter();
        userConverter.userJsonToMap(userJson, this);
        this.users = userConverter.getUserConfigMap();
        this.blacklistConfig = userConverter.getBlackListConfigMap();
        this.userConfig = userJson;

        //db
        DBConverter dbConverter = new DBConverter();
        dbConverter.dbJsonToMap(dbJson, this, syncHaStatus);
        this.dbGroups = dbConverter.getDbGroupMap();
        this.dbConfig = dbJson;

        //sharding
        if (userConverter.isContainsShardingUser()) {
            ShardingConverter shardingConverter = new ShardingConverter();
            shardingConverter.shardingJsonToMap(shardingJson, dbConverter.getDbGroupMap(), sequenceJson, this);
            this.schemas = shardingConverter.getSchemaConfigMap();
            this.erRelations = shardingConverter.getErRelations();
            this.shardingNodes = shardingConverter.getShardingNodeMap();
            this.functions = shardingConverter.getFunctionMap();
        }
        this.shardingConfig = shardingJson;

        this.sequenceConfig = sequenceJson;
        checkRwSplitDbGroup();
        checkWriteDbInstance();
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
        if (this.dbGroups.isEmpty()) {
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
                } else {
                    group.setRwSplitUseless(false);
                }
            }
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

    private void checkMaxCon(PhysicalDbGroup pool, int schemasCount) {
        for (PhysicalDbInstance dbInstance : pool.getDbInstances(true)) {
            if (dbInstance.getConfig().getMaxCon() < Math.max(schemasCount + 1, dbInstance.getConfig().getMinCon())) {
                errorInfos.add(new ErrorInfo("Xml", "NOTICE", "dbGroup[" + pool.getGroupName() + "." + dbInstance.getConfig().getInstanceName() + "] maxCon too little,would be change to " +
                        Math.max(schemasCount + 1, dbInstance.getConfig().getMinCon())));
            }

            if (Math.max(schemasCount + 1, dbInstance.getConfig().getMinCon()) != dbInstance.getConfig().getMinCon()) {
                errorInfos.add(new ErrorInfo("Xml", "NOTICE", "dbGroup[" + pool.getGroupName() + "] minCon too little, Dble would init dbGroup" +
                        " with " + (schemasCount + 1) + " connections"));
            }
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

    public boolean isFullyConfigured() {
        return fullyConfigured;
    }

    public List<ErrorInfo> getErrorInfos() {
        return errorInfos;
    }

    public String getDbConfig() {
        return dbConfig;
    }

    public String getShardingConfig() {
        return shardingConfig;
    }

    public String getUserConfig() {
        return userConfig;
    }

    public String getSequenceConfig() {
        return sequenceConfig;
    }
}
