/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.helper.TestSchemasTask;
import com.actiontech.dble.config.helper.TestTask;
import com.actiontech.dble.config.loader.xml.XMLDbLoader;
import com.actiontech.dble.config.loader.xml.XMLShardingLoader;
import com.actiontech.dble.config.loader.xml.XMLUserLoader;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.ShardingNodeConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.sequence.handler.IncrSequenceMySQLHandler;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author mycat
 */
public class ConfigInitializer implements ProblemReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);

    private volatile Map<UserName, UserConfig> users;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, ShardingNode> shardingNodes;
    private volatile Map<String, PhysicalDbGroup> dbGroups;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile boolean fullyConfigured = false;
    private volatile Map<String, Properties> blacklistConfig;
    private volatile Map<String, AbstractPartitionAlgorithm> functions;

    private List<ErrorInfo> errorInfos = new ArrayList<>();

    public ConfigInitializer(boolean lowerCaseNames) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("load-config-file");
        try {
            //load user.xml
            XMLUserLoader userLoader = new XMLUserLoader(null, this);
            this.users = userLoader.getUsers();
            this.blacklistConfig = userLoader.getBlacklistConfig();

            //load db.xml
            XMLDbLoader dbLoader = new XMLDbLoader(null, this);
            this.dbGroups = dbLoader.getDbGroups();

            if (userLoader.isContainsShardingUser()) {
                //load sharding.xml
                XMLShardingLoader shardingLoader = new XMLShardingLoader(lowerCaseNames, this);
                this.schemas = shardingLoader.getSchemas();
                this.erRelations = shardingLoader.getErRelations();
                this.shardingNodes = initShardingNodes(shardingLoader.getShardingNode());
                this.functions = shardingLoader.getFunctions();

                deleteUselessShardingNode();
            } else {
                this.schemas = Collections.EMPTY_MAP;
                this.erRelations = Collections.EMPTY_MAP;
                this.shardingNodes = Collections.EMPTY_MAP;
                this.functions = Collections.EMPTY_MAP;
            }

            checkRwSplitDbGroup();
            checkWriteDbInstance();
        } finally {
            TraceManager.finishSpan(traceObject);
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
        if (("localhost".equalsIgnoreCase(source.getConfig().getIp()) || "127.0.0.1".equals(source.getConfig().getIp()) ||
                "0:0:0:0:0:0:0:1".equals(source.getConfig().getIp()) || "::1".equals(source.getConfig().getIp())) &&
                (source.getConfig().getPort() == SystemConfig.getInstance().getServerPort() || source.getConfig().getPort() == SystemConfig.getInstance().getManagerPort())) {
            return true;
        }
        return false;
    }

    private void deleteUselessShardingNode() {
        if (schemas.size() == 0) {
            errorInfos.add(new ErrorInfo("Xml", "WARNING", "No sharding available"));
        }
        Set<String> allUseShardingNode = new HashSet<>();
        for (SchemaConfig sc : schemas.values()) {
            // check shardingNode / dbGroup
            Set<String> shardingNodeNames = sc.getAllShardingNodes();
            allUseShardingNode.addAll(shardingNodeNames);
        }

        // add global sequence node when it is some dedicated servers */
        if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL) {
            IncrSequenceMySQLHandler redundancy = new IncrSequenceMySQLHandler();
            redundancy.load(false);
            allUseShardingNode.addAll(redundancy.getShardingNodes());
        }

        //delete redundancy shardingNode
        Iterator<Map.Entry<String, ShardingNode>> iterator = this.shardingNodes.entrySet().iterator();
        PhysicalDbGroup shardingNodeGroup;
        while (iterator.hasNext()) {
            Map.Entry<String, ShardingNode> entry = iterator.next();
            String shardingNodeName = entry.getKey();
            if (allUseShardingNode.contains(shardingNodeName)) {
                shardingNodeGroup = entry.getValue().getDbGroup();
                if (shardingNodeGroup != null) {
                    shardingNodeGroup.setUseless(false);
                } else {
                    throw new ConfigException("dbGroup not exists " + entry.getValue().getDbGroupName());
                }
            } else {
                LOGGER.info("shardingNode " + shardingNodeName + " is useless,server will ignore it");
                errorInfos.add(new ErrorInfo("Xml", "WARNING", "shardingNode " + shardingNodeName + " is useless"));
                iterator.remove();
            }
        }
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
                } else if (!group.isUseless()) {
                    throw new ConfigException("The group[" + rwSplitUserConfig.getName() + "." + rwSplitUserConfig.getDbGroup() + "] has been used by sharding node, can't be used by rwSplit.");
                } else {
                    group.setUseless(false);
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

    private Map<String, ShardingNode> initShardingNodes(Map<String, ShardingNodeConfig> nodeConf) {
        Map<String, ShardingNode> nodes = new HashMap<>(nodeConf.size());
        for (ShardingNodeConfig conf : nodeConf.values()) {
            PhysicalDbGroup pool = this.dbGroups.get(conf.getDbGroupName());
            ShardingNode shardingNode = new ShardingNode(conf.getDbGroupName(), conf.getName(), conf.getDatabase(), pool);
            nodes.put(shardingNode.getName(), shardingNode);
        }
        return nodes;
    }

    public boolean isFullyConfigured() {
        return fullyConfigured;
    }

    public List<ErrorInfo> getErrorInfos() {
        return errorInfos;
    }

}
