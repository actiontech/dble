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
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
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

    private List<ErrorInfo> errorInfos = new ArrayList<>();

    public ConfigInitializer(boolean lowerCaseNames) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("load-config-file");
        try {
            //load db.xml
            XMLDbLoader dbLoader = new XMLDbLoader(null, this);
            this.dbGroups = dbLoader.getDbGroups();

            //load sharding.xml
            XMLShardingLoader shardingLoader = new XMLShardingLoader(lowerCaseNames, this);
            this.schemas = shardingLoader.getSchemas();
            this.erRelations = shardingLoader.getErRelations();
            this.shardingNodes = initShardingNodes(shardingLoader.getShardingNode());

            //load user.xml
            XMLUserLoader userLoader = new XMLUserLoader(null, this);
            this.users = userLoader.getUsers();

            deleteRedundancyConf();
            checkWriteHost();
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

    private void checkWriteHost() {
        if (this.dbGroups.isEmpty()) {
            return;
        }
        //Mark all dbInstance whether they are fake or not
        for (PhysicalDbGroup dbGroup : this.dbGroups.values()) {
            for (PhysicalDbInstance source : dbGroup.getAllDbInstances()) {
                if (checkSourceFake(source)) {
                    source.setFakeNode(true);
                } else if (!source.isDisabled()) {
                    this.fullyConfigured = true;
                }
            }
        }
        // if there are dbGroups exists. no empty shardingNodes allowed
        for (ShardingNode shardingNode : this.shardingNodes.values()) {
            if (shardingNode.getDbGroup() == null) {
                throw new ConfigException("dbGroup not exists " + shardingNode.getDbGroupName());
            }
        }
    }

    private boolean checkSourceFake(PhysicalDbInstance source) {
        if (("localhost".equalsIgnoreCase(source.getConfig().getIp()) || "127.0.0.1".equals(source.getConfig().getIp()) ||
                "0:0:0:0:0:0:0:1".equals(source.getConfig().getIp()) || "::1".equals(source.getConfig().getIp())) &&
                (source.getConfig().getPort() == SystemConfig.getInstance().getServerPort() || source.getConfig().getPort() == SystemConfig.getInstance().getManagerPort())) {
            return true;
        }
        return false;
    }

    private void deleteRedundancyConf() {
        Set<String> allUseShardingNode = new HashSet<>();

        if (schemas.size() == 0) {
            errorInfos.add(new ErrorInfo("Xml", "WARNING", "No sharding available"));
        }

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

        Set<String> allUseHost = new HashSet<>();
        //delete redundancy shardingNode
        Iterator<Map.Entry<String, ShardingNode>> iterator = this.shardingNodes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ShardingNode> entry = iterator.next();
            String shardingNodeName = entry.getKey();
            if (allUseShardingNode.contains(shardingNodeName)) {
                if (entry.getValue().getDbGroup() != null) {
                    allUseHost.add(entry.getValue().getDbGroup().getGroupName());
                }
            } else {
                LOGGER.info("shardingNode " + shardingNodeName + " is useless,server will ignore it");
                errorInfos.add(new ErrorInfo("Xml", "WARNING", "shardingNode " + shardingNodeName + " is useless"));
                iterator.remove();
            }
        }
        allUseShardingNode.clear();
        //delete redundancy dbGroup
        if (allUseHost.size() < this.dbGroups.size()) {
            Iterator<String> dbGroup = this.dbGroups.keySet().iterator();
            while (dbGroup.hasNext()) {
                String dbGroupName = dbGroup.next();
                if (!allUseHost.contains(dbGroupName)) {
                    LOGGER.info("dbGroup " + dbGroupName + " is useless,server will ignore it");
                    errorInfos.add(new ErrorInfo("Xml", "WARNING", "dbGroup " + dbGroupName + " is useless"));
                    dbGroup.remove();
                }
            }
        }
        allUseHost.clear();
    }

    public void testConnection() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("test-connection");
        try {
            Map<String, List<Pair<String, String>>> hostSchemaMap = genHostSchemaMap();
            Set<String> errNodeKeys = new HashSet<>();
            Set<String> errSourceKeys = new HashSet<>();
            BoolPtr isConnectivity = new BoolPtr(true);
            BoolPtr isAllDbInstanceConnected = new BoolPtr(true);
            for (Map.Entry<String, List<Pair<String, String>>> entry : hostSchemaMap.entrySet()) {
                String hostName = entry.getKey();
                List<Pair<String, String>> nodeList = entry.getValue();
                PhysicalDbGroup pool = dbGroups.get(hostName);

                checkMaxCon(pool);
                for (PhysicalDbInstance ds : pool.getAllDbInstances()) {
                    if (ds.getConfig().isDisabled()) {
                        errorInfos.add(new ErrorInfo("Backend", "WARNING", "dbGroup[" + pool.getGroupName() + "," + ds.getName() + "] is disabled"));
                        LOGGER.info("dbGroup[" + ds.getDbGroupConfig().getName() + "] is disabled,just mark testing failed and skip it");
                        ds.setTestConnSuccess(false);
                        continue;
                    } else if (ds.isFakeNode()) {
                        errorInfos.add(new ErrorInfo("Backend", "WARNING", "dbGroup[" + pool.getGroupName() + "," + ds.getName() + "] is fake Node"));
                        LOGGER.info("dbGroup[" + ds.getDbGroupConfig().getName() + "] is disabled,just mark testing failed and skip it");
                        ds.setTestConnSuccess(false);
                        continue;
                    }
                    testDbInstance(errNodeKeys, errSourceKeys, isConnectivity, isAllDbInstanceConnected, nodeList, pool, ds);
                }
            }

            if (!isAllDbInstanceConnected.get()) {
                StringBuilder sb = new StringBuilder("SelfCheck### there are some dbInstance connection failed, pls check these dbInstance:");
                for (String key : errSourceKeys) {
                    sb.append("{");
                    sb.append(key);
                    sb.append("},");
                }
                throw new ConfigException(sb.toString());
            }

            if (!isConnectivity.get()) {
                StringBuilder sb = new StringBuilder("SelfCheck### there are some sharding node connection failed, pls check these dbInstance:");
                for (String key : errNodeKeys) {
                    sb.append("{");
                    sb.append(key);
                    sb.append("},");
                }
                LOGGER.warn(sb.toString());
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }


    private void checkMaxCon(PhysicalDbGroup pool) {
        int schemasCount = 0;
        for (ShardingNode dn : shardingNodes.values()) {
            if (dn.getDbGroup() == pool) {
                schemasCount++;
            }
        }
        for (PhysicalDbInstance dbInstance : pool.getAllDbInstances()) {
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

    private void testDbInstance(Set<String> errNodeKeys, Set<String> errSourceKeys, BoolPtr isConnectivity,
                                BoolPtr isAllDbInstanceConnected, List<Pair<String, String>> nodeList, PhysicalDbGroup pool, PhysicalDbInstance ds) {
        boolean isMaster = ds == pool.getWriteDbInstance();
        String dbInstanceName = "dbInstance[" + ds.getDbGroupConfig().getName() + "." + ds.getName() + "]";
        try {
            BoolPtr isDSConnectedPtr = new BoolPtr(false);
            TestTask testDsTask = new TestTask(ds, isDSConnectedPtr);
            testDsTask.start();
            testDsTask.join(3000);
            boolean isDbInstanceConnected = isDSConnectedPtr.get();
            ds.setTestConnSuccess(isDbInstanceConnected);
            if (!isDbInstanceConnected) {
                isConnectivity.set(false);
                isAllDbInstanceConnected.set(false);
                errSourceKeys.add(dbInstanceName);
                errorInfos.add(new ErrorInfo("Backend", "WARNING", "Can't connect to [" + ds.getDbGroupConfig().getName() + "," + ds.getName() + "]"));
                markDbInstanceSchemaFail(errNodeKeys, nodeList, dbInstanceName);
            } else {
                BoolPtr isSchemaConnectedPtr = new BoolPtr(true);
                TestSchemasTask testSchemaTask = new TestSchemasTask(ds, nodeList, errNodeKeys, isSchemaConnectedPtr, isMaster);
                testSchemaTask.start();
                testSchemaTask.join(3000);
                boolean isConnected = isSchemaConnectedPtr.get();
                if (!isConnected) {
                    isConnectivity.set(false);
                    for (Map.Entry<String, String> entry : testSchemaTask.getNodes().entrySet()) {
                        shardingNodes.get(entry.getValue()).setSchemaExists(false);
                    }
                }
            }
        } catch (InterruptedException e) {
            isConnectivity.set(false);
            isAllDbInstanceConnected.set(false);
            errSourceKeys.add(dbInstanceName);
            markDbInstanceSchemaFail(errNodeKeys, nodeList, dbInstanceName);
        }
    }

    private void markDbInstanceSchemaFail(Set<String> errKeys, List<Pair<String, String>> nodeList, String dbInstanceName) {
        for (Pair<String, String> node : nodeList) {
            String key = dbInstanceName + ",sharding_node[" + node.getKey() + "],sharding[" + node.getValue() + "]";
            errKeys.add(key);
            shardingNodes.get(node.getKey()).setSchemaExists(false);
            LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
        }
    }

    private Map<String, List<Pair<String, String>>> genHostSchemaMap() {
        Map<String, List<Pair<String, String>>> hostSchemaMap = new HashMap<>();
        if (this.shardingNodes != null && this.dbGroups != null) {
            for (Map.Entry<String, PhysicalDbGroup> entry : dbGroups.entrySet()) {
                String hostName = entry.getKey();
                PhysicalDbGroup pool = entry.getValue();
                for (ShardingNode shardingNode : shardingNodes.values()) {
                    if (pool.equals(shardingNode.getDbGroup())) {
                        List<Pair<String, String>> nodes = hostSchemaMap.computeIfAbsent(hostName, k -> new ArrayList<>());
                        nodes.add(new Pair<>(shardingNode.getName(), shardingNode.getDatabase()));
                    }
                }
            }
        }
        return hostSchemaMap;
    }


    public Map<UserName, UserConfig> getUsers() {
        return users;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
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
