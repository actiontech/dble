/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.config.helper.TestSchemasTask;
import com.actiontech.dble.config.helper.TestTask;
import com.actiontech.dble.config.loader.SchemaLoader;
import com.actiontech.dble.config.loader.xml.XMLSchemaLoader;
import com.actiontech.dble.config.loader.xml.XMLServerLoader;
import com.actiontech.dble.config.model.*;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.sequence.handler.IncrSequenceMySQLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author mycat
 */
public class ConfigInitializer implements ProblemReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);

    private volatile SystemConfig system;
    private volatile FirewallConfig firewall;
    private volatile Map<String, UserConfig> users;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, PhysicalDataNode> dataNodes;
    private volatile Map<String, PhysicalDataHost> dataHosts;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile boolean dataHostWithoutWH = true;

    private List<ErrorInfo> errorInfos = new ArrayList<>();

    public ConfigInitializer(boolean lowerCaseNames) {
        //load server.xml
        XMLServerLoader serverLoader = new XMLServerLoader(this);

        //load rule.xml and schema.xml
        SchemaLoader schemaLoader = new XMLSchemaLoader(lowerCaseNames, this);
        this.schemas = schemaLoader.getSchemas();
        this.system = serverLoader.getSystem();
        this.users = serverLoader.getUsers();
        this.erRelations = schemaLoader.getErRelations();
        this.dataHosts = initDataHosts(schemaLoader);
        this.dataNodes = initDataNodes(schemaLoader);
        this.firewall = serverLoader.getFirewall();

        deleteRedundancyConf();
        checkWriteHost();
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
        for (Map.Entry<String, PhysicalDataHost> pool : this.dataHosts.entrySet()) {
            PhysicalDataSource writeSource = pool.getValue().getWriteSource();
            if (writeSource != null) {
                if (writeSource.getConfig().isDisabled()) {
                    boolean hasEnableNode = false;
                    for (PhysicalDataSource readSource : pool.getValue().getReadSources()) {
                        if (!readSource.isDisabled()) {
                            hasEnableNode = true;
                            break;
                        }
                        if (hasEnableNode) {
                            break;
                        }
                    }
                    if (!hasEnableNode) {
                        continue;
                    }
                }
                this.dataHostWithoutWH = false;
                break;
            }
        }
    }

    private void deleteRedundancyConf() {
        Set<String> allUseDataNode = new HashSet<>();

        if (schemas.size() == 0) {
            errorInfos.add(new ErrorInfo("Xml", "WARNING", "No schema available"));
        }

        for (SchemaConfig sc : schemas.values()) {
            // check dataNode / dataHost
            Set<String> dataNodeNames = sc.getAllDataNodes();
            allUseDataNode.addAll(dataNodeNames);
        }

        // add global sequence node when it is some dedicated servers */
        if (system.getSequnceHandlerType() == SystemConfig.SEQUENCE_HANDLER_MYSQL) {
            IncrSequenceMySQLHandler redundancy = new IncrSequenceMySQLHandler();
            redundancy.load(false);
            allUseDataNode.addAll(redundancy.getDataNodes());
        }

        Set<String> allUseHost = new HashSet<>();
        //delete redundancy dataNode
        Iterator<Map.Entry<String, PhysicalDataNode>> iterator = this.dataNodes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PhysicalDataNode> entry = iterator.next();
            String dataNodeName = entry.getKey();
            if (allUseDataNode.contains(dataNodeName)) {
                allUseHost.add(entry.getValue().getDataHost().getHostName());
            } else {
                LOGGER.info("dataNode " + dataNodeName + " is useless,server will ignore it");
                errorInfos.add(new ErrorInfo("Xml", "WARNING", "dataNode " + dataNodeName + " is useless"));
                iterator.remove();
            }
        }
        allUseDataNode.clear();
        //delete redundancy dataHost
        if (allUseHost.size() < this.dataHosts.size()) {
            Iterator<String> dataHost = this.dataHosts.keySet().iterator();
            while (dataHost.hasNext()) {
                String dataHostName = dataHost.next();
                if (!allUseHost.contains(dataHostName)) {
                    LOGGER.info("dataHost " + dataHostName + " is useless,server will ignore it");
                    errorInfos.add(new ErrorInfo("Xml", "WARNING", "dataHost " + dataHostName + " is useless"));
                    dataHost.remove();
                }
            }
        }
        allUseHost.clear();
    }

    public void testConnection(boolean isStart) {
        Map<String, List<Pair<String, String>>> hostSchemaMap = genHostSchemaMap();
        Set<String> errNodeKeys = new HashSet<>();
        Set<String> errSourceKeys = new HashSet<>();
        BoolPtr isConnectivity = new BoolPtr(true);
        BoolPtr isAllDataSourceConnected = new BoolPtr(true);
        for (Map.Entry<String, List<Pair<String, String>>> entry : hostSchemaMap.entrySet()) {
            String hostName = entry.getKey();
            List<Pair<String, String>> nodeList = entry.getValue();
            PhysicalDataHost pool = dataHosts.get(hostName);

            checkMaxCon(pool);

            if (isStart) {
                // start for first time, 1.you can set write host as empty
                if (pool.getWriteSource() == null) {
                    continue;
                }
                DataSourceConfig wHost = pool.getWriteSource().getConfig();
                // start for first time, 2.you can set write host as yourself
                if (("localhost".equalsIgnoreCase(wHost.getIp()) || "127.0.0.1".equalsIgnoreCase(wHost.getIp())) &&
                        wHost.getPort() == this.system.getServerPort()) {
                    continue;
                }
            }
            for (PhysicalDataSource ds : pool.getAllDataSources()) {
                if (ds.getConfig().isDisabled()) {
                    errorInfos.add(new ErrorInfo("Backend", "WARNING", "DataHost[" + pool.getHostName() + "," + ds.getName() + "] is disabled"));
                    LOGGER.info("DataHost[" + ds.getHostConfig().getName() + "] is disabled,just mark testing failed and skip it");
                    ds.setTestConnSuccess(false);
                    continue;
                }
                testDataSource(errNodeKeys, errSourceKeys, isConnectivity, isAllDataSourceConnected, nodeList, pool, ds);
            }
        }

        if (!isAllDataSourceConnected.get()) {
            StringBuilder sb = new StringBuilder("SelfCheck### there are some datasource connection failed, pls check these datasource:");
            for (String key : errSourceKeys) {
                sb.append("{");
                sb.append(key);
                sb.append("},");
            }
            throw new ConfigException(sb.toString());
        }

        if (!isConnectivity.get()) {
            StringBuilder sb = new StringBuilder("SelfCheck### there are some data node connection failed, pls check these datasource:");
            for (String key : errNodeKeys) {
                sb.append("{");
                sb.append(key);
                sb.append("},");
            }
            LOGGER.warn(sb.toString());
        }
    }


    private void checkMaxCon(PhysicalDataHost pool) {
        int schemasCount = 0;
        for (PhysicalDataNode dn : dataNodes.values()) {
            if (dn.getDataHost() == pool) {
                schemasCount++;
            }
        }
        if (pool.getDataHostConfig().getMaxCon() < Math.max(schemasCount + 1, pool.getDataHostConfig().getMinCon())) {
            errorInfos.add(new ErrorInfo("Xml", "NOTICE", "DataHost[" + pool.getHostName() + "] maxCon too little,would be change to " +
                    Math.max(schemasCount + 1, pool.getDataHostConfig().getMinCon())));
        }

        if (Math.max(schemasCount + 1, pool.getDataHostConfig().getMinCon()) != pool.getDataHostConfig().getMinCon()) {
            errorInfos.add(new ErrorInfo("Xml", "NOTICE", "DataHost[" + pool.getHostName() + "] minCon too little,Dble would init dataHost" +
                    " with " + (schemasCount + 1) + " connections"));
        }
    }

    private void testDataSource(Set<String> errNodeKeys, Set<String> errSourceKeys, BoolPtr isConnectivity,
                                BoolPtr isAllDataSourceConnected, List<Pair<String, String>> nodeList, PhysicalDataHost pool, PhysicalDataSource ds) {
        boolean isMaster = ds == pool.getWriteSource();
        String dataSourceName = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getName() + "]";
        try {
            BoolPtr isDSConnectedPtr = new BoolPtr(false);
            TestTask testDsTask = new TestTask(ds, isDSConnectedPtr);
            testDsTask.start();
            testDsTask.join(3000);
            boolean isDataSourceConnected = isDSConnectedPtr.get();
            ds.setTestConnSuccess(isDataSourceConnected);
            if (!isDataSourceConnected) {
                isConnectivity.set(false);
                isAllDataSourceConnected.set(false);
                errSourceKeys.add(dataSourceName);
                errorInfos.add(new ErrorInfo("Backend", "WARNING", "Can't connect to [" + ds.getHostConfig().getName() + "," + ds.getName() + "]"));
                markDataSourceSchemaFail(errNodeKeys, nodeList, dataSourceName);
            } else {
                BoolPtr isSchemaConnectedPtr = new BoolPtr(true);
                TestSchemasTask testSchemaTask = new TestSchemasTask(ds, nodeList, errNodeKeys, isSchemaConnectedPtr, isMaster);
                testSchemaTask.start();
                testSchemaTask.join(3000);
                boolean isConnected = isSchemaConnectedPtr.get();
                if (!isConnected) {
                    isConnectivity.set(false);
                    for (Map.Entry<String, String> entry : testSchemaTask.getNodes().entrySet()) {
                        dataNodes.get(entry.getValue()).setSchemaExists(false);
                    }
                }
            }
        } catch (InterruptedException e) {
            isConnectivity.set(false);
            isAllDataSourceConnected.set(false);
            errSourceKeys.add(dataSourceName);
            markDataSourceSchemaFail(errNodeKeys, nodeList, dataSourceName);
        }
    }

    private void markDataSourceSchemaFail(Set<String> errKeys, List<Pair<String, String>> nodeList, String dataSourceName) {
        for (Pair<String, String> node : nodeList) {
            String key = dataSourceName + ",data_node[" + node.getKey() + "],schema[" + node.getValue() + "]";
            errKeys.add(key);
            dataNodes.get(node.getKey()).setSchemaExists(false);
            LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
        }
    }

    private Map<String, List<Pair<String, String>>> genHostSchemaMap() {
        Map<String, List<Pair<String, String>>> hostSchemaMap = new HashMap<>();
        if (this.dataNodes != null && this.dataHosts != null) {
            for (Map.Entry<String, PhysicalDataHost> entry : dataHosts.entrySet()) {
                String hostName = entry.getKey();
                PhysicalDataHost pool = entry.getValue();
                for (PhysicalDataNode dataNode : dataNodes.values()) {
                    if (pool.equals(dataNode.getDataHost())) {
                        List<Pair<String, String>> nodes = hostSchemaMap.computeIfAbsent(hostName, k -> new ArrayList<>());
                        nodes.add(new Pair<>(dataNode.getName(), dataNode.getDatabase()));
                    }
                }
            }
        }
        return hostSchemaMap;
    }

    public SystemConfig getSystem() {
        return system;
    }


    public FirewallConfig getFirewall() {
        return firewall;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, PhysicalDataNode> getDataNodes() {
        return dataNodes;
    }

    public Map<String, PhysicalDataHost> getDataHosts() {
        return this.dataHosts;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    private Map<String, PhysicalDataHost> initDataHosts(SchemaLoader schemaLoader) {
        Map<String, DataHostConfig> nodeConf = schemaLoader.getDataHosts();
        //create PhysicalDBPool according to DataHost
        Map<String, PhysicalDataHost> nodes = new HashMap<>(nodeConf.size());
        for (DataHostConfig conf : nodeConf.values()) {
            PhysicalDataHost pool = getPhysicalDBPoolSingleWH(conf);
            nodes.put(pool.getHostName(), pool);
        }
        return nodes;
    }

    private PhysicalDataSource createDataSource(DataHostConfig conf, DataSourceConfig node,
                                                boolean isRead) {
        node.setIdleTimeout(system.getIdleTimeout());
        return new MySQLDataSource(node, conf, isRead);
    }

    private PhysicalDataHost getPhysicalDBPoolSingleWH(DataHostConfig conf) {
        //create PhysicalDatasource for write host
        PhysicalDataSource writeSource = createDataSource(conf, conf.getWriteHost(), false);
        PhysicalDataSource[] readSources = new PhysicalDataSource[conf.getReadHosts().length];
        int i = 0;
        for (DataSourceConfig readNode : conf.getReadHosts()) {
            readSources[i++] = createDataSource(conf, readNode, true);
        }

        return new PhysicalDataHost(conf.getName(), conf, writeSource, readSources, conf.getBalance());
    }

    private Map<String, PhysicalDataNode> initDataNodes(SchemaLoader schemaLoader) {
        Map<String, DataNodeConfig> nodeConf = schemaLoader.getDataNodes();
        Map<String, PhysicalDataNode> nodes = new HashMap<>(nodeConf.size());
        for (DataNodeConfig conf : nodeConf.values()) {
            PhysicalDataHost pool = this.dataHosts.get(conf.getDataHost());
            if (pool == null) {
                throw new ConfigException("dataHost not exists " + conf.getDataHost());
            }
            PhysicalDataNode dataNode = new PhysicalDataNode(conf.getName(), conf.getDatabase(), pool);
            nodes.put(dataNode.getName(), dataNode);
        }
        return nodes;
    }


    public boolean isDataHostWithoutWH() {
        return dataHostWithoutWH;
    }

    public List<ErrorInfo> getErrorInfos() {
        return errorInfos;
    }


}
