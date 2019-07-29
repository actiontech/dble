/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
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

import java.io.IOException;
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
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, PhysicalDBPool> dataHosts;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile boolean dataHostWithoutWH = true;

    private List<ErrorInfo> errorInfos = new ArrayList<>();

    public ConfigInitializer(boolean loadDataHost, boolean lowerCaseNames) {
        //load server.xml
        XMLServerLoader serverLoader = new XMLServerLoader(this);

        //load rule.xml and schema.xml
        SchemaLoader schemaLoader = new XMLSchemaLoader(lowerCaseNames, this);
        this.schemas = schemaLoader.getSchemas();
        this.system = serverLoader.getSystem();
        this.users = serverLoader.getUsers();
        this.erRelations = schemaLoader.getErRelations();
        // need reload DataHost and DataNode?
        if (loadDataHost) {
            this.dataHosts = initDataHosts(schemaLoader);
            this.dataNodes = initDataNodes(schemaLoader);
        } else {
            this.dataHosts = DbleServer.getInstance().getConfig().getDataHosts();
            this.dataNodes = DbleServer.getInstance().getConfig().getDataNodes();
            //add the flag into the reload
            this.dataHostWithoutWH = DbleServer.getInstance().getConfig().isDataHostWithoutWR();
        }

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

    private void checkWriteHost() {
        for (Map.Entry<String, PhysicalDBPool> pool : this.dataHosts.entrySet()) {
            PhysicalDatasource[] writeSource = pool.getValue().getSources();
            if (writeSource != null && writeSource.length != 0) {
                if (writeSource[0].getConfig().isDisabled() && pool.getValue().getReadSources().isEmpty()) {
                    continue;
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
            allUseDataNode.addAll(IncrSequenceMySQLHandler.getInstance().getDataNodes());
        }

        Set<String> allUseHost = new HashSet<>();
        //delete redundancy dataNode
        Iterator<Map.Entry<String, PhysicalDBNode>> iterator = this.dataNodes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PhysicalDBNode> entry = iterator.next();
            String dataNodeName = entry.getKey();
            if (allUseDataNode.contains(dataNodeName)) {
                allUseHost.add(entry.getValue().getDbPool().getHostName());
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
            PhysicalDBPool pool = dataHosts.get(hostName);

            checkMaxCon(pool);

            if (isStart) {
                // start for first time, 1.you can set write host as empty
                if (pool.getSources() == null || pool.getSources().length == 0) {
                    continue;
                }
                DBHostConfig wHost = pool.getSource().getConfig();
                // start for first time, 2.you can set write host as yourself
                if (("localhost".equalsIgnoreCase(wHost.getIp()) || "127.0.0.1".equalsIgnoreCase(wHost.getIp())) &&
                        wHost.getPort() == this.system.getServerPort()) {
                    continue;
                }
            }
            for (PhysicalDatasource ds : pool.getAllDataSources()) {
                if (ds.getConfig().isDisabled()) {
                    errorInfos.add(new ErrorInfo("Backend", "WARNING", "DataHost[" + pool.getHostName() + "," + ds.getName() + "] is disabled"));
                    LOGGER.info("DataHost[" + ds.getHostConfig().getName() + "] is disabled,just mark testing failed and skip it");
                    ds.setTestConnSuccess(false);
                    continue;
                }
                testDataSource(errNodeKeys, errSourceKeys, isConnectivity, isAllDataSourceConnected, nodeList, pool, ds);
            }
            for (PhysicalDatasource[] dataSources : pool.getStandbyReadSourcesMap().values()) {
                for (PhysicalDatasource ds : dataSources) {
                    testDataSource(errNodeKeys, errSourceKeys, isConnectivity, isAllDataSourceConnected, nodeList, pool, ds);
                }
            }
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
        if (!isAllDataSourceConnected.get()) {
            StringBuilder sb = new StringBuilder("SelfCheck### there are some datasource connection failed, pls check these datasource:");
            for (String key : errSourceKeys) {
                sb.append("{");
                sb.append(key);
                sb.append("},");
            }
            throw new ConfigException(sb.toString());
        }
    }


    private void checkMaxCon(PhysicalDBPool pool) {
        int schemasCount = 0;
        for (PhysicalDBNode dn : dataNodes.values()) {
            if (dn.getDbPool() == pool) {
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
                                BoolPtr isAllDataSourceConnected, List<Pair<String, String>> nodeList, PhysicalDBPool pool, PhysicalDatasource ds) {
        boolean isMaster = ds == pool.getSource();
        String dataSourceName = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getName() + "]";
        try {
            BoolPtr isDSConnectedPtr = new BoolPtr(false);
            TestTask testDsTask = new TestTask(ds, errNodeKeys, isDSConnectedPtr);
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
                for (Pair<String, String> node : nodeList) {
                    String key = dataSourceName + ",data_node[" + node.getKey() + "],schema[" + node.getValue() + "]";
                    try {
                        BoolPtr isSchemaConnectedPtr = new BoolPtr(false);
                        TestTask testSchemaTask = new TestTask(ds, node, errNodeKeys, isSchemaConnectedPtr, isMaster);
                        testSchemaTask.start();
                        testSchemaTask.join(3000);
                        boolean isConnected = isSchemaConnectedPtr.get();
                        if (isConnected) {
                            LOGGER.info("SelfCheck### test " + key + " database connection success ");
                        } else {
                            isConnectivity.set(false);
                            errNodeKeys.add(key);
                            LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
                            errorInfos.add(new ErrorInfo("Backend", "WARNING", "Database [" + ds.getHostConfig().getName() +
                                    "," + ds.getName() + "," + node.getValue() + "] not exists"));
                        }
                    } catch (InterruptedException e) {
                        isConnectivity.set(false);
                        errNodeKeys.add(key);
                        LOGGER.warn("test conn " + key + " error:", e);
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
            LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
        }
    }

    private Map<String, List<Pair<String, String>>> genHostSchemaMap() {
        Map<String, List<Pair<String, String>>> hostSchemaMap = new HashMap<>();
        if (this.dataNodes != null && this.dataHosts != null) {
            for (Map.Entry<String, PhysicalDBPool> entry : dataHosts.entrySet()) {
                String hostName = entry.getKey();
                PhysicalDBPool pool = entry.getValue();
                for (PhysicalDBNode dataNode : dataNodes.values()) {
                    if (pool.equals(dataNode.getDbPool())) {
                        List<Pair<String, String>> nodes = hostSchemaMap.get(hostName);
                        if (nodes == null) {
                            nodes = new ArrayList<>();
                            hostSchemaMap.put(hostName, nodes);
                        }
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

    public Map<String, PhysicalDBNode> getDataNodes() {
        return dataNodes;
    }

    public Map<String, PhysicalDBPool> getDataHosts() {
        return this.dataHosts;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    private Map<String, PhysicalDBPool> initDataHosts(SchemaLoader schemaLoader) {
        Map<String, DataHostConfig> nodeConf = schemaLoader.getDataHosts();
        //create PhysicalDBPool according to DataHost
        Map<String, PhysicalDBPool> nodes = new HashMap<>(nodeConf.size());
        for (DataHostConfig conf : nodeConf.values()) {
            //create PhysicalDBPool
            PhysicalDBPool pool = getPhysicalDBPool(conf);
            nodes.put(pool.getHostName(), pool);
        }
        return nodes;
    }

    private PhysicalDatasource[] createDataSource(DataHostConfig conf, DBHostConfig[] nodes,
                                                  boolean isRead) {
        PhysicalDatasource[] dataSources = new PhysicalDatasource[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i].setIdleTimeout(system.getIdleTimeout());
            MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
            dataSources[i] = ds;
        }
        return dataSources;
    }

    private PhysicalDBPool getPhysicalDBPool(DataHostConfig conf) {
        //create PhysicalDatasource for write host
        PhysicalDatasource[] writeSources = createDataSource(conf, conf.getWriteHosts(), false);
        Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
        Map<Integer, PhysicalDatasource[]> readSourcesMap = new HashMap<>(readHostsMap.size());
        //create map for read host:writeHost-> readHost's  PhysicalDatasource[]
        for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
            PhysicalDatasource[] readSources = createDataSource(conf, entry.getValue(), true);
            readSourcesMap.put(entry.getKey(), readSources);
        }

        Map<Integer, DBHostConfig[]> standbyReadHostsMap = conf.getStandbyReadHosts();
        Map<Integer, PhysicalDatasource[]> standbyReadSourcesMap = new HashMap<>(standbyReadHostsMap.size());
        for (Map.Entry<Integer, DBHostConfig[]> entry : standbyReadHostsMap.entrySet()) {
            PhysicalDatasource[] standbyReadSources = createDataSource(conf, entry.getValue(), true);
            standbyReadSourcesMap.put(entry.getKey(), standbyReadSources);
        }

        return new PhysicalDBPool(conf.getName(), conf, writeSources, readSourcesMap, standbyReadSourcesMap, conf.getBalance());
    }

    private Map<String, PhysicalDBNode> initDataNodes(SchemaLoader schemaLoader) {
        Map<String, DataNodeConfig> nodeConf = schemaLoader.getDataNodes();
        Map<String, PhysicalDBNode> nodes = new HashMap<>(nodeConf.size());
        for (DataNodeConfig conf : nodeConf.values()) {
            PhysicalDBPool pool = this.dataHosts.get(conf.getDataHost());
            if (pool == null) {
                throw new ConfigException("dataHost not exists " + conf.getDataHost());
            }
            PhysicalDBNode dataNode = new PhysicalDBNode(conf.getName(), conf.getDatabase(), pool);
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

    private static class TestTask extends Thread {
        private PhysicalDatasource ds;
        private BoolPtr boolPtr;
        private Set<String> errKeys;
        private String schema = null;
        private String nodeName = null;
        private boolean needAlert = false;

        TestTask(PhysicalDatasource ds, Set<String> errKeys, BoolPtr boolPtr) {
            this.ds = ds;
            this.errKeys = errKeys;
            this.boolPtr = boolPtr;
        }

        TestTask(PhysicalDatasource ds, Pair<String, String> node, Set<String> errKeys, BoolPtr boolPtr, boolean needAlert) {
            this.ds = ds;
            this.errKeys = errKeys;
            this.boolPtr = boolPtr;
            this.needAlert = needAlert;
            if (node != null) {
                this.nodeName = node.getKey();
                this.schema = node.getValue();
            }
        }

        @Override
        public void run() {
            try {
                boolean isConnected = ds.testConnection(schema);
                boolPtr.set(isConnected);
            } catch (IOException e) {
                boolPtr.set(false);
                if (schema != null && needAlert) {
                    String key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + nodeName + "],schema[" + schema + "]";
                    errKeys.add(key);
                    LOGGER.warn("test conn " + key + " error:", e);
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                    labels.put("data_node", nodeName);
                    AlertUtil.alert(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", ds.getConfig().getId(), labels);
                    ToResolveContainer.DATA_NODE_LACK.add(key);
                }
            }
        }
    }

}
