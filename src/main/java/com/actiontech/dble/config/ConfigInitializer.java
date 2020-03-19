/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
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
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, AbstractPhysicalDBPool> dataHosts;
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
        for (Map.Entry<String, AbstractPhysicalDBPool> pool : this.dataHosts.entrySet()) {
            PhysicalDatasource writeSource = pool.getValue().getSource();
            if (writeSource != null) {
                if (writeSource.getConfig().isDisabled()) {
                    boolean hasEnableNode = false;
                    for (PhysicalDatasource readSource : pool.getValue().getReadSources()) {
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
            AbstractPhysicalDBPool pool = dataHosts.get(hostName);

            checkMaxCon(pool);

            if (isStart) {
                // start for first time, 1.you can set write host as empty
                if (pool.getSource() == null) {
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


    private void checkMaxCon(AbstractPhysicalDBPool pool) {
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
                                BoolPtr isAllDataSourceConnected, List<Pair<String, String>> nodeList, AbstractPhysicalDBPool pool, PhysicalDatasource ds) {
        boolean isMaster = ds == pool.getSource();
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
            for (Map.Entry<String, AbstractPhysicalDBPool> entry : dataHosts.entrySet()) {
                String hostName = entry.getKey();
                AbstractPhysicalDBPool pool = entry.getValue();
                for (PhysicalDBNode dataNode : dataNodes.values()) {
                    if (pool.equals(dataNode.getDbPool())) {
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

    public Map<String, PhysicalDBNode> getDataNodes() {
        return dataNodes;
    }

    public Map<String, AbstractPhysicalDBPool> getDataHosts() {
        return this.dataHosts;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        return erRelations;
    }

    private Map<String, AbstractPhysicalDBPool> initDataHosts(SchemaLoader schemaLoader) {
        Map<String, DataHostConfig> nodeConf = schemaLoader.getDataHosts();
        //create PhysicalDBPool according to DataHost
        Map<String, AbstractPhysicalDBPool> nodes = new HashMap<>(nodeConf.size());
        for (DataHostConfig conf : nodeConf.values()) {
            AbstractPhysicalDBPool pool = getPhysicalDBPoolSingleWH(conf);
            nodes.put(pool.getHostName(), pool);
        }
        return nodes;
    }

    private PhysicalDatasource createDataSource(DataHostConfig conf, DBHostConfig node,
                                                  boolean isRead) {
        node.setIdleTimeout(system.getIdleTimeout());
        return new MySQLDataSource(node, conf, isRead);
    }

    private PhysicalDNPoolSingleWH getPhysicalDBPoolSingleWH(DataHostConfig conf) {
        //create PhysicalDatasource for write host
        PhysicalDatasource writeSource = createDataSource(conf, conf.getWriteHost(), false);
        PhysicalDatasource[] readSources = new PhysicalDatasource[conf.getReadHosts().length];
        int i = 0;
        for (DBHostConfig readNode : conf.getReadHosts()) {
            readSources[i++] = createDataSource(conf, readNode, true);
        }

        return new PhysicalDNPoolSingleWH(conf.getName(), conf, writeSource, readSources, conf.getBalance());
    }

    private Map<String, PhysicalDBNode> initDataNodes(SchemaLoader schemaLoader) {
        Map<String, DataNodeConfig> nodeConf = schemaLoader.getDataNodes();
        Map<String, PhysicalDBNode> nodes = new HashMap<>(nodeConf.size());
        for (DataNodeConfig conf : nodeConf.values()) {
            AbstractPhysicalDBPool pool = this.dataHosts.get(conf.getDataHost());
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


}
