/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

import com.actiontech.dble.DbleServer;
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
import com.actiontech.dble.route.sequence.handler.IncrSequenceMySQLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author mycat
 */
public class ConfigInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigInitializer.class);

    private volatile SystemConfig system;
    private volatile FirewallConfig firewall;
    private volatile Map<String, UserConfig> users;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, PhysicalDBPool> dataHosts;
    private volatile Map<ERTable, Set<ERTable>> erRelations;


    private volatile boolean dataHostWithoutWH = true;

    public ConfigInitializer(boolean loadDataHost, boolean lowerCaseNames) {
        //load server.xml
        XMLServerLoader serverLoader = new XMLServerLoader();

        //load rule.xml and schema.xml
        SchemaLoader schemaLoader = new XMLSchemaLoader(lowerCaseNames);
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

    private void checkWriteHost() {
        for (Map.Entry<String, PhysicalDBPool> pool : this.dataHosts.entrySet()) {
            PhysicalDatasource[] writeSource = pool.getValue().getSources();
            if (writeSource != null && writeSource.length != 0) {
                if (writeSource[0].getConfig().isFake() && pool.getValue().getReadSources().isEmpty()) {
                    continue;
                }
                this.dataHostWithoutWH = false;
                break;
            }
        }
    }

    private void deleteRedundancyConf() {
        Set<String> allUseDataNode = new HashSet<>();

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
                    dataHost.remove();
                }
            }
        }
        allUseHost.clear();
    }

    public void testConnection(boolean isStart) {
        Map<String, List<String>> hostSchemaMap = genHostSchemaMap();
        Set<String> errNodeKeys = new HashSet<>();
        Set<String> errSourceKeys = new HashSet<>();
        boolean isConnectivity = true;
        boolean isAllDataSourceConnected = true;
        for (Map.Entry<String, List<String>> entry : hostSchemaMap.entrySet()) {
            String hostName = entry.getKey();
            List<String> schemaList = entry.getValue();
            PhysicalDBPool pool = dataHosts.get(hostName);
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
                if (ds.getConfig().isFake()) {
                    LOGGER.info("DataHost[" + ds.getHostConfig().getName() + "] contains an empty and faked config,just mark testing failed and skip it");
                    ds.setTestConnSuccess(false);
                    continue;
                }
                String dataSourceName = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getName() + "]";
                try {
                    BoolPtr isDSConnectedPtr = new BoolPtr(false);
                    TestTask testDsTask = new TestTask(ds, null, errNodeKeys, isDSConnectedPtr);
                    testDsTask.start();
                    testDsTask.join(3000);
                    boolean isDataSourceConnected = isDSConnectedPtr.get();
                    ds.setTestConnSuccess(isDataSourceConnected);
                    if (!isDataSourceConnected) {
                        isConnectivity = false;
                        isAllDataSourceConnected = false;
                        errSourceKeys.add(dataSourceName);
                        markDataSourceSchemaFail(errNodeKeys, schemaList, dataSourceName);
                    } else {
                        for (String schema : schemaList) {
                            String key = dataSourceName + ",schema[" + schema + "]";
                            try {
                                BoolPtr isSchemaConnectedPtr = new BoolPtr(false);
                                TestTask testSchemaTask = new TestTask(ds, schema, errNodeKeys, isSchemaConnectedPtr);
                                testSchemaTask.start();
                                testSchemaTask.join(3000);
                                boolean isConnected = isSchemaConnectedPtr.get();
                                if (isConnected) {
                                    LOGGER.info("SelfCheck### test " + key + " database connection success ");
                                } else {
                                    isConnectivity = false;
                                    errNodeKeys.add(key);
                                    LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
                                }
                            } catch (InterruptedException e) {
                                isConnectivity = false;
                                errNodeKeys.add(key);
                                LOGGER.warn("test conn " + key + " error:", e);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    isConnectivity = false;
                    isAllDataSourceConnected = false;
                    errSourceKeys.add(dataSourceName);
                    markDataSourceSchemaFail(errNodeKeys, schemaList, dataSourceName);
                }

            }
        }
        if (!isConnectivity) {
            StringBuilder sb = new StringBuilder("SelfCheck### there are some data node connection failed, pls check these datasource:");
            for (String key : errNodeKeys) {
                sb.append("{");
                sb.append(key);
                sb.append("},");
            }
            LOGGER.warn(sb.toString());
        }
        if (!isAllDataSourceConnected) {
            StringBuilder sb = new StringBuilder("SelfCheck### there are some datasource connection failed, pls check these datasource:");
            for (String key : errSourceKeys) {
                sb.append("{");
                sb.append(key);
                sb.append("},");
            }
            throw new ConfigException(sb.toString());
        }
    }

    private void markDataSourceSchemaFail(Set<String> errKeys, List<String> schemaList, String dataSourceName) {
        for (String schema : schemaList) {
            String key = dataSourceName + ",schema[" + schema + "]";
            errKeys.add(key);
            LOGGER.warn("SelfCheck### test " + key + " database connection failed ");
        }
    }

    private Map<String, List<String>> genHostSchemaMap() {
        Map<String, List<String>> hostSchemaMap = new HashMap<>();
        if (this.dataNodes != null && this.dataHosts != null) {
            for (Map.Entry<String, PhysicalDBPool> entry : dataHosts.entrySet()) {
                String hostName = entry.getKey();
                PhysicalDBPool pool = entry.getValue();
                for (PhysicalDBNode dataNode : dataNodes.values()) {
                    if (pool.equals(dataNode.getDbPool())) {
                        List<String> nodes = hostSchemaMap.get(hostName);
                        if (nodes == null) {
                            nodes = new ArrayList<>();
                            hostSchemaMap.put(hostName, nodes);
                        }
                        nodes.add(dataNode.getDatabase());
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

        return new PhysicalDBPool(conf.getName(), conf, writeSources, readSourcesMap, conf.getBalance());
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

    private static class TestTask extends Thread {
        private PhysicalDatasource ds;
        private String schema;
        private BoolPtr boolPtr;
        private Set<String> errKeys;

        TestTask(PhysicalDatasource ds, String schema, Set<String> errKeys, BoolPtr boolPtr) {
            this.ds = ds;
            this.schema = schema;
            this.errKeys = errKeys;
            this.boolPtr = boolPtr;
        }

        @Override
        public void run() {
            try {
                boolean isConnected = ds.testConnection(schema);
                boolPtr.set(isConnected);
            } catch (IOException e) {
                if (schema != null) {
                    String key = ds.getName() + ",schema[" + schema + "]";
                    errKeys.add(key);
                    LOGGER.warn("test conn " + key + " error:", e);
                }
            }
        }
    }

}
