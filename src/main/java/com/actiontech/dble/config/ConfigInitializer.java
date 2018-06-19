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
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.route.sequence.handler.IncrSequenceMySQLHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @author mycat
 */
public class ConfigInitializer {

    private static final Logger LOGGER = Logger.getLogger(ConfigInitializer.class);

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
        if (this.dataNodes != null && this.dataHosts != null) {
            Set<String> errKeys = new HashSet<>();
            boolean isConnectivity = true;
            for (PhysicalDBNode dataNode : dataNodes.values()) {
                String database = dataNode.getDatabase();
                PhysicalDBPool pool = dataNode.getDbPool();
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
                    String key = ds.getName() + "_" + database;
                    if (ds.getConfig().isFake()) {
                        LOGGER.info(key + " is an empty and faked config,just mark testing failed and skip it");
                        ds.setTestConnSuccess(false);
                        continue;
                    }
                    try {
                        boolean isConnected = ds.testConnection(database);
                        ds.setTestConnSuccess(isConnected);
                        if (isConnected) {
                            LOGGER.info("SelfCheck### test " + key + " database connection success ");
                        } else {
                            isConnectivity = false;
                            errKeys.add(key);
                            LOGGER.warn(AlarmCode.CORE_GENERAL_WARN + "SelfCheck### test " + key + " database connection failed ");
                        }
                    } catch (IOException e) {
                        isConnectivity = false;
                        errKeys.add(key);
                        LOGGER.warn(AlarmCode.CORE_GENERAL_WARN + "test conn " + key + " error:", e);
                    }
                }
            }
            if (!isConnectivity) {
                StringBuilder sb = new StringBuilder("SelfCheck### there are some datasource connection failed, pls check these datasource:");
                for (String key : errKeys) {
                    sb.append("[");
                    sb.append(key);
                    sb.append("].");
                }

                throw new ConfigException(sb.toString());
            }
        }
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



}
