/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.backend.mysql.nio.handler.GetConnectionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.helper.GetAndSyncDataSourceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DataSourceStatus;
import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PhysicalDataHost {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDataHost.class);
    public static final String JSON_NAME = "dataHost";
    public static final String JSON_LIST = "dataSources";
    public static final String JSON_WRITE_SOURCE = "writeSource";

    private volatile PhysicalDataSource writeSource;
    private Map<String, PhysicalDataSource> allSourceMap = new ConcurrentHashMap<>();
    public static final int BALANCE_NONE = 0;
    protected static final int BALANCE_ALL_BACK = 1;
    protected static final int BALANCE_ALL = 2;

    public static final int WEIGHT = 0;

    private final String hostName;
    private final int balance;
    private final DataHostConfig dataHostConfig;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private volatile boolean initSuccess = false;
    protected String[] schemas;

    private PhysicalDataSource[] readSources;

    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();


    public PhysicalDataHost(String name, DataHostConfig config, PhysicalDataSource writeSource, PhysicalDataSource[] readSources, int balance) {
        this.hostName = name;
        this.balance = balance;
        this.dataHostConfig = config;
        this.writeSource = writeSource;
        this.readSources = readSources;
        allSourceMap.put(writeSource.getName(), writeSource);

        for (PhysicalDataSource s : readSources) {
            allSourceMap.put(s.getName(), s);
        }
        setDataSourceProps();
    }

    public PhysicalDataHost(PhysicalDataHost org) {
        this.hostName = org.hostName;
        this.balance = org.balance;
        this.dataHostConfig = org.dataHostConfig;
        allSourceMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, PhysicalDataSource> entry : org.allSourceMap.entrySet()) {
            MySQLDataSource newSource = new MySQLDataSource((MySQLDataSource) entry.getValue());
            allSourceMap.put(entry.getKey(), newSource);
            if (entry.getValue() == org.writeSource) {
                writeSource = newSource;
            }
        }
    }


    public boolean isInitSuccess() {
        return initSuccess;
    }



    public String[] getSchemas() {
        return schemas;
    }

    public void setSchemas(String[] mySchemas) {
        this.schemas = mySchemas;
    }

    public DataHostConfig getDataHostConfig() {
        return dataHostConfig;
    }


    PhysicalDataSource findDatasource(BackendConnection exitsCon) {
        MySQLConnection con = (MySQLConnection) exitsCon;
        PhysicalDataSource source = allSourceMap.get(con.getPool().getName());
        if (source != null && source == con.getPool()) {
            return source;
        }
        LOGGER.info("can't find connection in pool " + this.hostName + " con:" + exitsCon);
        return null;
    }

    boolean isSlave(PhysicalDataSource ds) {
        return !(writeSource == ds);
    }


    public PhysicalDataSource getWriteSource() {
        return writeSource;
    }

    public boolean init() {
        if (balance != 0) {
            for (Map.Entry<String, PhysicalDataSource> entry : allSourceMap.entrySet()) {
                if (initSource(entry.getValue())) {
                    initSuccess = true;
                    LOGGER.info(hostName + " " + entry.getKey() + " init success");
                }
            }
        } else {
            if (initSource(writeSource)) {
                initSuccess = true;
                LOGGER.info(hostName + " " + writeSource.getName() + " init success");
            }
        }
        if (!initSuccess) {
            LOGGER.warn(hostName + " init failure");
        }
        return initSuccess;
    }

    public void doHeartbeat() {
        for (PhysicalDataSource source : allSourceMap.values()) {
            if (source != null) {
                source.doHeartbeat();
            } else {
                LOGGER.warn(hostName + " current dataSource is null!");
            }
        }
    }

    public void heartbeatCheck(long ildCheckPeriod) {
        for (PhysicalDataSource ds : allSourceMap.values()) {
            // only read node or all write node
            // and current write node will check
            if (ds != null && (ds.getHeartbeat().getStatus() == MySQLHeartbeat.OK_STATUS) &&
                    (ds.isReadNode() || ds == this.getWriteSource())) {
                ds.connectionHeatBeatCheck(ildCheckPeriod);
            }
        }
    }

    public void startHeartbeat() {
        for (PhysicalDataSource source : allSourceMap.values()) {
            source.startHeartbeat();
        }
    }

    public void stopHeartbeat() {
        for (PhysicalDataSource source : allSourceMap.values()) {
            source.stopHeartbeat();
        }
    }

    public void clearDataSources(String reason) {
        for (PhysicalDataSource source : allSourceMap.values()) {
            LOGGER.info("clear datasource of pool  " + this.hostName + " ds:" + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }
    }

    public Collection<PhysicalDataSource> getAllActiveDataSources() {
        if (this.dataHostConfig.getBalance() != BALANCE_NONE) {
            return allSourceMap.values();
        } else {
            return Collections.singletonList(writeSource);
        }
    }


    public Collection<PhysicalDataSource> getAllDataSources() {
        return new LinkedList<>(allSourceMap.values());
    }

    void getRWBalanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {
        PhysicalDataSource theNode = getRWBalanceNode();
        if (theNode.isDisabled()) {
            if (this.getReadSources().length > 0) {
                theNode = this.getReadSources()[0];
            } else {
                String errorMsg = "the dataHost[" + theNode.getHostConfig().getName() + "] is disabled, please check it";
                throw new IOException(errorMsg);
            }
        }
        if (!theNode.isAlive()) {
            String heartbeatError = "the data source[" + theNode.getConfig().getUrl() + "] can't reach. Please check the dataHost status";
            if (dataHostConfig.isShowSlaveSql()) {
                heartbeatError += ",Tip:heartbeat[show slave status] need the SUPER or REPLICATION CLIENT privilege(s)";
            }
            LOGGER.warn(heartbeatError);
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", theNode.getHostConfig().getName() + "-" + theNode.getConfig().getHostName());
            AlertUtil.alert(AlarmCode.DATA_HOST_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", theNode.getConfig().getId(), labels);
            throw new IOException(heartbeatError);
        }
        theNode.getConnection(schema, autocommit, handler, attachment, false);
    }

    PhysicalDataSource getRWBalanceNode() {
        PhysicalDataSource theNode;
        ArrayList<PhysicalDataSource> okSources;
        switch (balance) {
            case BALANCE_ALL: {
                okSources = getAllActiveRWSources(true, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_ALL_BACK: {
                okSources = getAllActiveRWSources(false, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_NONE:
            default:
                // return default write data source
                theNode = this.getWriteSource();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read source " + theNode.getName() + " for dataHost:" + this.getHostName());
        }
        theNode.setReadCount();
        return theNode;
    }

    PhysicalDataSource getReadNode() throws Exception {
        for (int i = 0; i < allSourceMap.size(); i++) {
            int index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % allSourceMap.size();
            PhysicalDataSource ds = (PhysicalDataSource) allSourceMap.values().toArray()[index];
            if (ds != writeSource && ds.isAlive()) {
                if (checkSlaveSynStatus() && canSelectAsReadNode(ds)) {
                    return ds;
                } else if (!checkSlaveSynStatus()) {
                    return ds;
                }
            }
        }
        return null;
    }

    boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws
            Exception {
        LOGGER.debug("!readSources.isEmpty() " + (allSourceMap.values().size() > 1));
        if (allSourceMap.values().size() > 1) {
            PhysicalDataSource theNode = getReadNode();
            if (theNode != null) {
                theNode.setReadCount();
                theNode.getConnection(schema, autocommit, handler, attachment, false);
                return true;
            } else {
                LOGGER.info("read host is not available.");
                return false;
            }
        } else {
            LOGGER.info("read host is empty, readSources is empty.");
            return false;
        }
    }

    public PhysicalDataSource[] getReadSources() {
        if (this.dataHostConfig.getBalance() != BALANCE_NONE) {
            return readSources;
        } else {
            return new PhysicalDataSource[0];
        }
    }

    public Map<Integer, PhysicalDataSource[]> getReadSourceAll() {
        PhysicalDataSource[] list = new PhysicalDataSource[allSourceMap.size() - 1];
        int i = 0;
        for (PhysicalDataSource ds : allSourceMap.values()) {
            if (ds != writeSource) {
                list[i++] = ds;
            }
        }

        Map<Integer, PhysicalDataSource[]> result = new HashMap<Integer, PhysicalDataSource[]>();
        if (list.length > 0) {
            result.put(0, list);
        }
        return result;
    }

    private void setDataSourceProps() {
        for (PhysicalDataSource ds : this.allSourceMap.values()) {
            ds.setDataHost(this);
        }
    }


    private boolean initSource(PhysicalDataSource ds) {
        if (ds.getConfig().isDisabled()) {
            LOGGER.info(ds.getConfig().getHostName() + " is disabled, skipped");
            return true;
        }
        int initSize = ds.getConfig().getMinCon();
        if (initSize < this.schemas.length + 1) {
            initSize = this.schemas.length + 1;
            LOGGER.warn("minCon size is less than (the count of schema +1), so dble will create at least 1 conn for every schema and an empty schema conn, " +
                    "minCon size before:{}, now:{}", ds.getConfig().getMinCon(), initSize);
            ds.getConfig().setMinCon(initSize);
        }

        if (ds.getConfig().getMaxCon() < initSize) {
            ds.getConfig().setMaxCon(initSize);
            ds.setSize(initSize);
            LOGGER.warn("maxCon is less than the initSize of dataHost:" + initSize + " change the maxCon into " + initSize);
        }

        LOGGER.info("init backend mysql source ,create connections total " + initSize + " for " + ds.getName());

        CopyOnWriteArrayList<BackendConnection> list = new CopyOnWriteArrayList<>();
        GetConnectionHandler getConHandler = new GetConnectionHandler(list, initSize);
        // long start = System.currentTimeMillis();
        // long timeOut = start + 5000 * 1000L;
        boolean hasConnectionInPool = false;
        try {
            if (ds.getTotalConCount() <= 0) {
                ds.initMinConnection(null, true, getConHandler, null);
            } else {
                LOGGER.info("connection with null schema has been created,because we tested the connection of the datasource at first");
                getConHandler.initIncrement();
                hasConnectionInPool = true;
            }
        } catch (Exception e) {
            LOGGER.warn("init connection with schema null error", e);
        }

        for (int i = 0; i < initSize - 1; i++) {
            try {
                ds.initMinConnection(this.schemas[i % schemas.length], true, getConHandler, null);
            } catch (Exception e) {
                LOGGER.warn(ds.getName() + " init connection error.", e);
            }
        }


        long timeOut = System.currentTimeMillis() + 60 * 1000;

        // waiting for finish
        while (!getConHandler.finished() && (System.currentTimeMillis() < timeOut)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                /*
                 * hardly triggered no error is needed
                 */
                LOGGER.info("initError", e);
            }
        }
        LOGGER.info("init result :" + getConHandler.getStatusInfo());
        return !list.isEmpty() || hasConnectionInPool;
    }


    private ArrayList<PhysicalDataSource> getAllActiveRWSources(boolean includeWriteNode, boolean filterWithSlaveThreshold) {
        ArrayList<PhysicalDataSource> okSources = new ArrayList<>(allSourceMap.values().size());

        for (PhysicalDataSource ds : allSourceMap.values()) {
            if (!includeWriteNode && ds == writeSource) {
                continue;
            } else {
                if (ds.isAlive()) {
                    if (writeSource == ds || (!filterWithSlaveThreshold || canSelectAsReadNode(ds))) {
                        okSources.add(ds);
                    }
                }
            }
        }

        return okSources;
    }


    public String disableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {

            HaConfigManager.getInstance().updateConfDataHost(createDisableSnapshot(this, nameList), syncWriteConf);

            for (String dsName : nameList) {
                PhysicalDataSource datasource = allSourceMap.get(dsName);
                if (datasource.setDisabled(true)) {
                    //clear old resource
                    datasource.clearCons("ha command disable datasource");
                    datasource.stopHeartbeat();
                }
            }
            return this.getClusterHaJson();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    public PhysicalDataHost createDisableSnapshot(PhysicalDataHost org, String[] nameList) {
        PhysicalDataHost snapshot = new PhysicalDataHost(org);
        for (String dsName : nameList) {
            PhysicalDataSource datasource = snapshot.allSourceMap.get(dsName);
            datasource.setDisabled(true);
        }
        return snapshot;
    }


    public String enableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {

            HaConfigManager.getInstance().updateConfDataHost(createEnableSnapshot(this, nameList), syncWriteConf);

            for (String dsName : nameList) {
                PhysicalDataSource datasource = allSourceMap.get(dsName);
                if (datasource.setDisabled(false)) {
                    datasource.startHeartbeat();
                }
            }
            return this.getClusterHaJson();
        } catch (Exception e) {
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    public PhysicalDataHost createEnableSnapshot(PhysicalDataHost org, String[] nameList) {
        PhysicalDataHost snapshot = new PhysicalDataHost(org);
        for (String dsName : nameList) {
            PhysicalDataSource datasource = allSourceMap.get(dsName);
            datasource.setDisabled(false);
        }
        return snapshot;
    }

    public String switchMaster(String writeHost, boolean syncWriteConf) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            HaConfigManager.getInstance().updateConfDataHost(createSwitchSnapshot(writeHost), syncWriteConf);

            PhysicalDataSource newWriteHost = allSourceMap.get(writeHost);
            writeSource.setReadNode(true);
            //close all old master connection ,so that new write query would not put into the old writeHost
            writeSource.clearCons("ha command switch datasource");
            if (!newWriteHost.isDisabled()) {
                GetAndSyncDataSourceKeyVariables task = new GetAndSyncDataSourceKeyVariables(newWriteHost);
                KeyVariables variables = task.call();
                if (variables != null) {
                    newWriteHost.setReadOnly(variables.isReadOnly());
                } else {
                    LOGGER.warn(" GetAndSyncDataSourceKeyVariables failed, set newWriteHost ReadOnly");
                    newWriteHost.setReadOnly(true);
                }
            }
            newWriteHost.setReadNode(false);
            writeSource = newWriteHost;
            return this.getClusterHaJson();
        } catch (Exception e) {
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    public PhysicalDataHost createSwitchSnapshot(String writeHost) {
        PhysicalDataHost snapshot = new PhysicalDataHost(this);
        PhysicalDataSource newWriteHost = snapshot.allSourceMap.get(writeHost);
        snapshot.writeSource.setReadNode(true);
        newWriteHost.setReadNode(false);
        snapshot.writeSource = newWriteHost;
        return snapshot;
    }


    public void changeIntoLatestStatus(String jsonStatus) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            JSONObject jsonObj = JSONObject.parseObject(jsonStatus);
            JsonProcessBase base = new JsonProcessBase();
            Type parseType = new TypeToken<List<DataSourceStatus>>() {
            }.getType();
            List<DataSourceStatus> list = base.toBeanformJson(jsonObj.getJSONArray(JSON_LIST).toJSONString(), parseType);
            for (DataSourceStatus status : list) {
                PhysicalDataSource phys = allSourceMap.get(status.getName());
                if (phys != null) {
                    if (phys.setDisabled(status.isDisable())) {
                        if (status.isDisable()) {
                            //clear old resource
                            phys.clearCons("ha command disable datasource");
                            phys.stopHeartbeat();
                        } else {
                            //change dataSource from disable to enable ,start heartbeat
                            phys.startHeartbeat();
                        }
                    }
                    if (status.isWriteHost() &&
                            phys != writeSource) {
                        writeSource.setReadNode(true);
                        writeSource.clearCons("ha command switch datasource");
                        phys.setReadNode(false);
                        writeSource = phys;
                    }
                } else {
                    LOGGER.warn("Can match dataSource" + status.getName() + ".Check for the config file please");
                }
            }
            HaConfigManager.getInstance().updateConfDataHost(this, false);
        } catch (Exception e) {
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    public String getClusterHaJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JSON_NAME, this.getHostName());
        List<DataSourceStatus> list = new ArrayList<>();
        for (PhysicalDataSource phys : allSourceMap.values()) {
            list.add(new DataSourceStatus(phys.getName(), phys.isDisabled(), !phys.isReadNode()));
        }
        jsonObject.put(JSON_LIST, list);
        jsonObject.put(JSON_WRITE_SOURCE, new DataSourceStatus(writeSource.getName(), writeSource.isDisabled(), !writeSource.isReadNode()));
        return jsonObject.toJSONString();
    }

    public boolean checkDataSourceExist(String subHostName) {
        //add check for subHostName
        if (subHostName != null) {
            for (String dn : subHostName.split(",")) {
                boolean find = false;
                for (PhysicalDataSource pds : this.getAllDataSources()) {
                    if (pds.getName().equals(dn)) {
                        find = true;
                        break;
                    }
                }
                if (!find) {
                    return false;
                }
            }
        }
        return true;
    }
    public String getHostName() {
        return hostName;
    }

    public PhysicalDataSource randomSelect(ArrayList<PhysicalDataSource> okSources) {
        if (okSources.isEmpty()) {
            return this.getWriteSource();
        } else {
            int length = okSources.size();
            int totalWeight = 0;
            boolean sameWeight = true;
            for (int i = 0; i < length; i++) {
                int weight = okSources.get(i).getConfig().getWeight();
                totalWeight += weight;
                if (sameWeight && i > 0 && weight != okSources.get(i - 1).getConfig().getWeight()) {
                    sameWeight = false;
                }
            }

            if (totalWeight > 0 && !sameWeight) {
                // random by different weight
                int offset = random.nextInt(totalWeight);
                for (PhysicalDataSource okSource : okSources) {
                    offset -= okSource.getConfig().getWeight();
                    if (offset < 0) {
                        return okSource;
                    }
                }
            }
            return okSources.get(random.nextInt(length));
        }
    }


    protected boolean checkSlaveSynStatus() {
        return (dataHostConfig.getSlaveThreshold() != -1) &&
                (dataHostConfig.isShowSlaveSql());
    }

    protected boolean canSelectAsReadNode(PhysicalDataSource theSource) {
        Integer slaveBehindMaster = theSource.getHeartbeat().getSlaveBehindMaster();
        int dbSynStatus = theSource.getHeartbeat().getDbSynStatus();
        if (slaveBehindMaster == null || dbSynStatus == MySQLHeartbeat.DB_SYN_ERROR) {
            return false;
        }
        boolean isSync = dbSynStatus == MySQLHeartbeat.DB_SYN_NORMAL;
        boolean isNotDelay = slaveBehindMaster < this.dataHostConfig.getSlaveThreshold();
        return isSync && isNotDelay;
    }


    boolean equalsBaseInfo(PhysicalDataHost pool) {
        return pool.getDataHostConfig().getName().equals(this.dataHostConfig.getName()) &&
                pool.getDataHostConfig().getHearbeatSQL().equals(this.dataHostConfig.getHearbeatSQL()) &&
                pool.getDataHostConfig().getHeartbeatTimeout() == this.dataHostConfig.getHeartbeatTimeout() &&
                pool.getDataHostConfig().getErrorRetryCount() == this.dataHostConfig.getErrorRetryCount() &&
                pool.getDataHostConfig().getBalance() == this.dataHostConfig.getBalance() &&
                pool.getDataHostConfig().getMaxCon() == this.dataHostConfig.getMaxCon() &&
                pool.getDataHostConfig().getMinCon() == this.dataHostConfig.getMinCon() &&
                pool.getHostName().equals(this.hostName);
    }
}
