package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.GetConnectionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.loader.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DataSourceStatus;
import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by szf on 2019/10/17.
 */
public class PhysicalDNPoolSingleWH extends AbstractPhysicalDBPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDNPoolSingleWH.class);
    public static final String JSON_NAME = "dataHost";
    public static final String JSON_LIST = "dataSources";
    public static final String JSON_WRITE_SOURCE = "writeSource";

    private volatile PhysicalDatasource writeSource;

    private Map<String, PhysicalDatasource> allSourceMap = new ConcurrentHashMap<>();


    public PhysicalDNPoolSingleWH(String name, DataHostConfig conf, PhysicalDatasource[] writeSources, Map<Integer, PhysicalDatasource[]> readSources, Map<Integer, PhysicalDatasource[]> standbyReadSourcesMap, int balance) {
        super(name, balance, conf);
        this.writeSource = writeSources[0];
        allSourceMap.put(writeSource.getName(), writeSource);
        PhysicalDatasource[] read = readSources.get(new Integer(0));
        PhysicalDatasource[] disabled = standbyReadSourcesMap.get(new Integer(0));
        putAllIntoMap(read);
        putAllIntoMap(disabled);
        setDataSourceProps();
    }

    @Override
    PhysicalDatasource findDatasource(BackendConnection exitsCon) {
        MySQLConnection con = (MySQLConnection) exitsCon;
        PhysicalDatasource source = allSourceMap.get(con.getPool().getName());
        if (source != null && source == con.getPool()) {
            return source;
        }
        LOGGER.info("can't find connection in pool " + this.hostName + " con:" + exitsCon);
        return null;
    }

    @Override
    boolean isSlave(PhysicalDatasource ds) {
        return !(writeSource == ds);
    }

    @Override
    public PhysicalDatasource[] getSources() {
        PhysicalDatasource[] list = new PhysicalDatasource[1];
        list[0] = writeSource;
        return list;
    }

    @Override
    public PhysicalDatasource getSource() {
        return writeSource;
    }

    @Override
    public boolean switchSource(int newIndex, String reason) {
        //not allowed when use the outter ha
        throw new RuntimeException("not allow in this");
    }

    @Override
    public void init(int index) {
        init();
    }

    public void init() {
        for (Map.Entry<String, PhysicalDatasource> entry : allSourceMap.entrySet()) {
            if (initSource(entry.getValue())) {
                initSuccess = true;
                LOGGER.info(hostName + " " + entry.getKey() + " init success");
            }
        }
        if (initSuccess) {
            LOGGER.warn(hostName + " init failure");
        }
    }

    @Override
    public void reloadInit(int index) {
        if (initSuccess) {
            LOGGER.info(hostName + "dataHost already inited doing nothing");
            return;
        }
        init();
    }

    @Override
    public void doHeartbeat() {
        for (PhysicalDatasource source : allSourceMap.values()) {
            if (source != null) {
                source.doHeartbeat();
            } else {
                LOGGER.warn(hostName + " current dataSource is null!");
            }
        }
    }

    @Override
    public void heartbeatCheck(long ildCheckPeriod) {
        for (PhysicalDatasource ds : allSourceMap.values()) {
            // only read node or all write node
            // and current write node will check
            if (ds != null && (ds.getHeartbeat().getStatus() == MySQLHeartbeat.OK_STATUS) &&
                    (ds.isReadNode() || ds == this.getSource())) {
                ds.connectionHeatBeatCheck(ildCheckPeriod);
            }
        }
    }

    @Override
    public void startHeartbeat() {
        for (PhysicalDatasource source : allSourceMap.values()) {
            source.startHeartbeat();
        }
    }

    @Override
    public void stopHeartbeat() {
        for (PhysicalDatasource source : allSourceMap.values()) {
            source.stopHeartbeat();
        }
    }

    @Override
    public void clearDataSources(String reason) {
        for (PhysicalDatasource source : allSourceMap.values()) {
            LOGGER.info("clear datasource of pool  " + this.hostName + " ds:" + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }
    }

    @Override
    public Collection<PhysicalDatasource> getAllDataSources() {
        return new LinkedList<PhysicalDatasource>(allSourceMap.values());
    }

    @Override
    public Map<Integer, PhysicalDatasource[]> getStandbyReadSourcesMap() {
        if (this.getDataHostConfig().getBalance() == BALANCE_NONE) {
            return getReadSources();
        } else {
            return new HashMap<Integer, PhysicalDatasource[]>();
        }
    }

    @Override
    void getRWBalanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {
        PhysicalDatasource theNode = getRWBalanceNode();
        if (theNode.getConfig().isDisabled()) {
            if (this.getReadSources().values().size() > 0) {
                theNode = this.getReadSources().values().iterator().next()[0];
            } else {
                String errorMsg = "the dataHost[" + theNode.getHostConfig().getName() + "] is disabled, please check it";
                throw new IOException(errorMsg);
            }
        }
        if (!theNode.isAlive()) {
            String heartbeatError = "the data source[" + theNode.getConfig().getUrl() + "] can't reached, please check the dataHost";
            LOGGER.warn(heartbeatError);
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", theNode.getHostConfig().getName() + "-" + theNode.getConfig().getHostName());
            AlertUtil.alert(AlarmCode.DATA_HOST_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", theNode.getConfig().getId(), labels);
            throw new IOException(heartbeatError);
        }
        theNode.getConnection(schema, autocommit, handler, attachment, false);
    }

    @Override
    PhysicalDatasource getRWBalanceNode() {
        PhysicalDatasource theNode;
        ArrayList<PhysicalDatasource> okSources;
        switch (balance) {
            case BALANCE_ALL: {
                okSources = getAllActiveRWSources(true, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_ALL_BACK:
            case BALANCE_ALL_READ: {
                okSources = getAllActiveRWSources(false, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_NONE:
            default:
                // return default write data source
                theNode = this.getSource();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read source " + theNode.getName() + " for dataHost:" + this.getHostName());
        }
        theNode.setReadCount();
        return theNode;
    }

    @Override
    PhysicalDatasource getReadNode() throws Exception {
        for (int i = 0; i < allSourceMap.size(); i++) {
            int index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % allSourceMap.size();
            PhysicalDatasource ds = (PhysicalDatasource) allSourceMap.values().toArray()[index];
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

    @Override
    boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws
            Exception {
        LOGGER.debug("!readSources.isEmpty() " + (allSourceMap.values().size() > 1));
        if (allSourceMap.values().size() > 1) {
            PhysicalDatasource theNode = getReadNode();
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


    @Override
    PhysicalDatasource[] getWriteSources() {
        return this.getSources();
    }

    @Override
    public int getActiveIndex() {
        return 0;
    }

    @Override
    public Map<Integer, PhysicalDatasource[]> getReadSources() {
        PhysicalDatasource[] list = new PhysicalDatasource[allSourceMap.size() - 1];
        int i = 0;
        for (PhysicalDatasource ds : allSourceMap.values()) {
            if (ds != writeSource) {
                list[i++] = ds;
            }
        }
        Map<Integer, PhysicalDatasource[]> result = new HashMap<Integer, PhysicalDatasource[]>();
        result.put(0, list);
        return result;
    }


    @Override
    public void switchSourceIfNeed(PhysicalDatasource ds, String reason) {
        throw new RuntimeException("not allowed here");
    }


    @Override
    public int next(int i) {
        return 0;
    }

    private void setDataSourceProps() {
        for (PhysicalDatasource ds : this.allSourceMap.values()) {
            ds.setDbPool(this);
        }
    }


    private void putAllIntoMap(PhysicalDatasource[] source) {
        if (source != null && source.length > 0) {
            for (PhysicalDatasource s : source) {
                allSourceMap.put(s.getName(), s);
            }
        }
    }


    private boolean initSource(PhysicalDatasource ds) {
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


    private ArrayList<PhysicalDatasource> getAllActiveRWSources(boolean includeWriteNode, boolean filterWithSlaveThreshold) {
        ArrayList<PhysicalDatasource> okSources = new ArrayList<>(allSourceMap.values().size());

        for (PhysicalDatasource ds : allSourceMap.values()) {
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


    public void disableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            for (String dsName : nameList) {
                PhysicalDatasource datasource = allSourceMap.get(dsName);
                if (datasource.setDisabled(true)) {
                    //clear old resource
                    datasource.clearCons("ha command disable datasource");
                    datasource.stopHeartbeat();
                }
            }

            HaConfigManager.getInstance().updateConfDataHost(this, syncWriteConf);

        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }


    public void enableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            for (String dsName : nameList) {
                PhysicalDatasource datasource = allSourceMap.get(dsName);
                datasource.setDisabled(false);
            }

            HaConfigManager.getInstance().updateConfDataHost(this, syncWriteConf);
        } finally {
            lock.readLock().lock();
            adjustLock.writeLock().unlock();
        }
    }

    public void switchMaster(String writeHost, boolean syncWriteConf) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            PhysicalDatasource newWriteHost = allSourceMap.get(writeHost);
            writeSource.setReadNode(true);
            //close all old master connection ,so that new write query would not put into the old writeHost
            writeSource.clearCons("ha command switch datasource");
            newWriteHost.setReadNode(false);
            writeSource = newWriteHost;
            HaConfigManager.getInstance().updateConfDataHost(this, syncWriteConf);
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }


    public void changeIntoLastestStatus(String jsonStatus) {
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
                PhysicalDatasource phys = allSourceMap.get(status.getName());
                if (phys != null) {
                    if (phys.setDisabled(status.isDisable()) && status.isDisable()) {
                        //clear old resource
                        phys.clearCons("ha command disable datasource");
                        phys.stopHeartbeat();
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
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    public String getClusterHaJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JSON_NAME, this.getHostName());
        List<DataSourceStatus> list = new ArrayList<>();
        for (PhysicalDatasource phys : allSourceMap.values()) {
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
                for (PhysicalDatasource pds : this.getAllDataSources()) {
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
}
