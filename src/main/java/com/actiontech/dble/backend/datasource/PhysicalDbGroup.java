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
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.backend.mysql.nio.handler.GetConnectionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.cluster.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DbInstanceStatus;
import com.actiontech.dble.config.model.DbGroupConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PhysicalDbGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbGroup.class);
    public static final String JSON_NAME = "dbGroup";
    public static final String JSON_LIST = "dbInstance";

    private volatile PhysicalDbInstance writeSource;
    private Map<String, PhysicalDbInstance> allSourceMap = new HashMap<>();
    public static final int RW_SPLIT_OFF = 0;
    private static final int RW_SPLIT_ALL_SLAVES = 1;
    private static final int RW_SPLIT_ALL = 2;

    public static final int WEIGHT = 0;

    private final String groupName;
    private final int rwSplitMode;
    private final DbGroupConfig dbGroupConfig;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private volatile boolean initSuccess = false;
    protected String[] schemas;

    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();


    public PhysicalDbGroup(String name, DbGroupConfig config, PhysicalDbInstance writeSource, PhysicalDbInstance[] readSources, int rwSplitMode) {
        this.groupName = name;
        this.rwSplitMode = rwSplitMode;
        this.dbGroupConfig = config;
        this.writeSource = writeSource;
        allSourceMap.put(writeSource.getName(), writeSource);

        for (PhysicalDbInstance s : readSources) {
            allSourceMap.put(s.getName(), s);
        }
        setDbInstanceProps();
    }

    public PhysicalDbGroup(PhysicalDbGroup org) {
        this.groupName = org.groupName;
        this.rwSplitMode = org.rwSplitMode;
        this.dbGroupConfig = org.dbGroupConfig;
        allSourceMap = new HashMap<>();
        for (Map.Entry<String, PhysicalDbInstance> entry : org.allSourceMap.entrySet()) {
            MySQLInstance newSource = new MySQLInstance((MySQLInstance) entry.getValue());
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

    public DbGroupConfig getDbGroupConfig() {
        return dbGroupConfig;
    }

    public boolean isAllFakeNode() {
        for (PhysicalDbInstance source : allSourceMap.values()) {
            if (!source.isFakeNode()) {
                return false;
            }
        }
        return true;
    }

    PhysicalDbInstance findDbInstance(BackendConnection exitsCon) {
        MySQLConnection con = (MySQLConnection) exitsCon;
        PhysicalDbInstance source = allSourceMap.get(con.getPool().getName());
        if (source != null && source == con.getPool()) {
            return source;
        }
        LOGGER.info("can't find connection in pool " + this.groupName + " con:" + exitsCon);
        return null;
    }

    boolean isSlave(PhysicalDbInstance ds) {
        return !(writeSource == ds);
    }


    public PhysicalDbInstance getWriteSource() {
        return writeSource;
    }

    public boolean init() {
        if (rwSplitMode != 0) {
            for (Map.Entry<String, PhysicalDbInstance> entry : allSourceMap.entrySet()) {
                if (initSource(entry.getValue())) {
                    initSuccess = true;
                    LOGGER.info(groupName + " " + entry.getKey() + " init success");
                }
            }
        } else {
            if (initSource(writeSource)) {
                initSuccess = true;
                LOGGER.info(groupName + " " + writeSource.getName() + " init success");
            }
        }
        if (!initSuccess) {
            LOGGER.warn(groupName + " init failure");
        }
        return initSuccess;
    }

    public void doHeartbeat() {
        for (PhysicalDbInstance source : allSourceMap.values()) {
            if (source != null) {
                source.doHeartbeat();
            } else {
                LOGGER.warn(groupName + " current dbInstance is null!");
            }
        }
    }

    public void heartbeatCheck(long ildCheckPeriod) {
        for (PhysicalDbInstance ds : allSourceMap.values()) {
            // only read node or all write node
            // and current write node will check
            if (ds != null && (ds.getHeartbeat().getStatus() == MySQLHeartbeat.OK_STATUS) &&
                    (ds.isReadInstance() || ds == this.getWriteSource())) {
                ds.connectionHeatBeatCheck(ildCheckPeriod);
            }
        }
    }

    public void startHeartbeat() {
        for (PhysicalDbInstance source : allSourceMap.values()) {
            source.startHeartbeat();
        }
    }

    public void stopHeartbeat() {
        for (PhysicalDbInstance source : allSourceMap.values()) {
            source.stopHeartbeat();
        }
    }

    public void clearDbInstances(String reason) {
        for (PhysicalDbInstance source : allSourceMap.values()) {
            LOGGER.info("clear dbInstance of pool  " + this.groupName + " ds:" + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }
    }

    public Collection<PhysicalDbInstance> getAllActiveDbInstances() {
        if (this.dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF) {
            return allSourceMap.values();
        } else {
            return Collections.singletonList(writeSource);
        }
    }


    public Collection<PhysicalDbInstance> getAllDbInstances() {
        return new LinkedList<>(allSourceMap.values());
    }


    public Map<String, PhysicalDbInstance> getAllDbInstanceMap() {
        return allSourceMap;
    }

    void getRWSplistCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {
        PhysicalDbInstance theNode = getRWSplistNode();
        if (theNode.isDisabled() || theNode.isFakeNode()) {
            if (this.getAllActiveDbInstances().size() > 0) {
                theNode = this.getAllActiveDbInstances().iterator().next();
            } else {
                if (theNode.isDisabled()) {
                    String errorMsg = "the dbGroup[" + theNode.getDbGroupConfig().getName() + "] is disabled, please check it";
                    throw new IOException(errorMsg);
                } else {
                    String errorMsg = "the dbGroup[" + theNode.getDbGroupConfig().getName() + "] is a fake node, please check it";
                    throw new IOException(errorMsg);
                }
            }
        }
        if (!theNode.isAlive()) {
            String heartbeatError = "dbInstance[" + theNode.getConfig().getUrl() + "] can't reach. Please check the dbInstance status";
            if (dbGroupConfig.isShowSlaveSql()) {
                heartbeatError += ",Tip:heartbeat[show slave status] need the SUPER or REPLICATION CLIENT privilege(s)";
            }
            LOGGER.warn(heartbeatError);
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", theNode.getDbGroupConfig().getName() + "-" + theNode.getConfig().getInstanceName());
            AlertUtil.alert(AlarmCode.DB_INSTANCE_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", theNode.getConfig().getId(), labels);
            throw new IOException(heartbeatError);
        }
        theNode.getConnection(schema, autocommit, handler, attachment, false);
    }

    PhysicalDbInstance getRWSplistNode() {
        PhysicalDbInstance theNode;
        ArrayList<PhysicalDbInstance> okSources;
        switch (rwSplitMode) {
            case RW_SPLIT_ALL: {
                okSources = getAllActiveRWSources(true, checkSlaveSynStatus());
                theNode = randomSelect(okSources, true);
                break;
            }
            case RW_SPLIT_ALL_SLAVES: {
                okSources = getAllActiveRWSources(false, checkSlaveSynStatus());
                theNode = randomSelect(okSources, true);
                break;
            }
            case RW_SPLIT_OFF:
            default:
                // return default primary dbInstance
                theNode = this.getWriteSource();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read dbInstance " + theNode.getName() + " for dbGroup:" + this.getGroupName());
        }
        theNode.setReadCount();
        return theNode;
    }

    PhysicalDbInstance getRandomAliveReadNode() throws Exception {
        if (rwSplitMode == RW_SPLIT_OFF) {
            return null;
        } else {
            return randomSelect(getAllActiveRWSources(false, checkSlaveSynStatus()), false);
        }
    }

    boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws
            Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("!readSources.isEmpty() " + (allSourceMap.values().size() > 1));
        }
        if (allSourceMap.values().size() > 1) {
            PhysicalDbInstance theNode = getRandomAliveReadNode();
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

    PhysicalDbInstance[] getReadSources() {
        PhysicalDbInstance[] readSources = new PhysicalDbInstance[allSourceMap.size() - 1];
        int i = 0;
        for (PhysicalDbInstance source : allSourceMap.values()) {
            if (source.getName().equals(writeSource.getName())) {
                continue;
            }
            readSources[i++] = source;
        }
        return readSources;
    }

    private void setDbInstanceProps() {
        for (PhysicalDbInstance ds : this.allSourceMap.values()) {
            ds.setDbGroup(this);
        }
    }


    private boolean initSource(PhysicalDbInstance ds) {
        if (ds.getConfig().isDisabled() || ds.isFakeNode()) {
            LOGGER.info(ds.getConfig().getInstanceName() + " is disabled or fakeNode, skipped");
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
            LOGGER.warn("maxCon is less than the initSize of dbInstance:" + initSize + " change the maxCon into " + initSize);
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
                LOGGER.info("connection with null schema has been created,because we tested the connection of the dbInstance at first");
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

    private ArrayList<PhysicalDbInstance> getAllActiveRWSources(boolean includeWriteNode, boolean filterWithDelayThreshold) {
        ArrayList<PhysicalDbInstance> okSources = new ArrayList<>(allSourceMap.values().size());
        if (writeSource.isAlive() && includeWriteNode) {
            okSources.add(writeSource);
        }
        for (PhysicalDbInstance ds : allSourceMap.values()) {
            if (ds == writeSource) {
                continue;
            }
            if (ds.isAlive() && (!filterWithDelayThreshold || canSelectAsReadNode(ds))) {
                okSources.add(ds);
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

            HaConfigManager.getInstance().updateDbGroupConf(createDisableSnapshot(this, nameList), syncWriteConf);

            for (String dsName : nameList) {
                PhysicalDbInstance dbInstance = allSourceMap.get(dsName);
                if (dbInstance.setDisabled(true)) {
                    //clear old resource
                    dbInstance.clearCons("ha command disable dbInstance");
                    dbInstance.stopHeartbeat();
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

    private PhysicalDbGroup createDisableSnapshot(PhysicalDbGroup org, String[] nameList) {
        PhysicalDbGroup snapshot = new PhysicalDbGroup(org);
        for (String dsName : nameList) {
            PhysicalDbInstance dbInstance = snapshot.allSourceMap.get(dsName);
            dbInstance.setDisabled(true);
        }
        return snapshot;
    }


    public String enableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {

            HaConfigManager.getInstance().updateDbGroupConf(createEnableSnapshot(this, nameList), syncWriteConf);

            for (String dsName : nameList) {
                PhysicalDbInstance dbInstance = allSourceMap.get(dsName);
                if (dbInstance.setDisabled(false)) {
                    dbInstance.startHeartbeat();
                }
            }
            return this.getClusterHaJson();
        } catch (Exception e) {
            LOGGER.warn("enableHosts Exception ", e);
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    private PhysicalDbGroup createEnableSnapshot(PhysicalDbGroup org, String[] nameList) {
        PhysicalDbGroup snapshot = new PhysicalDbGroup(org);
        for (String dsName : nameList) {
            PhysicalDbInstance dbInstance = snapshot.allSourceMap.get(dsName);
            dbInstance.setDisabled(false);
        }
        return snapshot;
    }

    public String switchMaster(String writeHost, boolean syncWriteConf) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            HaConfigManager.getInstance().updateDbGroupConf(createSwitchSnapshot(writeHost), syncWriteConf);

            PhysicalDbInstance newWriteHost = allSourceMap.get(writeHost);
            writeSource.setReadInstance(true);
            //close all old master connection ,so that new write query would not put into the old writeHost
            writeSource.clearCons("ha command switch dbInstance");
            if (!newWriteHost.isDisabled()) {
                GetAndSyncDbInstanceKeyVariables task = new GetAndSyncDbInstanceKeyVariables(newWriteHost, true);
                KeyVariables variables = task.call();
                if (variables != null) {
                    newWriteHost.setReadOnly(variables.isReadOnly());
                } else {
                    LOGGER.warn(" GetAndSyncDbInstanceKeyVariables failed, set new Primary dbInstance ReadOnly");
                    newWriteHost.setReadOnly(true);
                }
            }
            newWriteHost.setReadInstance(false);
            writeSource = newWriteHost;
            return this.getClusterHaJson();
        } catch (Exception e) {
            LOGGER.warn("switchMaster Exception ", e);
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    private PhysicalDbGroup createSwitchSnapshot(String writeHost) {
        PhysicalDbGroup snapshot = new PhysicalDbGroup(this);
        PhysicalDbInstance newWriteHost = snapshot.allSourceMap.get(writeHost);
        snapshot.writeSource.setReadInstance(true);
        newWriteHost.setReadInstance(false);
        snapshot.writeSource = newWriteHost;
        return snapshot;
    }


    public void changeIntoLatestStatus(String jsonStatus) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            JsonObject jsonObj = new JsonParser().parse(jsonStatus).getAsJsonObject();
            JsonProcessBase base = new JsonProcessBase();
            Type parseType = new TypeToken<List<DbInstanceStatus>>() {
            }.getType();
            List<DbInstanceStatus> list = base.toBeanformJson(jsonObj.get(JSON_LIST).toString(), parseType);
            for (DbInstanceStatus status : list) {
                PhysicalDbInstance phys = allSourceMap.get(status.getName());
                if (phys != null) {
                    if (phys.setDisabled(status.isDisable())) {
                        if (status.isDisable()) {
                            //clear old resource
                            phys.clearCons("ha command disable dbInstance");
                            phys.stopHeartbeat();
                        } else {
                            //change dbInstance from disable to enable ,start heartbeat
                            phys.startHeartbeat();
                        }
                    }
                    if (status.isPrimary() &&
                            phys != writeSource) {
                        writeSource.setReadInstance(true);
                        writeSource.clearCons("ha command switch dbInstance");
                        phys.setReadInstance(false);
                        writeSource = phys;
                    }
                } else {
                    LOGGER.warn("Can match dbInstance " + status.getName() + ".Check for the config file please");
                }
            }
            HaConfigManager.getInstance().updateDbGroupConf(this, false);
        } catch (Exception e) {
            LOGGER.warn("changeIntoLatestStatus Exception ", e);
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
        }
    }

    public String getClusterHaJson() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_NAME, this.getGroupName());
        List<DbInstanceStatus> list = new ArrayList<>();
        for (PhysicalDbInstance phys : allSourceMap.values()) {
            list.add(new DbInstanceStatus(phys.getName(), phys.isDisabled(), !phys.isReadInstance()));
        }
        Gson gson = new Gson();
        jsonObject.add(JSON_LIST, gson.toJsonTree(list));
        return gson.toJson(jsonObject);
    }

    public boolean checkInstanceExist(String instanceName) {
        //add check for subHostName
        if (instanceName != null) {
            for (String dn : instanceName.split(",")) {
                boolean find = false;
                for (PhysicalDbInstance pds : this.getAllDbInstances()) {
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

    public String getGroupName() {
        return groupName;
    }

    public PhysicalDbInstance randomSelect(ArrayList<PhysicalDbInstance> okSources, boolean useWriteWhenEmpty) {
        if (okSources.isEmpty()) {
            if (useWriteWhenEmpty) {
                return this.getWriteSource();
            } else {
                return null;
            }
        } else {
            int length = okSources.size();
            int totalWeight = 0;
            boolean sameWeight = true;
            for (int i = 0; i < length; i++) {
                int readWeight = okSources.get(i).getConfig().getReadWeight();
                totalWeight += readWeight;
                if (sameWeight && i > 0 && readWeight != okSources.get(i - 1).getConfig().getReadWeight()) {
                    sameWeight = false;
                }
            }

            if (totalWeight > 0 && !sameWeight) {
                // random by different weight
                int offset = random.nextInt(totalWeight);
                for (PhysicalDbInstance okSource : okSources) {
                    offset -= okSource.getConfig().getReadWeight();
                    if (offset < 0) {
                        return okSource;
                    }
                }
            }
            return okSources.get(random.nextInt(length));
        }
    }


    private boolean checkSlaveSynStatus() {
        return (dbGroupConfig.getDelayThreshold() != -1) &&
                (dbGroupConfig.isShowSlaveSql());
    }

    private boolean canSelectAsReadNode(PhysicalDbInstance theSource) {
        Integer slaveBehindMaster = theSource.getHeartbeat().getSlaveBehindMaster();
        int dbSynStatus = theSource.getHeartbeat().getDbSynStatus();
        if (slaveBehindMaster == null || dbSynStatus == MySQLHeartbeat.DB_SYN_ERROR) {
            return false;
        }
        boolean isSync = dbSynStatus == MySQLHeartbeat.DB_SYN_NORMAL;
        boolean isNotDelay = slaveBehindMaster < this.dbGroupConfig.getDelayThreshold();
        return isSync && isNotDelay;
    }


    boolean equalsBaseInfo(PhysicalDbGroup pool) {
        return pool.getDbGroupConfig().getName().equals(this.dbGroupConfig.getName()) &&
                pool.getDbGroupConfig().getHearbeatSQL().equals(this.dbGroupConfig.getHearbeatSQL()) &&
                pool.getDbGroupConfig().getHeartbeatTimeout() == this.dbGroupConfig.getHeartbeatTimeout() &&
                pool.getDbGroupConfig().getErrorRetryCount() == this.dbGroupConfig.getErrorRetryCount() &&
                pool.getDbGroupConfig().getRwSplitMode() == this.dbGroupConfig.getRwSplitMode() &&
                pool.getGroupName().equals(this.groupName);
    }
}
