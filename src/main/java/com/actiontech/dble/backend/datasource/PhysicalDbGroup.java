/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.cluster.values.DbInstanceStatus;
import com.actiontech.dble.cluster.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PhysicalDbGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbGroup.class);
    public static final String JSON_NAME = "dbGroup";
    public static final String JSON_LIST = "dbInstance";

    public static final int RW_SPLIT_OFF = 0;
    private static final int RW_SPLIT_ALL_SLAVES = 1;
    private static final int RW_SPLIT_ALL = 2;

    public static final int WEIGHT = 0;

    private final String groupName;
    private final DbGroupConfig dbGroupConfig;
    private volatile PhysicalDbInstance writeDbInstance;
    private Map<String, PhysicalDbInstance> allSourceMap = new HashMap<>();

    private final int rwSplitMode;
    protected String[] schemas;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();

    public PhysicalDbGroup(String name, DbGroupConfig config, PhysicalDbInstance writeDbInstances, PhysicalDbInstance[] readDbInstances, int rwSplitMode) {
        this.groupName = name;
        this.rwSplitMode = rwSplitMode;
        this.dbGroupConfig = config;

        writeDbInstances.setDbGroup(this);
        this.writeDbInstance = writeDbInstances;
        allSourceMap.put(writeDbInstances.getName(), writeDbInstances);

        for (PhysicalDbInstance readDbInstance : readDbInstances) {
            readDbInstance.setDbGroup(this);
            allSourceMap.put(readDbInstance.getName(), readDbInstance);
        }
    }

    public PhysicalDbGroup(PhysicalDbGroup org) {
        this.groupName = org.groupName;
        this.rwSplitMode = org.rwSplitMode;
        this.dbGroupConfig = org.dbGroupConfig;
        allSourceMap = new HashMap<>();
        for (Map.Entry<String, PhysicalDbInstance> entry : org.allSourceMap.entrySet()) {
            MySQLInstance newSource = new MySQLInstance((MySQLInstance) entry.getValue());
            allSourceMap.put(entry.getKey(), newSource);
            if (entry.getValue() == org.writeDbInstance) {
                writeDbInstance = newSource;
            }
        }
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
        PhysicalDbInstance source = (PhysicalDbInstance) exitsCon.getPoolRelated().getInstance();
        PhysicalDbInstance target = allSourceMap.get(source.getName());
        if (source == target) {
            return source;
        }
        LOGGER.info("can't find connection in pool " + this.groupName + " con:" + exitsCon);
        return null;
    }

    boolean isSlave(PhysicalDbInstance ds) {
        return !(writeDbInstance == ds);
    }

    public PhysicalDbInstance getWriteDbInstance() {
        return writeDbInstance;
    }

    public void init(String reason) {
        if (rwSplitMode == 0) {
            writeDbInstance.init(reason);
            return;
        }

        for (Map.Entry<String, PhysicalDbInstance> entry : allSourceMap.entrySet()) {
            entry.getValue().init(reason);
        }
    }

    public void init(String[] sourceNames, String reason, boolean isFresh) {
        if (rwSplitMode == 0) {
            writeDbInstance.init(reason, isFresh);
            return;
        }

        for (String sourceName : sourceNames) {
            if (allSourceMap.containsKey(sourceName)) {
                allSourceMap.get(sourceName).init(reason, isFresh);
            }
        }
    }

    public void stop(String reason) {
        stop(reason, false);
    }

    public void stop(String reason, boolean closeFront) {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            dbInstance.stop(reason, closeFront);
        }
    }

    public void stop(String[] sourceNames, String reason, boolean closeFront) {
        for (String sourceName : sourceNames) {
            if (allSourceMap.containsKey(sourceName)) {
                allSourceMap.get(sourceName).stop(reason, closeFront);
            }
        }

        if (closeFront) {
            Iterator<PooledConnection> iterator = IOProcessor.BACKENDS_OLD.iterator();
            while (iterator.hasNext()) {
                PooledConnection con = iterator.next();
                if (con instanceof BackendConnection) {
                    BackendConnection backendCon = (BackendConnection) con;
                    if (backendCon.getPoolDestroyedTime() != 0 && Arrays.asList(sourceNames).contains(backendCon.getInstance().getConfig().getInstanceName())) {
                        backendCon.closeWithFront("old active backend conn will be forced closed by closing front conn");
                        iterator.remove();
                    }
                }
            }
        }
    }

    public Collection<PhysicalDbInstance> getAllActiveDbInstances() {
        if (this.dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF) {
            return allSourceMap.values();
        } else {
            return Collections.singletonList(writeDbInstance);
        }
    }

    public Collection<PhysicalDbInstance> getAllDbInstances() {
        return new LinkedList<>(allSourceMap.values());
    }

    public Map<String, PhysicalDbInstance> getAllDbInstanceMap() {
        return allSourceMap;
    }

    void getRWSplitCon(String schema, ResponseHandler handler, Object attachment) throws Exception {
        PhysicalDbInstance theNode = getRWSplitNode();
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

        theNode.getConnection(schema, handler, attachment, false);
    }

    PhysicalDbInstance getRWSplitNode() {
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
                theNode = this.writeDbInstance;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read dbInstance " + theNode.getName() + " for dbGroup:" + this.getGroupName());
        }
        theNode.incrementReadCount();
        return theNode;
    }

    PhysicalDbInstance getRandomAliveReadNode() {
        if (rwSplitMode == RW_SPLIT_OFF) {
            return null;
        } else {
            return randomSelect(getAllActiveRWSources(false, checkSlaveSynStatus()), false);
        }
    }

    boolean getReadCon(String schema, ResponseHandler handler, Object attachment) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("!readSources.isEmpty() " + (allSourceMap.values().size() > 1));
        }
        if (allSourceMap.values().size() > 1) {
            PhysicalDbInstance theNode = getRandomAliveReadNode();
            if (theNode != null) {
                theNode.incrementReadCount();
                theNode.getConnection(schema, handler, attachment, false);
                return true;
            } else {
                LOGGER.info("read host is not available.");
                return false;
            }
        } else {
            LOGGER.info("read host is empty, read dbInstance is empty.");
            return false;
        }
    }

    PhysicalDbInstance[] getReadSources() {
        PhysicalDbInstance[] readSources = new PhysicalDbInstance[allSourceMap.size() - 1];
        int i = 0;
        for (PhysicalDbInstance source : allSourceMap.values()) {
            if (source.getName().equals(writeDbInstance.getName())) {
                continue;
            }
            readSources[i++] = source;
        }
        return readSources;
    }

    private ArrayList<PhysicalDbInstance> getAllActiveRWSources(boolean includeWriteNode, boolean filterWithDelayThreshold) {
        ArrayList<PhysicalDbInstance> okSources = new ArrayList<>(allSourceMap.values().size());
        if (writeDbInstance.isAlive() && includeWriteNode) {
            okSources.add(writeDbInstance);
        }
        for (PhysicalDbInstance ds : allSourceMap.values()) {
            if (ds == writeDbInstance) {
                continue;
            }
            if (ds.isAlive() && (!filterWithDelayThreshold || ds.canSelectAsReadNode())) {
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
                allSourceMap.get(dsName).disable("ha command disable dbInstance");
            }
            return this.getClusterHaJson();
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
                allSourceMap.get(dsName).enable();
            }
            return this.getClusterHaJson();
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
            writeDbInstance.setReadInstance(true);
            //close all old master connection ,so that new writeDirectly query would not put into the old writeHost
            writeDbInstance.closeAllConnection("ha command switch dbInstance");
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
            writeDbInstance = newWriteHost;
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
        snapshot.writeDbInstance.setReadInstance(true);
        newWriteHost.setReadInstance(false);
        snapshot.writeDbInstance = newWriteHost;
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
                PhysicalDbInstance dbInstance = allSourceMap.get(status.getName());
                if (dbInstance != null) {
                    if (status.isDisable()) {
                        //clear old resource
                        dbInstance.disable("ha command disable dbInstance");
                    } else {
                        //change dbInstance from disable to enable ,start heartbeat
                        dbInstance.enable();
                    }
                    if (status.isPrimary() && dbInstance != writeDbInstance) {
                        writeDbInstance.setReadInstance(true);
                        writeDbInstance.closeAllConnection("ha command switch dbInstance");
                        dbInstance.setReadInstance(false);
                        writeDbInstance = dbInstance;
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
                return writeDbInstance;
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

    boolean equalsBaseInfo(PhysicalDbGroup pool) {
        return pool.getDbGroupConfig().getName().equals(this.dbGroupConfig.getName()) &&
                pool.getDbGroupConfig().getHeartbeatSQL().equals(this.dbGroupConfig.getHeartbeatSQL()) &&
                pool.getDbGroupConfig().getHeartbeatTimeout() == this.dbGroupConfig.getHeartbeatTimeout() &&
                pool.getDbGroupConfig().getErrorRetryCount() == this.dbGroupConfig.getErrorRetryCount() &&
                pool.getDbGroupConfig().getRwSplitMode() == this.dbGroupConfig.getRwSplitMode() &&
                pool.getGroupName().equals(this.groupName);
    }
}
