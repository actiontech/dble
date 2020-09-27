/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PhysicalDbGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbGroup.class);
    public static final String JSON_NAME = "dbGroup";
    public static final String JSON_LIST = "dbInstance";
    // rw split
    public static final int RW_SPLIT_OFF = 0;
    public static final int RW_SPLIT_ALL_SLAVES = 1;
    public static final int RW_SPLIT_ALL = 2;
    // weight
    public static final int WEIGHT = 0;
    private final List<PhysicalDbInstance> writeInstanceList;

    private final String groupName;
    private final DbGroupConfig dbGroupConfig;
    private volatile PhysicalDbInstance writeDbInstance;
    private Map<String, PhysicalDbInstance> allSourceMap = new HashMap<>();

    private final int rwSplitMode;
    protected String[] schemas;
    private final LoadBalancer loadBalancer = new RandomLoadBalancer();
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();

    private boolean useless = false;

    public PhysicalDbGroup(String name, DbGroupConfig config, PhysicalDbInstance writeDbInstances, PhysicalDbInstance[] readDbInstances, int rwSplitMode) {
        this.groupName = name;
        this.rwSplitMode = rwSplitMode;
        this.dbGroupConfig = config;

        writeDbInstances.setDbGroup(this);
        this.writeDbInstance = writeDbInstances;
        this.writeInstanceList = Collections.singletonList(writeDbInstance);
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
        this.allSourceMap = new HashMap<>();
        for (Map.Entry<String, PhysicalDbInstance> entry : org.allSourceMap.entrySet()) {
            MySQLInstance newSource = new MySQLInstance((MySQLInstance) entry.getValue());
            this.allSourceMap.put(entry.getKey(), newSource);
            if (entry.getValue() == org.writeDbInstance) {
                this.writeDbInstance = newSource;
            }
        }
        writeInstanceList = Collections.singletonList(writeDbInstance);
    }

    public String getGroupName() {
        return groupName;
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

    public int getRwSplitMode() {
        return rwSplitMode;
    }

    public boolean isUseless() {
        return useless;
    }

    public void setUseless(boolean useless) {
        this.useless = useless;
    }

    private boolean checkSlaveSynStatus() {
        return (dbGroupConfig.getDelayThreshold() != -1) &&
                (dbGroupConfig.isShowSlaveSql());
    }

    public PhysicalDbInstance getWriteDbInstance() {
        return writeDbInstance;
    }

    public void init(String reason) {
        for (Map.Entry<String, PhysicalDbInstance> entry : allSourceMap.entrySet()) {
            entry.getValue().init(reason);
        }
    }

    public void init(List<String> sourceNames, String reason) {
        for (String sourceName : sourceNames) {
            if (allSourceMap.containsKey(sourceName)) {
                allSourceMap.get(sourceName).init(reason, false);
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

    public void stop(List<String> sourceNames, String reason, boolean closeFront) {
        for (String sourceName : sourceNames) {
            if (allSourceMap.containsKey(sourceName)) {
                allSourceMap.get(sourceName).stop(reason, closeFront, false);
            }
        }

        if (closeFront) {
            Iterator<PooledConnection> iterator = IOProcessor.BACKENDS_OLD.iterator();
            while (iterator.hasNext()) {
                PooledConnection con = iterator.next();
                if (con instanceof BackendConnection) {
                    BackendConnection backendCon = (BackendConnection) con;
                    if (backendCon.getPoolDestroyedTime() != 0 && sourceNames.contains(backendCon.getInstance().getConfig().getInstanceName())) {
                        backendCon.closeWithFront("old active backend conn will be forced closed by closing front conn");
                        iterator.remove();
                    }
                }
            }
        }
    }

    public Collection<PhysicalDbInstance> getDbInstances(boolean isAll) {
        if (!isAll && rwSplitMode == RW_SPLIT_OFF) {
            return writeInstanceList;
        }
        return allSourceMap.values();
    }

    public Map<String, PhysicalDbInstance> getAllDbInstanceMap() {
        return allSourceMap;
    }

    public PhysicalDbInstance[] getReadDbInstances() {
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

    public PhysicalDbInstance select(Boolean master) throws IOException {
        if (rwSplitMode == RW_SPLIT_OFF && (master != null && !master)) {
            LOGGER.warn("force slave,but the dbGroup[{}] doesn't contains active slave dbInstance", groupName);
            throw new IOException("force slave,but the dbGroup[" + groupName + "] doesn't contain active slave dbInstance");
        }

        if (rwSplitMode == RW_SPLIT_OFF || allSourceMap.size() == 1 || (master != null && master)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select write {}", writeDbInstance);
            }
            return writeDbInstance;
        }

        List<PhysicalDbInstance> instances = getRWDbInstances(master == null);
        if (instances.size() == 0) {
            throw new IOException("the dbGroup[" + groupName + "] doesn't contain active dbInstance.");
        }
        PhysicalDbInstance selectInstance = loadBalancer.select(instances);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select {}", selectInstance);
        }
        return selectInstance;
    }

    private List<PhysicalDbInstance> getRWDbInstances(boolean includeWrite) {
        ArrayList<PhysicalDbInstance> okSources = new ArrayList<>(allSourceMap.values().size());
        for (PhysicalDbInstance ds : allSourceMap.values()) {
            if (ds == writeDbInstance) {
                if (includeWrite && rwSplitMode == RW_SPLIT_ALL && writeDbInstance.isAlive()) {
                    okSources.add(ds);
                }
                continue;
            }

            if (ds.isAlive() && (!checkSlaveSynStatus() || ds.canSelectAsReadNode())) {
                okSources.add(ds);
            } else {
                LOGGER.warn("can't select dbInstance[{}] as read node", ds);
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
                for (PhysicalDbInstance pds : this.allSourceMap.values()) {
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

    boolean equalsBaseInfo(PhysicalDbGroup pool) {
        return pool.getDbGroupConfig().getName().equals(this.dbGroupConfig.getName()) &&
                pool.getDbGroupConfig().getHeartbeatSQL().equals(this.dbGroupConfig.getHeartbeatSQL()) &&
                pool.getDbGroupConfig().getHeartbeatTimeout() == this.dbGroupConfig.getHeartbeatTimeout() &&
                pool.getDbGroupConfig().getErrorRetryCount() == this.dbGroupConfig.getErrorRetryCount() &&
                pool.getDbGroupConfig().getRwSplitMode() == this.dbGroupConfig.getRwSplitMode() &&
                pool.getDbGroupConfig().getDelayThreshold() == this.dbGroupConfig.getDelayThreshold() &&
                pool.getDbGroupConfig().isDisableHA() == this.dbGroupConfig.isDisableHA() &&
                pool.getGroupName().equals(this.groupName) && pool.isUseless() == this.isUseless();
    }
}
