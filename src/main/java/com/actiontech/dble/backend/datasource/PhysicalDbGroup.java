/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.delyDetection.DelayDetection;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.cluster.JsonFactory;
import com.actiontech.dble.cluster.values.DbInstanceStatus;
import com.actiontech.dble.cluster.values.JsonObjectWriter;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.parse.JsonProcessBase;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PhysicalDbGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDbGroup.class);
    public static final String JSON_NAME = "dbGroup";
    public static final String JSON_LIST = "dbInstance";
    // rw split
    public static final int RW_SPLIT_OFF = 0;
    public static final int RW_SPLIT_ALL = 2;
    public static final int RW_SPLIT_ALL_SLAVES_MAY_MASTER = 3;
    private List<PhysicalDbInstance> writeInstanceList;

    private String groupName;
    private DbGroupConfig dbGroupConfig;
    private volatile PhysicalDbInstance writeDbInstance;
    private Map<String, PhysicalDbInstance> allSourceMap = new HashMap<>();

    private int rwSplitMode;
    protected List<String> schemas = Lists.newArrayList();
    private final LoadBalancer loadBalancer = new RandomLoadBalancer();
    private final LocalReadLoadBalancer localReadLoadBalancer = new LocalReadLoadBalancer();
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();

    private boolean shardingUseless = true;
    private boolean rwSplitUseless = true;
    private boolean analysisUseless = true;
    private boolean hybridTAUseless = true;
    private Set<Session> rwSplitSessionSet = Sets.newConcurrentHashSet();
    private volatile Integer state = Integer.valueOf(INITIAL);

    //delayDetection
    private AtomicLong logicTimestamp = new AtomicLong();


    public static final int STATE_DELETING = 2;
    public static final int STATE_ABANDONED = 1;
    public static final int INITIAL = 0;

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

    public List<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<String> mySchemas) {
        this.schemas = mySchemas;
    }

    public void addSchema(String schema) {
        this.schemas.add(schema);
    }

    public void removeSchema(String schema) {
        this.schemas.remove(schema);
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
        return shardingUseless && rwSplitUseless && analysisUseless && hybridTAUseless;
    }

    public boolean isShardingUseless() {
        return shardingUseless;
    }

    public void setShardingUseless(boolean shardingUseless) {
        this.shardingUseless = shardingUseless;
    }

    public boolean isRwSplitUseless() {
        return rwSplitUseless;
    }

    public void setRwSplitUseless(boolean rwSplitUseless) {
        this.rwSplitUseless = rwSplitUseless;
    }

    public boolean isAnalysisUseless() {
        return analysisUseless;
    }

    public void setAnalysisUseless(boolean analysisUseless) {
        this.analysisUseless = analysisUseless;
    }

    public boolean isHybridTAUseless() {
        return hybridTAUseless;
    }

    public void setHybridTAUseless(boolean hybridTAUseless) {
        this.hybridTAUseless = hybridTAUseless;
    }

    private boolean checkSlaveSynStatus(PhysicalDbInstance ds) {
        return (dbGroupConfig.getDelayThreshold() != -1 &&
                dbGroupConfig.isShowSlaveSql()) || ds.getDbGroup().isDelayDetectionStart();
    }

    public PhysicalDbInstance getWriteDbInstance() {
        return writeDbInstance;
    }

    public void init(String reason) {
        if (LOGGER.isDebugEnabled()) {
            ReloadLogHelper.debug("init new group :{},reason:{}", this.toString(), reason);
        }
        for (Map.Entry<String, PhysicalDbInstance> entry : allSourceMap.entrySet()) {
            entry.getValue().init(reason, true, true);
        }
    }

    public void startOfFresh(List<String> sourceNames, String reason) {
        for (String sourceName : sourceNames) {
            if (allSourceMap.containsKey(sourceName)) {
                allSourceMap.get(sourceName).init(reason, false, false);
            }
        }
    }

    private boolean checkState() {
        if (isStop()) {
            return false;
        }
        if (getBindingCount() != 0) {
            state = STATE_DELETING;
            IOProcessor.BACKENDS_OLD_GROUP.add(this);
            return false;
        }
        state = STATE_ABANDONED;
        return true;
    }

    public void stop(String reason) {
        stop(reason, false);
    }

    public void stop(String reason, boolean closeFront) {
        if (LOGGER.isDebugEnabled()) {
            ReloadLogHelper.debug("recycle old group :{},reason:{},is close front:{}", this.toString(), reason, closeFront);
        }
        boolean flag = checkState();
        if (!flag) {
            return;
        }
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            dbInstance.stopDirectly(reason, closeFront, false);
        }
    }

    public void stopOfFresh(List<String> sourceNames, String reason, boolean closeFront) {
        for (String sourceName : sourceNames) {
            if (allSourceMap.containsKey(sourceName)) {
                PhysicalDbInstance dbInstance = allSourceMap.get(sourceName);
                dbInstance.stop(reason, closeFront, false, dbGroupConfig.getRwSplitMode() != RW_SPLIT_OFF || writeDbInstance == dbInstance, false);
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


    public boolean stopOfBackground(String reason) {
        if (state.intValue() == STATE_DELETING && getBindingCount() == 0) {
            for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
                dbInstance.stopDirectly(reason, false, false);
            }
            return true;
        }
        return false;
    }


    public boolean isStop() {
        return state.intValue() != INITIAL;
    }

    public void stopPool(String reason, boolean closeFront, boolean closeWrite) {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            if (!closeWrite && writeDbInstance == dbInstance) {
                continue;
            }
            dbInstance.stopPool(reason, closeFront);
        }
    }

    public void startPool(String reason, boolean startWrite) {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            if (!startWrite && writeDbInstance == dbInstance) {
                continue;
            }
            dbInstance.startPool(reason);
        }
    }

    public void stopHeartbeat(String reason) {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            dbInstance.stopHeartbeat(reason);
        }
    }

    public void startHeartbeat() {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            if (dbInstance.heartbeat.isStop()) {
                dbInstance.heartbeat = new MySQLHeartbeat(dbInstance);
            }
            dbInstance.startHeartbeat();
        }
    }

    public void startDelayDetection() {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            if (dbInstance.delayDetection != null && dbInstance.delayDetection.isStop()) {
                dbInstance.delayDetection = new DelayDetection(dbInstance);
            }
            dbInstance.startDelayDetection();
        }
    }

    public void stopDelayDetection(String reason) {
        for (PhysicalDbInstance dbInstance : allSourceMap.values()) {
            dbInstance.stopDelayDetection(reason);
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

    /**
     * rwsplit user
     *
     * @param master
     * @param writeStatistical
     * @return
     * @throws IOException
     */
    public PhysicalDbInstance rwSelect(Boolean master, Boolean writeStatistical) throws IOException {
        return rwSelect(master, writeStatistical, false);
    }

    /**
     * rwsplit user
     *
     * @param master
     * @param writeStatistical
     * @param localRead        only the SELECT and show statements attempt to localRead
     * @return
     * @throws IOException
     */
    public PhysicalDbInstance rwSelect(Boolean master, Boolean writeStatistical, boolean localRead) throws IOException {
        if (Objects.nonNull(writeStatistical)) {
            return select(master, false, writeStatistical, localRead);
        }
        return select(master, false, Objects.nonNull(master) ? master : false, localRead);
    }

    /**
     * Sharding user
     *
     * @param master
     * @param isForUpdate
     * @return
     * @throws IOException
     */
    public PhysicalDbInstance select(Boolean master, boolean isForUpdate, boolean localRead) throws IOException {
        if (Objects.nonNull(master)) {
            return select(master, isForUpdate, master, localRead);
        }
        return select(master, isForUpdate, false, localRead);
    }

    /**
     * Select an instance
     *
     * @param master
     * @param isForUpdate
     * @param writeStatistical
     * @param localRead        only the SELECT and show statements attempt to localRead
     * @return
     * @throws IOException
     */
    public PhysicalDbInstance select(Boolean master, boolean isForUpdate, boolean writeStatistical, boolean localRead) throws IOException {

        if (rwSplitMode == RW_SPLIT_OFF && (master != null && !master)) {
            LOGGER.warn("force slave,but the dbGroup[{}] doesn't contains active slave dbInstance", groupName);
            throw new IOException("force slave,but the dbGroup[" + groupName + "] doesn't contain active slave dbInstance");
        }

        if (rwSplitMode == RW_SPLIT_OFF || allSourceMap.size() == 1 || (master != null && master) || isForUpdate) {
            if (writeDbInstance.isAlive()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("select write {}", writeDbInstance);
                }
                if (writeStatistical) {
                    writeDbInstance.incrementWriteCount();
                } else {
                    writeDbInstance.incrementReadCount();
                }
                return writeDbInstance;
            } else {
                reportError(writeDbInstance);
            }
        }

        List<PhysicalDbInstance> instances = getRWDbInstances(master == null);
        if (instances.size() == 0) {
            throw new IOException("the dbGroup[" + groupName + "] doesn't contain active dbInstance.");
        }

        if (localRead) {
            return getPhysicalDbInstance(instances, localReadLoadBalancer);
        }
        return getPhysicalDbInstance(instances, loadBalancer);
    }

    @NotNull
    private PhysicalDbInstance getPhysicalDbInstance(List<PhysicalDbInstance> instances, LoadBalancer pLoadBalancer) throws IOException {
        PhysicalDbInstance selectInstance = pLoadBalancer.select(instances);
        selectInstance.incrementReadCount();
        if (selectInstance.isAlive()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("select {}", selectInstance);
            }
        } else {
            reportError(selectInstance);
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

            if (ds.isAlive() && (!checkSlaveSynStatus(ds) || ds.canSelectAsReadNode())) {
                if (ds.getLogCount() != 0) {
                    ds.setLogCount(0);
                }
                okSources.add(ds);
            } else {
                if (ds.isAlive()) {
                    if (ds.getLogCount() != 0) {
                        ds.setLogCount(0);
                    }
                    LOGGER.warn("can't select dbInstance[{}] as read node, please check delay with primary", ds);
                } else {
                    if (ds.getLogCount() < 10) {
                        ds.setLogCount(ds.getLogCount() + 1);
                        LOGGER.warn("can't select dbInstance[{}] as read node, please check the disabled and heartbeat status", ds);
                    }
                }
            }
        }
        if (okSources.size() == 0 && rwSplitMode == RW_SPLIT_ALL_SLAVES_MAY_MASTER && includeWrite) {
            if (writeDbInstance.isAlive()) {
                okSources.add(writeDbInstance);
            } else {
                LOGGER.warn("can't select dbInstance[{}] as read node, please check delay with primary", writeDbInstance);
            }
        }
        return okSources;
    }

    public RawJson disableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        HaConfigManager.getInstance().info("added dbGroupLock");
        try {
            HaConfigManager.getInstance().updateDbGroupConf(createDisableSnapshot(this, nameList), syncWriteConf);
            for (String dsName : nameList) {
                allSourceMap.get(dsName).disable("ha command disable dbInstance");
            }
            return this.getClusterHaJson();
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
            HaConfigManager.getInstance().info("released dbGroupLock");
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

    public RawJson enableHosts(String hostNames, boolean syncWriteConf) {
        String[] nameList = hostNames == null ? Arrays.copyOf(allSourceMap.keySet().toArray(), allSourceMap.keySet().toArray().length, String[].class) : hostNames.split(",");
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        HaConfigManager.getInstance().info("added dbGroupLock");
        try {

            HaConfigManager.getInstance().updateDbGroupConf(createEnableSnapshot(this, nameList), syncWriteConf);

            for (String dsName : nameList) {
                allSourceMap.get(dsName).enable();
            }
            return this.getClusterHaJson();
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
            HaConfigManager.getInstance().info("released dbGroupLock");
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

    public RawJson switchMaster(String writeHost, boolean syncWriteConf) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        HaConfigManager.getInstance().info("added dbGroupLock");
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
                    HaConfigManager.getInstance().warn("GetAndSyncDbInstanceKeyVariables failed, set new Primary dbInstance ReadOnly");
                    newWriteHost.setReadOnly(true);
                }
            }
            newWriteHost.setReadInstance(false);
            String oldWriteInstance = writeDbInstance.getName();
            writeDbInstance = newWriteHost;
            newWriteHost.start("switch master from " + oldWriteInstance + " to the instance", false, false);
            return this.getClusterHaJson();
        } catch (Exception e) {
            HaConfigManager.getInstance().warn("switchMaster Exception ", e);
            throw e;
        } finally {
            lock.readLock().unlock();
            adjustLock.writeLock().unlock();
            HaConfigManager.getInstance().info("released dbGroupLock");
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


    public void changeIntoLatestStatus(RawJson jsonStatus) {
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        adjustLock.writeLock().lock();
        try {
            JsonObject jsonObj = jsonStatus.getJsonObject();
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

    public RawJson getClusterHaJson() {
        JsonObjectWriter jsonObject = new JsonObjectWriter();
        jsonObject.addProperty(JSON_NAME, this.getGroupName());
        List<DbInstanceStatus> list = new ArrayList<>();
        for (PhysicalDbInstance phys : allSourceMap.values()) {
            list.add(new DbInstanceStatus(phys.getName(), phys.isDisabled(), !phys.isReadInstance()));
        }
        Gson gson = JsonFactory.getJson();
        jsonObject.add(JSON_LIST, gson.toJsonTree(list));
        return RawJson.of(jsonObject);
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

    private void reportDisableError(PhysicalDbInstance ins) throws IOException {
        final DbInstanceConfig config = ins.getConfig();
        String disableError = "the dbInstance[" + config.getUrl() + "] is disable. Please check the dbInstance disable status";
        LOGGER.warn(disableError);
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", dbGroupConfig.getName() + "-" + config.getInstanceName());
        AlertUtil.alert(AlarmCode.DB_INSTANCE_CAN_NOT_REACH, Alert.AlertLevel.WARN, disableError, "mysql", config.getId(), labels);
        throw new IOException(disableError);
    }

    private void reportFakeNodeError(PhysicalDbInstance ins) throws IOException {
        final DbInstanceConfig config = ins.getConfig();
        String fakeNodeError = "the dbInstance[" + config.getUrl() + "] is fake node. Please check the dbInstance whether or not it is used";
        LOGGER.warn(fakeNodeError);
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", dbGroupConfig.getName() + "-" + config.getInstanceName());
        AlertUtil.alert(AlarmCode.DB_INSTANCE_CAN_NOT_REACH, Alert.AlertLevel.WARN, fakeNodeError, "mysql", config.getId(), labels);
        throw new IOException(fakeNodeError);
    }

    private void reportHeartbeatError(PhysicalDbInstance ins) throws IOException {
        final DbInstanceConfig config = ins.getConfig();
        String heartbeatError = "the dbInstance[" + config.getUrl() + "] can't reach. Please check the dbInstance is accessible";
        if (dbGroupConfig.isShowSlaveSql()) {
            heartbeatError += " and the privileges of user is sufficient (NOTE:heartbeat[show slave status] need grant the SUPER or REPLICATION CLIENT privilege(s) to db user,and then restart the dble or fresh conn).";
        }
        LOGGER.warn(heartbeatError);
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", dbGroupConfig.getName() + "-" + config.getInstanceName());
        AlertUtil.alert(AlarmCode.DB_INSTANCE_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", config.getId(), labels);
        throw new IOException(heartbeatError);
    }

    private void reportError(PhysicalDbInstance dbInstance) throws IOException {
        if (dbInstance.isFakeNode()) {
            reportFakeNodeError(dbInstance);
        } else if (dbInstance.isDisabled()) {
            reportDisableError(dbInstance);
        } else {
            reportHeartbeatError(dbInstance);
        }
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public Integer getState() {
        return state;
    }

    public int getBindingCount() {
        return rwSplitSessionSet.size();
    }

    public boolean bindRwSplitSession(RWSplitNonBlockingSession session) {
        return this.rwSplitSessionSet.add(session);
    }

    public boolean unBindRwSplitSession(RWSplitNonBlockingSession session) {
        return this.rwSplitSessionSet.remove(session);
    }

    public void setWriteDbInstance(PhysicalDbInstance writeDbInstance) {
        this.writeDbInstance = writeDbInstance;
        if (writeDbInstance == null) {
            this.writeInstanceList = Lists.newArrayList();
        } else {
            this.writeInstanceList = Collections.singletonList(writeDbInstance);
        }
    }

    public void setDbInstance(PhysicalDbInstance dbInstance) {
        dbInstance.setDbGroup(this);
        if (dbInstance.getConfig().isPrimary()) {
            setWriteDbInstance(dbInstance);
            this.dbGroupConfig.setWriteInstanceConfig(dbInstance.getConfig());
        } else {
            this.dbGroupConfig.addReadInstance(dbInstance.getConfig());
        }
        this.allSourceMap.put(dbInstance.getName(), dbInstance);
    }

    public AtomicLong getLogicTimestamp() {
        return logicTimestamp;
    }

    public void setLogicTimestamp(AtomicLong logicTimestamp) {
        this.logicTimestamp = logicTimestamp;
    }

    public boolean isDelayDetectionStart() {
        return !Strings.isNullOrEmpty(dbGroupConfig.getDelayDatabase()) &&
                dbGroupConfig.getDelayThreshold() > 0 && dbGroupConfig.getDelayPeriodMillis() > 0 && getDbGroupConfig().getWriteInstanceConfig().getDataBaseType() == DataBaseType.MYSQL;
    }

    public boolean equalsBaseInfo(PhysicalDbGroup pool) {
        return pool.dbGroupConfig.equalsBaseInfo(this.dbGroupConfig) &&
                pool.rwSplitMode == this.rwSplitMode &&
                pool.getGroupName().equals(this.groupName) &&
                pool.isUseless() == this.isUseless();
    }

    public boolean equalsForConnectionPool(PhysicalDbGroup pool) {
        boolean rwSplitModeFlag1 = pool.getDbGroupConfig().getRwSplitMode() != 0 && this.dbGroupConfig.getRwSplitMode() != 0;
        boolean rwSplitModeFlag2 = pool.getDbGroupConfig().getRwSplitMode() == 0 && this.dbGroupConfig.getRwSplitMode() == 0;
        return (rwSplitModeFlag1 || rwSplitModeFlag2) && pool.isUseless() == this.isUseless();
    }

    public boolean equalsForHeartbeat(PhysicalDbGroup pool) {
        return pool.getDbGroupConfig().getHeartbeatSQL().equals(this.dbGroupConfig.getHeartbeatSQL()) &&
                pool.getDbGroupConfig().getHeartbeatTimeout() == this.dbGroupConfig.getHeartbeatTimeout() &&
                pool.getDbGroupConfig().getErrorRetryCount() == this.dbGroupConfig.getErrorRetryCount() &&
                pool.getDbGroupConfig().getKeepAlive() == this.getDbGroupConfig().getKeepAlive();
    }

    public boolean equalsForDelayDetection(PhysicalDbGroup pool) {
        return pool.getDbGroupConfig().getDelayThreshold() == (this.dbGroupConfig.getDelayThreshold()) &&
                pool.getDbGroupConfig().getDelayPeriodMillis() == this.dbGroupConfig.getDelayPeriodMillis() &&
                StringUtil.equals(pool.getDbGroupConfig().getDelayDatabase(), this.dbGroupConfig.getDelayDatabase());
    }


    public void copyBaseInfo(PhysicalDbGroup physicalDbGroup) {
        this.dbGroupConfig = physicalDbGroup.dbGroupConfig;
        this.groupName = physicalDbGroup.groupName;
        this.rwSplitMode = physicalDbGroup.rwSplitMode;
        this.schemas = physicalDbGroup.schemas;
        this.rwSplitUseless = physicalDbGroup.rwSplitUseless;
        this.shardingUseless = physicalDbGroup.shardingUseless;
        for (PhysicalDbInstance dbInstance : this.allSourceMap.values()) {
            dbInstance.setDbGroupConfig(physicalDbGroup.dbGroupConfig);
        }
    }

    @Override
    public String toString() {
        return "PhysicalDbGroup{" +
                "groupName='" + groupName + '\'' +
                ", dbGroupConfig=" + dbGroupConfig +
                ", writeDbInstance=" + writeDbInstance +
                ", allSourceMap=" + allSourceMap +
                ", rwSplitMode=" + rwSplitMode +
                ", schemas=" + schemas +
                ", shardingUseless=" + shardingUseless +
                ", rwSplitUseless=" + rwSplitUseless +
                '}';
    }


}
