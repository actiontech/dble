/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.*;
import com.actiontech.dble.cluster.JsonFactory;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.sharding.table.ShardingTableFakeConfig;
import com.actiontech.dble.config.model.user.HybridTAUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.ManagerParseConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.services.manager.response.ChangeItem;
import com.actiontech.dble.services.manager.response.ChangeItemType;
import com.actiontech.dble.services.manager.response.ChangeType;
import com.actiontech.dble.singleton.*;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @author mycat
 */
public class ServerConfig {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);

    private volatile Map<UserName, UserConfig> users;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, ShardingNode> shardingNodes;
    private volatile Map<String, ApNode> apNodes;
    private volatile Map<String, PhysicalDbGroup> dbGroups;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile Map<String, Set<ERTable>> funcNodeERMap;
    private volatile boolean fullyConfigured;
    private volatile long reloadTime;
    private volatile boolean changing = false;
    private final ReentrantReadWriteLock lock;
    private ConfigInitializer confInitNew;
    private volatile Map<String, Properties> blacklistConfig;
    private volatile Map<String, AbstractPartitionAlgorithm> functions;
    private RawJson dbConfig;
    private RawJson shardingConfig;
    private RawJson userConfig;
    private RawJson sequenceConfig;
    private Boolean lowerCase;

    public ServerConfig() {
        //read sharding.xml,db.xml and user.xml
        confInitNew = new ConfigInitializer();
        this.users = confInitNew.getUsers();
        this.dbGroups = confInitNew.getDbGroups();

        this.schemas = confInitNew.getSchemas();
        this.shardingNodes = confInitNew.getShardingNodes();
        this.apNodes = confInitNew.getApNodes();
        this.erRelations = confInitNew.getErRelations();
        this.funcNodeERMap = confInitNew.getFuncNodeERMap();
        this.functions = confInitNew.getFunctions();
        this.fullyConfigured = confInitNew.isFullyConfigured();
        ConfigUtil.setSchemasForPool(dbGroups, shardingNodes);

        this.reloadTime = TimeUtil.currentTimeMillis();

        this.lock = new ReentrantReadWriteLock();
        this.blacklistConfig = confInitNew.getBlacklistConfig();
        this.userConfig = confInitNew.getUserConfig();
        this.dbConfig = confInitNew.getDbConfig();
        this.shardingConfig = confInitNew.getShardingConfig();
        this.sequenceConfig = confInitNew.getSequenceConfig();
    }


    public ServerConfig(ConfigInitializer confInit) {
        //read sharding.xml,db.xml and user.xml
        this.users = confInit.getUsers();
        this.dbGroups = confInit.getDbGroups();
        this.schemas = confInit.getSchemas();
        this.shardingNodes = confInit.getShardingNodes();
        this.apNodes = confInit.getApNodes();
        this.erRelations = confInit.getErRelations();
        this.funcNodeERMap = confInit.getFuncNodeERMap();
        this.functions = confInit.getFunctions();
        this.fullyConfigured = confInit.isFullyConfigured();
        ConfigUtil.setSchemasForPool(dbGroups, shardingNodes);

        this.reloadTime = TimeUtil.currentTimeMillis();

        this.lock = new ReentrantReadWriteLock();
        this.blacklistConfig = confInit.getBlacklistConfig();
    }

    public ServerConfig(RawJson userConfig, RawJson dbConfig, RawJson shardingConfig, RawJson sequenceConfig) {
        confInitNew = new ConfigInitializer(userConfig, dbConfig, shardingConfig, sequenceConfig);
        this.users = confInitNew.getUsers();
        this.dbGroups = confInitNew.getDbGroups();

        this.schemas = confInitNew.getSchemas();
        this.shardingNodes = confInitNew.getShardingNodes();
        this.apNodes = confInitNew.getApNodes();
        this.erRelations = confInitNew.getErRelations();
        this.funcNodeERMap = confInitNew.getFuncNodeERMap();
        this.functions = confInitNew.getFunctions();
        this.fullyConfigured = confInitNew.isFullyConfigured();
        ConfigUtil.setSchemasForPool(dbGroups, shardingNodes);

        this.reloadTime = TimeUtil.currentTimeMillis();

        this.lock = new ReentrantReadWriteLock();
        this.blacklistConfig = confInitNew.getBlacklistConfig();
        this.userConfig = userConfig;
        this.dbConfig = dbConfig;
        this.shardingConfig = shardingConfig;
        this.sequenceConfig = sequenceConfig;
    }

    private void waitIfChanging() {
        while (changing) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    public void testConnection() {
        try {
            confInitNew.testConnection();
        } catch (ConfigException e) {
            LOGGER.warn("TestConnection fail", e);
            AlertUtil.alertSelf(AlarmCode.TEST_CONN_FAIL, Alert.AlertLevel.WARN, "TestConnection fail:" + e.getMessage(), null);
        }
    }

    public void getAndSyncKeyVariables() throws Exception {
        ConfigUtil.checkDbleAndMysqlVersion(confInitNew.getDbGroups());
        ConfigUtil.getAndSyncKeyVariables(confInitNew.getDbGroups(), true);
        DbleServer.getInstance().getConfig().setLowerCase(DbleTempConfig.getInstance().isLowerCase());
    }

    public boolean isFullyConfigured() {
        waitIfChanging();
        return fullyConfigured;
    }

    public void fulllyConfigured() {
        waitIfChanging();
        fullyConfigured = true;
    }

    public Map<UserName, UserConfig> getUsers() {
        waitIfChanging();
        return users;
    }

    public Map<String, Properties> getBlacklistConfig() {
        waitIfChanging();
        return blacklistConfig;
    }

    public Map<String, SchemaConfig> getSchemas() {
        waitIfChanging();
        return schemas;
    }

    public Map<String, AbstractPartitionAlgorithm> getFunctions() {
        waitIfChanging();
        return functions;
    }

    public Map<String, ShardingNode> getShardingNodes() {
        waitIfChanging();
        return shardingNodes;
    }

    public Map<String, ApNode> getApNodes() {
        waitIfChanging();
        return apNodes;
    }

    public Map<String, ShardingNode> getAllNodes() {
        waitIfChanging();
        Map<String, ShardingNode> nodeMap = Maps.newHashMap();
        nodeMap.putAll(shardingNodes);
        nodeMap.putAll(apNodes);
        return nodeMap;
    }

    public Map<String, PhysicalDbGroup> getDbGroups() {
        waitIfChanging();
        return dbGroups;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        waitIfChanging();
        return erRelations;
    }

    public Map<String, Set<ERTable>> getFuncNodeERMap() {
        waitIfChanging();
        return funcNodeERMap;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public long getReloadTime() {
        waitIfChanging();
        return reloadTime;
    }

    public boolean reload(Map<UserName, UserConfig> newUsers, Map<String, SchemaConfig> newSchemas,
                          Map<String, ShardingNode> newShardingNodes, Map<String, PhysicalDbGroup> newDbGroups,
                          Map<String, PhysicalDbGroup> oldDbGroups,
                          Map<ERTable, Set<ERTable>> newErRelations,
                          Map<String, Set<ERTable>> newFuncNodeERMap,
                          SystemVariables newSystemVariables, boolean isFullyConfigured,
                          final int loadAllMode, Map<String, Properties> newBlacklistConfig, Map<String, AbstractPartitionAlgorithm> newFunctions,
                          RawJson userJsonConfig, RawJson sequenceJsonConfig, RawJson shardingJsonConfig, RawJson dbJsonConfig, List<ChangeItem> changeItemList) throws SQLNonTransientException {
        boolean result = apply(newUsers, newSchemas, newShardingNodes, newDbGroups, oldDbGroups, newErRelations, newFuncNodeERMap,
                newSystemVariables, isFullyConfigured, loadAllMode, newBlacklistConfig, newFunctions, userJsonConfig,
                sequenceJsonConfig, shardingJsonConfig, dbJsonConfig, changeItemList);
        this.reloadTime = TimeUtil.currentTimeMillis();
        return result;
    }

    private void calcDiffForMetaData(Map<String, SchemaConfig> newSchemas, Map<String, ShardingNode> newShardingNodes, int loadAllMode,
                                     List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables,
                                     List<String> delSchema, List<String> reloadSchema) {
        for (Map.Entry<String, SchemaConfig> schemaEntry : this.schemas.entrySet()) {
            String oldSchema = schemaEntry.getKey();
            SchemaConfig newSchemaConfig = newSchemas.get(oldSchema);
            if (newSchemaConfig == null) {
                delSchema.add(oldSchema);
            } else if ((loadAllMode & ManagerParseConfig.OPTR_MODE) == 0) { // reload @@config_all not contains -r
                SchemaConfig oldSchemaConfig = schemaEntry.getValue();
                if (!oldSchemaConfig.equalsConfigInfo(newSchemaConfig)) {
                    delSchema.add(oldSchema);
                    reloadSchema.add(oldSchema);
                } else {
                    if (newSchemaConfig.getDefaultShardingNodes() != null) { // reload config_all
                        //check shardingNode and dbGroup change
                        List<String> strShardingNodes = newSchemaConfig.getDefaultShardingNodes();
                        if (isShardingNodeChanged(strShardingNodes, newShardingNodes)) {
                            delSchema.add(oldSchema);
                            reloadSchema.add(oldSchema);
                            continue;
                        } else if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 && isDbGroupChanged(strShardingNodes, newShardingNodes)) { // reload @@config_all not contains -s
                            delSchema.add(oldSchema);
                            reloadSchema.add(oldSchema);
                            continue;
                        }
                    }
                    calcTableDiffForMetaData(newShardingNodes, loadAllMode, delTables, reloadTables, oldSchema, newSchemaConfig, oldSchemaConfig);
                }
            }
        }
        for (String newSchema : newSchemas.keySet()) {
            if ((loadAllMode & ManagerParseConfig.OPTR_MODE) != 0) { // reload @@config_all -r
                delSchema.add(newSchema);
                reloadSchema.add(newSchema);
            } else if (!this.schemas.containsKey(newSchema)) {
                reloadSchema.add(newSchema); // new added sharding
            }
        }
    }

    private void calcTableDiffForMetaData(Map<String, ShardingNode> newShardingNodes, int loadAllMode, List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables, String oldSchema, SchemaConfig newSchemaConfig, SchemaConfig oldSchemaConfig) {
        for (Map.Entry<String, BaseTableConfig> tableEntry : oldSchemaConfig.getTables().entrySet()) {
            String oldTable = tableEntry.getKey();
            BaseTableConfig newTableConfig = newSchemaConfig.getTables().get(oldTable);
            if (newTableConfig == null) {
                if (tableEntry.getValue() instanceof ShardingTableFakeConfig) {
                    newSchemaConfig.getTables().put(tableEntry.getKey(), tableEntry.getValue());
                } else {
                    delTables.add(new Pair<>(oldSchema, oldTable));
                }
            } else {
                BaseTableConfig oldTableConfig = tableEntry.getValue();
                if (newTableConfig.getClass() != oldTableConfig.getClass()) {
                    Pair<String, String> table = new Pair<>(oldSchema, oldTable);
                    delTables.add(table);
                    reloadTables.add(table);
                } else if (!newTableConfig.getShardingNodes().equals(oldTableConfig.getShardingNodes())) {
                    Pair<String, String> table = new Pair<>(oldSchema, oldTable);
                    delTables.add(table);
                    reloadTables.add(table);
                } else if (isShardingNodeChanged(oldTableConfig.getShardingNodes(), newShardingNodes)) {
                    Pair<String, String> table = new Pair<>(oldSchema, oldTable);
                    delTables.add(table);
                    reloadTables.add(table);
                } else if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 &&
                        isDbGroupChanged(oldTableConfig.getShardingNodes(), newShardingNodes)) { // reload @@config_all not contains -s
                    Pair<String, String> table = new Pair<>(oldSchema, oldTable);
                    delTables.add(table);
                    reloadTables.add(table);
                }
            }
        }
        for (String newTable : newSchemaConfig.getTables().keySet()) {
            if (!oldSchemaConfig.getTables().containsKey(newTable)) {
                reloadTables.add(new Pair<>(oldSchema, newTable)); // new added table
            }
        }
    }

    private boolean isShardingNodeChanged(List<String> strShardingNodes, Map<String, ShardingNode> newShardingNodes) {
        for (String strShardingNode : strShardingNodes) {
            ShardingNode newDBNode = newShardingNodes.get(strShardingNode);
            ShardingNode oldDBNode = shardingNodes.get(strShardingNode);
            if (!oldDBNode.getDatabase().equals(newDBNode.getDatabase()) ||
                    oldDBNode.getDbGroup() == null ||
                    !oldDBNode.getDbGroup().getGroupName().equals(newDBNode.getDbGroup().getGroupName())) {
                return true;
            }
        }
        return false;
    }

    private boolean isDbGroupChanged(List<String> strShardingNodes, Map<String, ShardingNode> newShardingNodes) {
        for (String strShardingNode : strShardingNodes) {
            PhysicalDbGroup newDBPool = newShardingNodes.get(strShardingNode).getDbGroup();
            PhysicalDbGroup oldDBPool = shardingNodes.get(strShardingNode).getDbGroup();
            PhysicalDbGroupDiff diff = new PhysicalDbGroupDiff(oldDBPool, newDBPool);
            if (!PhysicalDbGroupDiff.CHANGE_TYPE_NO.equals(diff.getChangeType())) {
                return true;
            }
        }
        return false;
    }

    private boolean apply(Map<UserName, UserConfig> newUsers,
                          Map<String, SchemaConfig> newSchemas,
                          Map<String, ShardingNode> newShardingNodes,
                          Map<String, PhysicalDbGroup> newDbGroups,
                          Map<String, PhysicalDbGroup> oldDbGroups,
                          Map<ERTable, Set<ERTable>> newErRelations,
                          Map<String, Set<ERTable>> newFuncNodeERMap,
                          SystemVariables newSystemVariables,
                          boolean isFullyConfigured, final int loadAllMode, Map<String, Properties> newBlacklistConfig, Map<String, AbstractPartitionAlgorithm> newFunctions,
                          RawJson userJsonConfig, RawJson sequenceJsonConfig, RawJson shardingJsonConfig, RawJson dbJsonConfig, List<ChangeItem> changeItemList) throws SQLNonTransientException {
        List<Pair<String, String>> delTables = new ArrayList<>();
        List<Pair<String, String>> reloadTables = new ArrayList<>();
        List<String> delSchema = new ArrayList<>();
        List<String> reloadSchema = new ArrayList<>();
        if (isFullyConfigured) {
            ReloadLogHelper.briefInfo("calcDiffForMetaData ...");
            calcDiffForMetaData(newSchemas, newShardingNodes, loadAllMode, delTables, reloadTables, delSchema, reloadSchema);
        }
        final ReentrantLock metaLock = ProxyMeta.getInstance().getTmManager().getMetaLock();
        metaLock.lock();
        this.changing = true;
        try {
            ReloadLogHelper.briefInfo("added metaLock");
            // user in use cannot be deleted
            ReloadLogHelper.briefInfo("checkUser ...");
            checkUser(changeItemList);
            String checkResult = ProxyMeta.getInstance().getTmManager().metaCountCheck();
            if (checkResult != null) {
                LOGGER.warn(checkResult);
                throw new SQLNonTransientException(checkResult, "HY000", ErrorCode.ER_DOING_DDL);
            }

            ReloadLogHelper.briefInfo("init new dbGroup start");
            if ((loadAllMode & ManagerParseConfig.OPTR_MODE) != 0) {
                //all dbGroup reload & recycle
                initDbGroupByMap(oldDbGroups, newDbGroups, newShardingNodes, isFullyConfigured, loadAllMode);
            } else {
                //replace dbGroup reference
                for (Map.Entry<String, ShardingNode> shardingNodeEntry : newShardingNodes.entrySet()) {
                    ShardingNode shardingNode = shardingNodeEntry.getValue();
                    PhysicalDbGroup oldDbGroup = oldDbGroups.get(shardingNode.getDbGroupName());
                    if (null == oldDbGroup) {
                        oldDbGroup = newDbGroups.get(shardingNode.getDbGroupName());
                    }
                    shardingNode.setDbGroup(oldDbGroup);
                }
                //only change dbGroup reload & recycle
                initDbGroupByMap(changeItemList, oldDbGroups, newShardingNodes, isFullyConfigured, loadAllMode);
                newDbGroups = oldDbGroups;
            }
            ReloadLogHelper.briefInfo("init new dbGroup end");
            ReloadLogHelper.briefInfo("config the transformation ...");
            this.shardingNodes = newShardingNodes;
            this.dbGroups = newDbGroups;
            this.fullyConfigured = isFullyConfigured;
            DbleServer.getInstance().reloadSystemVariables(newSystemVariables);
            CacheService.getInstance().reloadCache(newSystemVariables.isLowerCaseTableNames());
            this.users = newUsers;
            this.schemas = newSchemas;
            this.erRelations = newErRelations;
            this.funcNodeERMap = newFuncNodeERMap;
            this.blacklistConfig = newBlacklistConfig;
            this.functions = newFunctions;
            this.userConfig = userJsonConfig;
            this.dbConfig = dbJsonConfig;
            this.shardingConfig = shardingJsonConfig;
            this.sequenceConfig = sequenceJsonConfig;
            this.lowerCase = DbleTempConfig.getInstance().isLowerCase();

            try {
                ReloadLogHelper.briefInfo("ha config init ...");
                HaConfigManager.getInstance().init(true);
            } catch (Exception e) {
                throw new SQLNonTransientException("HaConfigManager init failed", "HY000", ErrorCode.ER_YES);
            }
            CacheService.getInstance().clearCache();
            this.changing = false;
            if (isFullyConfigured) {
                ReloadLogHelper.briefInfo("reloadMetaData start");
                boolean result = reloadMetaData(delTables, reloadTables, delSchema, reloadSchema);
                ReloadLogHelper.briefInfo("reloadMetaData end");
                return result;
            }
        } finally {
            this.changing = false;
            metaLock.unlock();
            ReloadLogHelper.briefInfo("released metaLock");
        }
        return true;
    }


    /**
     * user in use cannot be deleted
     */
    private static void checkUser(List<ChangeItem> changeItemList) {
        for (ChangeItem changeItem : changeItemList) {
            ChangeType type = changeItem.getType();
            Object item = changeItem.getItem();
            ChangeItemType itemType = changeItem.getItemType();
            if (type == ChangeType.DELETE && itemType == ChangeItemType.USERNAME) {
                //check is it in use
                Integer count = FrontendUserManager.getInstance().getUserConnectionMap().get(item);
                if (null != count && count > 0) {
                    throw new ConfigException("user['" + item.toString() + "'] is being used.");
                }
            } else if (type == ChangeType.UPDATE && changeItem.isAffectEntryDbGroup() && itemType == ChangeItemType.USERNAME) {
                //check is it in use
                Integer count = FrontendUserManager.getInstance().getUserConnectionMap().get(item);
                if (null != count && count > 0) {
                    throw new ConfigException("user['" + item.toString() + "'] is being used.");
                }
            }
        }
    }

    private static void initDbGroupByMap(Map<String, PhysicalDbGroup> oldDbGroups, Map<String, PhysicalDbGroup> newDbGroups,
                                         Map<String, ShardingNode> newShardingNodes, boolean fullyConfigured, int loadAllMode) {
        if (oldDbGroups != null) {
            //Only -r uses this method to recycle the connection pool
            String recycleGroupName;
            PhysicalDbGroup recycleGroup;
            for (Map.Entry<String, PhysicalDbGroup> entry : oldDbGroups.entrySet()) {
                recycleGroupName = entry.getKey();
                recycleGroup = entry.getValue();
                // avoid recycleGroup == newGroup, can't stop recycleGroup
                if (newDbGroups.get(recycleGroupName) != recycleGroup) {
                    ReloadLogHelper.briefInfo("recycle old group. old active backend conn will be close");
                    recycleGroup.stop("reload config, recycle old group", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
                }
            }
        }
        for (PhysicalDbGroup dbGroup : newDbGroups.values()) {
            String hostName = dbGroup.getGroupName();
            // set schemas
            ArrayList<String> dnSchemas = new ArrayList<>(30);
            for (ShardingNode dn : newShardingNodes.values()) {
                if (dn.getDbGroup().getGroupName().equals(hostName)) {
                    dn.setDbGroup(dbGroup);
                    dnSchemas.add(dn.getDatabase());
                }
            }
            dbGroup.setSchemas(dnSchemas);
            if (fullyConfigured) {
                dbGroup.init("reload config");
            } else {
                ReloadLogHelper.briefInfo("dbGroup[" + hostName + "] is not fullyConfigured, so doing nothing");
            }
        }
    }


    private void initDbGroupByMap(List<ChangeItem> changeItemList, Map<String, PhysicalDbGroup> oldDbGroupMap, Map<String, ShardingNode> newShardingNodes,
                                  boolean isFullyConfigured, int loadAllMode) {
        Map<ChangeItem, PhysicalDbGroup> updateDbGroupMap = Maps.newHashMap();
        for (ChangeItem changeItem : changeItemList) {
            Object item = changeItem.getItem();
            ChangeItemType itemType = changeItem.getItemType();
            switch (changeItem.getType()) {
                case ADD:
                    addItem(item, itemType, oldDbGroupMap, newShardingNodes, isFullyConfigured);
                    break;
                case UPDATE:
                    updateItem(item, itemType, oldDbGroupMap, newShardingNodes, changeItem, updateDbGroupMap, loadAllMode);
                    break;
                case DELETE:
                    deleteItem(item, itemType, oldDbGroupMap, loadAllMode);
                    break;
                default:
                    break;
            }
        }
        updateDbGroupMap.forEach((changeItem, dbGroup) -> {
            if (changeItem.isAffectHeartbeat()) {
                dbGroup.startHeartbeat();
            }
            if (changeItem.isAffectDelayDetection() && dbGroup.isDelayDetectionStart()) {
                dbGroup.startDelayDetection();
            }
        });
    }

    private void deleteItem(Object item, ChangeItemType itemType, Map<String, PhysicalDbGroup> oldDbGroupMap, int loadAllMode) {
        if (itemType == ChangeItemType.PHYSICAL_DB_GROUP) {
            //delete dbGroup
            PhysicalDbGroup physicalDbGroup = (PhysicalDbGroup) item;
            PhysicalDbGroup oldDbGroup = oldDbGroupMap.remove(physicalDbGroup.getGroupName());
            oldDbGroup.stop("reload config, recycle old group", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
            oldDbGroup = null;
        } else if (itemType == ChangeItemType.PHYSICAL_DB_INSTANCE) {
            PhysicalDbInstance physicalDbInstance = (PhysicalDbInstance) item;
            //delete slave instance
            PhysicalDbGroup physicalDbGroup = oldDbGroupMap.get(physicalDbInstance.getDbGroupConfig().getName());
            PhysicalDbInstance oldDbInstance = physicalDbGroup.getAllDbInstanceMap().get(physicalDbInstance.getName());
            oldDbInstance.stop("reload config, recycle old instance", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0), true);
            oldDbInstance = null;
            removeDbInstance(physicalDbGroup, physicalDbInstance.getName());
        } else if (itemType == ChangeItemType.SHARDING_NODE) {
            ShardingNode shardingNode = (ShardingNode) item;
            if (shardingNode.getDbGroup() != null) {
                shardingNode.getDbGroup().removeSchema(shardingNode.getDatabase());
            }
        } else if (itemType == ChangeItemType.AP_NODE) {
            ApNode apNode = (ApNode) item;
            if (apNode.getDbGroup() != null) {
                apNode.getDbGroup().removeSchema(apNode.getDatabase());
            }
        }
    }

    private void updateItem(Object item, ChangeItemType itemType, Map<String, PhysicalDbGroup> oldDbGroupMap, Map<String, ShardingNode> newShardingNodes, ChangeItem changeItem,
                            Map<ChangeItem, PhysicalDbGroup> updateDbGroupMap, int loadAllMode) {
        if (itemType == ChangeItemType.PHYSICAL_DB_GROUP) {
            //change dbGroup
            PhysicalDbGroup physicalDbGroup = (PhysicalDbGroup) item;
            PhysicalDbGroup oldDbGroup = oldDbGroupMap.get(physicalDbGroup.getGroupName());
            boolean dbGroupCopy = false;
            if (changeItem.isAffectHeartbeat()) {
                oldDbGroup.stopHeartbeat("reload config, stop group heartbeat");
                oldDbGroup.copyBaseInfo(physicalDbGroup);
                dbGroupCopy = true;
                //create a new heartbeat in the follow-up
                updateDbGroupMap.put(changeItem, oldDbGroup);
            }
            if (changeItem.isAffectDelayDetection()) {
                oldDbGroup.stopDelayDetection("reload config, stop group delayDetection");
                if (!dbGroupCopy) {
                    oldDbGroup.copyBaseInfo(physicalDbGroup);
                    dbGroupCopy = true;
                }
                updateDbGroupMap.put(changeItem, oldDbGroup);
            }
            if (!dbGroupCopy) {
                oldDbGroup.copyBaseInfo(physicalDbGroup);
            }
            reloadSchema(oldDbGroup, newShardingNodes);
            if (changeItem.isAffectConnectionPool()) {
                if (physicalDbGroup.getRwSplitMode() == 0) {
                    oldDbGroup.stopPool("reload config, recycle read instance", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0), false);
                } else {
                    oldDbGroup.startPool("reload config, init read instance", false);
                }
                if (physicalDbGroup.isUseless()) {
                    oldDbGroup.stopPool("reload config, recycle all instance", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0), true);
                } else {
                    oldDbGroup.startPool("reload config, init all instance", true);
                }
            }
            oldDbGroupMap.put(physicalDbGroup.getGroupName(), oldDbGroup);
        } else if (itemType == ChangeItemType.PHYSICAL_DB_INSTANCE) {
            if (changeItem.isAffectHeartbeat() || changeItem.isAffectConnectionPool() || changeItem.isAffectDelayDetection()) {
                PhysicalDbInstance physicalDbInstance = (PhysicalDbInstance) item;
                PhysicalDbGroup physicalDbGroup = oldDbGroupMap.get(physicalDbInstance.getDbGroupConfig().getName());
                PhysicalDbInstance oldDbInstance = physicalDbGroup.getAllDbInstanceMap().get(physicalDbInstance.getName());
                oldDbInstance.stopDirectly("reload config, recycle old instance", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0), true);
                oldDbInstance = null;
                removeDbInstance(physicalDbGroup, physicalDbInstance.getName());
                physicalDbGroup.setDbInstance(physicalDbInstance);
                physicalDbInstance.init("reload config", true, true);
            } else {
                PhysicalDbInstance physicalDbInstance = (PhysicalDbInstance) item;
                PhysicalDbGroup physicalDbGroup = oldDbGroupMap.get(physicalDbInstance.getDbGroupConfig().getName());
                PhysicalDbInstance oldDbInstance = physicalDbGroup.getAllDbInstanceMap().get(physicalDbInstance.getName());
                //copy base config
                oldDbInstance.copyBaseInfo(physicalDbInstance);
                if (changeItem.isAffectPoolCapacity()) {
                    oldDbInstance.updatePoolCapacity();
                }
            }
        } else if (itemType == ChangeItemType.SHARDING_NODE) {
            ShardingNode shardingNode = (ShardingNode) item;
            ShardingNode oldShardingNode = this.shardingNodes.get(shardingNode.getName());
            if (oldShardingNode != null && oldShardingNode.getDbGroup() != null) {
                oldShardingNode.getDbGroup().removeSchema(oldShardingNode.getDatabase());
            }
            if (shardingNode.getDbGroup() != null) {
                shardingNode.getDbGroup().addSchema(shardingNode.getDatabase());
            }
        } else if (itemType == ChangeItemType.AP_NODE) {
            ApNode apNode = (ApNode) item;
            ApNode oldApNode = this.apNodes.get(apNode.getName());
            if (oldApNode != null && oldApNode.getDbGroup() != null) {
                oldApNode.getDbGroup().removeSchema(oldApNode.getDatabase());
            }
            if (apNode.getDbGroup() != null) {
                apNode.getDbGroup().addSchema(apNode.getDatabase());
            }
        }
    }

    public PhysicalDbInstance removeDbInstance(PhysicalDbGroup dbGroup, String instanceName) {
        PhysicalDbInstance dbInstance = dbGroup.getAllDbInstanceMap().remove(instanceName);
        if (dbInstance.getConfig().isPrimary()) {
            if (dbGroup.getWriteDbInstance() == dbInstance) {
                dbGroup.setWriteDbInstance(null);
                dbGroup.getDbGroupConfig().setWriteInstanceConfig(null);
            }
        } else {
            dbGroup.getDbGroupConfig().removeReadInstance(dbInstance.getConfig());
        }
        return dbInstance;
    }

    private void addItem(Object item, ChangeItemType itemType, Map<String, PhysicalDbGroup> oldDbGroupMap, Map<String, ShardingNode> newShardingNodes, boolean isFullyConfigured) {
        if (itemType == ChangeItemType.PHYSICAL_DB_GROUP) {
            //add dbGroup+dbInstance
            PhysicalDbGroup physicalDbGroup = (PhysicalDbGroup) item;
            initDbGroup(physicalDbGroup, newShardingNodes, isFullyConfigured);
            oldDbGroupMap.put(physicalDbGroup.getGroupName(), physicalDbGroup);
        } else if (itemType == ChangeItemType.PHYSICAL_DB_INSTANCE) {
            //add dbInstance
            PhysicalDbInstance dbInstance = (PhysicalDbInstance) item;
            PhysicalDbGroup physicalDbGroup = oldDbGroupMap.get(dbInstance.getDbGroupConfig().getName());
            if (isFullyConfigured) {
                physicalDbGroup.setDbInstance(dbInstance);
                dbInstance.init("reload config", true, true);
            } else {
                LOGGER.info("dbGroup[" + dbInstance.getDbGroupConfig().getName() + "] is not fullyConfigured, so doing nothing");
            }
        } else if (itemType == ChangeItemType.SHARDING_NODE) {
            ShardingNode shardingNode = (ShardingNode) item;
            if (shardingNode.getDbGroup() != null) {
                shardingNode.getDbGroup().addSchema(shardingNode.getDatabase());
            }
        } else if (itemType == ChangeItemType.AP_NODE) {
            ApNode apNode = (ApNode) item;
            if (apNode.getDbGroup() != null) {
                apNode.getDbGroup().addSchema(apNode.getDatabase());
            }
        }
    }

    public static void reloadSchema(PhysicalDbGroup dbGroup, Map<String, ShardingNode> newShardingNodes) {
        String hostName = dbGroup.getGroupName();
        // set schemas
        ArrayList<String> dnSchemas = new ArrayList<>(30);
        for (ShardingNode dn : newShardingNodes.values()) {
            if (dn.getDbGroup().getGroupName().equals(hostName)) {
                dn.setDbGroup(dbGroup);
                dnSchemas.add(dn.getDatabase());
            }
        }
        dbGroup.setSchemas(dnSchemas);
    }


    private static void initDbGroup(PhysicalDbGroup dbGroup, Map<String, ShardingNode> newShardingNodes, boolean fullyConfigured) {
        reloadSchema(dbGroup, newShardingNodes);
        if (fullyConfigured) {
            dbGroup.init("reload config");
        } else {
            LOGGER.info("dbGroup[" + dbGroup.getGroupName() + "] is not fullyConfigured, so doing nothing");
        }
    }

    private boolean reloadMetaData(List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables, List<String> delSchema, List<String> reloadSchema) {
        boolean reloadResult = true;
        if (delSchema.size() > 0) {
            if (LOGGER.isDebugEnabled()) {
                ReloadLogHelper.debug("metadata will delete schema:" + StringUtil.join(delSchema, ","));
            }
            for (String schema : delSchema) {
                ProxyMeta.getInstance().getTmManager().getCatalogs().remove(schema);
            }
            ReloadLogHelper.briefInfo("metadata finished for deleted schemas");
        }
        if (delTables.size() > 0) {
            if (LOGGER.isDebugEnabled()) {
                String tables = changeTablesToString(delTables);
                ReloadLogHelper.debug("metadata will delete Tables:" + tables);
            }
            for (Pair<String, String> table : delTables) {
                SchemaMeta oldSchemaMeta = ProxyMeta.getInstance().getTmManager().getCatalogs().get(table.getKey());
                if (oldSchemaMeta != null) {
                    oldSchemaMeta.dropTable(table.getValue());
                }
            }
            ReloadLogHelper.briefInfo("metadata finished for deleted tables");
        }
        if (reloadSchema.size() != 0 || reloadTables.size() != 0) {
            Map<String, Set<String>> specifiedSchemas = new HashMap<>();
            if (reloadSchema.size() > 0) {
                if (LOGGER.isDebugEnabled()) {
                    ReloadLogHelper.debug("metadata will reload schema:" + StringUtil.join(reloadSchema, ","));
                }
                for (String schema : reloadSchema) {
                    specifiedSchemas.put(schema, null);
                }
            }
            if (reloadTables.size() > 0) {
                if (LOGGER.isDebugEnabled()) {
                    String tables = changeTablesToString(reloadTables);
                    ReloadLogHelper.debug("metadata will reload Tables:" + tables);
                }
                for (Pair<String, String> table : reloadTables) {
                    Set<String> tables = specifiedSchemas.computeIfAbsent(table.getKey(), k -> new HashSet<>());
                    tables.add(table.getValue());
                }
            }
            reloadResult = ProxyMeta.getInstance().reloadMetaData(this, specifiedSchemas);
            ReloadLogHelper.briefInfo("metadata finished for changes of schemas and tables");
        }
        return reloadResult;
    }

    private String changeTablesToString(List<Pair<String, String>> delTables) {
        StringBuilder sb = new StringBuilder();
        for (Pair<String, String> delTable : delTables) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("'");
            sb.append(delTable.getKey());
            sb.append(delTable.getValue());
            sb.append("'");
        }
        return sb.toString();
    }

    public void reviseLowerCase() {

        //user sharding
        for (UserConfig uc : users.values()) {
            if (uc instanceof ShardingUserConfig) { // contains HybridTAUserConfig
                ShardingUserConfig shardingUser = (ShardingUserConfig) uc;
                shardingUser.changeMapToLowerCase();
                if (shardingUser.getPrivilegesConfig() != null) {
                    shardingUser.getPrivilegesConfig().changeMapToLowerCase();
                }
            }
        }

        for (ShardingNode physicalDBNode : shardingNodes.values()) {
            physicalDBNode.toLowerCase();
        }

        //schemas
        Map<String, SchemaConfig> newSchemas = new HashMap<>();
        for (Map.Entry<String, SchemaConfig> entry : schemas.entrySet()) {
            SchemaConfig newSchema = new SchemaConfig(entry.getValue());
            newSchemas.put(entry.getKey().toLowerCase(), newSchema);
        }
        this.schemas = newSchemas;


        if (erRelations != null) {
            HashMap<ERTable, Set<ERTable>> newErMap = new HashMap<>();
            for (Map.Entry<ERTable, Set<ERTable>> entry : erRelations.entrySet()) {
                ERTable key = entry.getKey();
                Set<ERTable> value = entry.getValue();

                Set<ERTable> newValues = new HashSet<>();
                for (ERTable table : value) {
                    newValues.add(table.changeToLowerCase());
                }

                newErMap.put(key.changeToLowerCase(), newValues);
            }

            erRelations = newErMap;
        }
    }

    public void reloadSequence(RawJson sequenceJson) {
        SequenceManager.reload(sequenceJson, this.getShardingNodes().keySet());
    }

    public void loadSequence(RawJson sequenceJson) {
        SequenceManager.load(sequenceJson, this.getShardingNodes().keySet());
    }

    public void tryLoadSequence(RawJson sequenceJson, Logger logger) {
        SequenceManager.tryLoad(sequenceJson, this.getShardingNodes().keySet(), logger);
    }

    public void selfChecking0() throws ConfigException {
        // check 1.user's schemas are all existed in sharding's conf
        // 2.sharding's conf is not empty
        if (users == null || users.isEmpty()) {
            throw new ConfigException("SelfCheck### user all node is empty!");
        } else {
            for (UserConfig uc : users.values()) {
                if (uc instanceof ShardingUserConfig) {
                    ShardingUserConfig shardingUser = (ShardingUserConfig) uc;
                    Set<String> authSchemas = shardingUser.getSchemas();
                    for (String schema : authSchemas) {
                        checkSchemaByUser(schema, shardingUser);
                    }

                    if (shardingUser.getPrivilegesConfig() != null) {
                        for (String schema : shardingUser.getPrivilegesConfig().getSchemaPrivileges().keySet()) {
                            if (!authSchemas.contains(schema)) {
                                throw new ConfigException("SelfCheck### privileges's schema[" + schema + "] was not found in the user [name:" + shardingUser.getName() + (shardingUser.getTenant() == null ? "" : ",tenant:" + shardingUser.getTenant()) + "]'s schemas");
                            }
                        }
                    }
                }
            }
        }

        // check sharding
        for (SchemaConfig sc : schemas.values()) {
            if (null == sc) {
                throw new ConfigException("SelfCheck### sharding all node is empty!");
            } else {
                // check shardingNode / dbGroup
                if (this.shardingNodes != null && this.dbGroups != null) {
                    Set<String> shardingNodeNames = sc.getAllShardingNodes();
                    for (String shardingNodeName : shardingNodeNames) {
                        ShardingNode node = this.shardingNodes.get(shardingNodeName);
                        if (node == null) {
                            throw new ConfigException("SelfCheck### sharding shardingNode[" + shardingNodeName + "] is empty!");
                        }
                    }
                }
            }
        }
    }

    private void checkSchemaByUser(String schema, ShardingUserConfig shardingUser) {
        if (!schemas.containsKey(schema)) {
            String errMsg = "SelfCheck### User[name:" + shardingUser.getName() + (shardingUser.getTenant() == null ? "" : ",tenant:" + shardingUser.getTenant()) + "]'s schema [" + schema + "] is not exist!";
            throw new ConfigException(errMsg);
        }
        if (shardingUser instanceof HybridTAUserConfig && schemas.get(schema).getDefaultApNode() == null) {
            String errMsg = "SelfCheck### User[name:" + shardingUser.getName() + (shardingUser.getTenant() == null ? "" : ",tenant:" + shardingUser.getTenant()) + "]'s schema [" + schema + "] must contain apNode!";
            throw new ConfigException(errMsg);
        } else if (!(shardingUser instanceof HybridTAUserConfig) && schemas.get(schema).getDefaultApNode() != null) { // ShardingUserConfig
            String errMsg = "SelfCheck### User[name:" + shardingUser.getName() + (shardingUser.getTenant() == null ? "" : ",tenant:" + shardingUser.getTenant()) + "]'s schema [" + schema + "] can not contain apNode!";
            throw new ConfigException(errMsg);
        }
    }

    public void syncJsonToLocal(boolean isWriteToLocal) throws Exception {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Shardings.class);
        xmlProcess.addParseClass(DbGroups.class);
        xmlProcess.addParseClass(Users.class);
        // init xml
        xmlProcess.initJaxbClass();


        //sharding
        if ((this.shardingConfig) != null) {
            ClusterLogic.forConfig().syncShardingXmlToLocal(this.shardingConfig, xmlProcess, JsonFactory.getJson(), isWriteToLocal);
        }

        //db
        ClusterLogic.forConfig().syncDbXmlToLocal(xmlProcess, this.dbConfig, isWriteToLocal);

        //user
        ClusterLogic.forConfig().syncUserXmlToLocal(this.userConfig, xmlProcess, JsonFactory.getJson(), isWriteToLocal);

        //sequence
        ClusterLogic.forConfig().syncSequenceToLocal(this.sequenceConfig, isWriteToLocal);
    }

    public RawJson getDbConfig() {
        return dbConfig;
    }

    public void setDbConfig(RawJson dbConfig) {
        this.dbConfig = dbConfig;
    }

    public RawJson getShardingConfig() {
        return shardingConfig;
    }

    public RawJson getUserConfig() {
        return userConfig;
    }

    public RawJson getSequenceConfig() {
        return sequenceConfig;
    }

    public Boolean isLowerCase() {
        return lowerCase;
    }

    public void setLowerCase(Boolean lowerCase) {
        this.lowerCase = lowerCase;
    }
}


