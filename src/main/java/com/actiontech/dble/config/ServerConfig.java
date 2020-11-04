/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbGroupDiff;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.ManagerParseConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.singleton.CacheService;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
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
    private volatile Map<String, PhysicalDbGroup> dbGroups;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile boolean fullyConfigured;
    private volatile long reloadTime;
    private volatile boolean changing = false;
    private final ReentrantReadWriteLock lock;
    private ConfigInitializer confInitNew;
    private volatile Map<String, Properties> blacklistConfig;
    private volatile Map<String, AbstractPartitionAlgorithm> functions;

    public ServerConfig() {
        //read sharding.xml,db.xml and user.xml
        confInitNew = new ConfigInitializer(false);
        this.users = confInitNew.getUsers();
        this.dbGroups = confInitNew.getDbGroups();

        this.schemas = confInitNew.getSchemas();
        this.shardingNodes = confInitNew.getShardingNodes();
        this.erRelations = confInitNew.getErRelations();
        this.functions = confInitNew.getFunctions();
        this.fullyConfigured = confInitNew.isFullyConfigured();
        ConfigUtil.setSchemasForPool(dbGroups, shardingNodes);

        this.reloadTime = TimeUtil.currentTimeMillis();

        this.lock = new ReentrantReadWriteLock();
        this.blacklistConfig = confInitNew.getBlacklistConfig();
    }


    public ServerConfig(ConfigInitializer confInit) {
        //read sharding.xml,db.xml and user.xml
        this.users = confInit.getUsers();
        this.dbGroups = confInit.getDbGroups();
        this.schemas = confInit.getSchemas();
        this.shardingNodes = confInit.getShardingNodes();
        this.erRelations = confInit.getErRelations();
        this.functions = confInit.getFunctions();
        this.fullyConfigured = confInit.isFullyConfigured();
        ConfigUtil.setSchemasForPool(dbGroups, shardingNodes);

        this.reloadTime = TimeUtil.currentTimeMillis();

        this.lock = new ReentrantReadWriteLock();
        this.blacklistConfig = confInit.getBlacklistConfig();
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
        ConfigUtil.getAndSyncKeyVariables(confInitNew.getDbGroups(), true);
    }

    public boolean isFullyConfigured() {
        waitIfChanging();
        return fullyConfigured;
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

    public Map<String, PhysicalDbGroup> getDbGroups() {
        waitIfChanging();
        return dbGroups;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        waitIfChanging();
        return erRelations;
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
                          Map<String, PhysicalDbGroup> recycleDbGroups,
                          Map<ERTable, Set<ERTable>> newErRelations,
                          SystemVariables newSystemVariables, boolean isFullyConfigured,
                          final int loadAllMode, Map<String, Properties> newBlacklistConfig, Map<String, AbstractPartitionAlgorithm> newFunctions) throws SQLNonTransientException {
        boolean result = apply(newUsers, newSchemas, newShardingNodes, newDbGroups, recycleDbGroups, newErRelations,
                newSystemVariables, isFullyConfigured, loadAllMode, newBlacklistConfig, newFunctions);
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
                if (!StringUtil.equalsWithEmpty(oldSchemaConfig.getShardingNode(), newSchemaConfig.getShardingNode())) {
                    delSchema.add(oldSchema);
                    reloadSchema.add(oldSchema);
                } else {
                    if (newSchemaConfig.getShardingNode() != null) { // reload config_all
                        //check shardingNode and dbGroup change
                        List<String> strShardingNodes = Collections.singletonList(newSchemaConfig.getShardingNode());
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
                delTables.add(new Pair<>(oldSchema, oldTable));
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
                          Map<String, PhysicalDbGroup> recycleDbGroups,
                          Map<ERTable, Set<ERTable>> newErRelations,
                          SystemVariables newSystemVariables,
                          boolean isFullyConfigured, final int loadAllMode, Map<String, Properties> newBlacklistConfig, Map<String, AbstractPartitionAlgorithm> newFunctions) throws SQLNonTransientException {
        List<Pair<String, String>> delTables = new ArrayList<>();
        List<Pair<String, String>> reloadTables = new ArrayList<>();
        List<String> delSchema = new ArrayList<>();
        List<String> reloadSchema = new ArrayList<>();
        if (isFullyConfigured) {
            calcDiffForMetaData(newSchemas, newShardingNodes, loadAllMode, delTables, reloadTables, delSchema, reloadSchema);
        }
        final ReentrantLock metaLock = ProxyMeta.getInstance().getTmManager().getMetaLock();
        metaLock.lock();
        this.changing = true;
        try {
            String checkResult = ProxyMeta.getInstance().getTmManager().metaCountCheck();
            if (checkResult != null) {
                LOGGER.warn(checkResult);
                throw new SQLNonTransientException(checkResult, "HY000", ErrorCode.ER_DOING_DDL);
            }
            try {
                HaConfigManager.getInstance().init();
            } catch (Exception e) {
                throw new SQLNonTransientException("HaConfigManager init failed", "HY000", ErrorCode.ER_YES);
            }
            // old dbGroup
            // 1 stop heartbeat
            // 2 backup
            //--------------------------------------------
            if (recycleDbGroups != null) {
                String recycleGroupName;
                PhysicalDbGroup recycleGroup;
                for (Map.Entry<String, PhysicalDbGroup> entry : recycleDbGroups.entrySet()) {
                    recycleGroupName = entry.getKey();
                    recycleGroup = entry.getValue();
                    // avoid recycleGroup == newGroup, can't stop recycleGroup
                    if (newDbGroups.get(recycleGroupName) != recycleGroup) {
                        ReloadLogHelper.info("reload config, recycle old group. old active backend conn will be close", LOGGER);
                        recycleGroup.stop("reload config, recycle old group", ((loadAllMode & ManagerParseConfig.OPTF_MODE) != 0));
                    }
                }
            }
            this.shardingNodes = newShardingNodes;
            this.dbGroups = newDbGroups;
            this.fullyConfigured = isFullyConfigured;
            DbleServer.getInstance().reloadSystemVariables(newSystemVariables);
            CacheService.getInstance().reloadCache(newSystemVariables.isLowerCaseTableNames());
            this.users = newUsers;
            this.schemas = newSchemas;
            this.erRelations = newErRelations;
            this.blacklistConfig = newBlacklistConfig;
            this.functions = newFunctions;
            CacheService.getInstance().clearCache();
            this.changing = false;
            if (isFullyConfigured) {
                return reloadMetaData(delTables, reloadTables, delSchema, reloadSchema);
            }
        } finally {
            this.changing = false;
            metaLock.unlock();
        }
        return true;
    }

    private boolean reloadMetaData(List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables, List<String> delSchema, List<String> reloadSchema) {
        boolean reloadResult = true;
        if (delSchema.size() > 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("metadata will delete schema:" + StringUtil.join(delSchema, ","));
            }
            for (String schema : delSchema) {
                ProxyMeta.getInstance().getTmManager().getCatalogs().remove(schema);
            }
            LOGGER.info("metadata finished for deleted schemas");
        }
        if (delTables.size() > 0) {
            if (LOGGER.isDebugEnabled()) {
                String tables = changeTablesToString(delTables);
                LOGGER.debug("metadata will delete Tables:" + tables);
            }
            for (Pair<String, String> table : delTables) {
                ProxyMeta.getInstance().getTmManager().getCatalogs().get(table.getKey()).dropTable(table.getValue());
            }
            LOGGER.info("metadata finished for deleted tables");
        }
        if (reloadSchema.size() != 0 || reloadTables.size() != 0) {
            Map<String, Set<String>> specifiedSchemas = new HashMap<>();
            if (reloadSchema.size() > 0) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("metadata will reload schema:" + StringUtil.join(reloadSchema, ","));
                }
                for (String schema : reloadSchema) {
                    specifiedSchemas.put(schema, null);
                }
            }
            if (reloadTables.size() > 0) {
                if (LOGGER.isDebugEnabled()) {
                    String tables = changeTablesToString(reloadTables);
                    LOGGER.debug("metadata will reload Tables:" + tables);
                }
                for (Pair<String, String> table : reloadTables) {
                    Set<String> tables = specifiedSchemas.computeIfAbsent(table.getKey(), k -> new HashSet<>());
                    tables.add(table.getValue());
                }
            }
            reloadResult = ProxyMeta.getInstance().reloadMetaData(this, specifiedSchemas);
            LOGGER.info("metadata finished for changes of schemas and tables");
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

    /**
     * turned all the config into lowerCase config
     */
    public void reviseLowerCase() {

        //user sharding
        for (UserConfig uc : users.values()) {
            if (uc instanceof ShardingUserConfig) {
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
        loadSequence();
        selfChecking0();

    }

    public void loadSequence() {
        SequenceManager.load(DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
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
                        if (!schemas.containsKey(schema)) {
                            String errMsg = "SelfCheck### User[name:" + shardingUser.getName() + (shardingUser.getTenant() == null ? "" : ",tenant:" + shardingUser.getTenant()) + "]'s schema [" + schema + "] is not exist!";
                            throw new ConfigException(errMsg);
                        }
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

}


