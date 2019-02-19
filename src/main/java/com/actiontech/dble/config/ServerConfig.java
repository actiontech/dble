/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDBPoolDiff;
import com.actiontech.dble.config.model.*;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.route.parser.ManagerParseConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.sequence.handler.DistributedSequenceHandler;
import com.actiontech.dble.route.sequence.handler.IncrSequenceMySQLHandler;
import com.actiontech.dble.route.sequence.handler.IncrSequenceTimeHandler;
import com.actiontech.dble.route.sequence.handler.IncrSequenceZKHandler;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class ServerConfig {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);
    private static final int RELOAD = 1;
    private static final int ROLLBACK = 2;
    private static final int RELOAD_ALL = 3;

    private volatile SystemConfig system;
    private volatile FirewallConfig firewall;
    private volatile FirewallConfig firewall2;
    private volatile Map<String, UserConfig> users;
    private volatile Map<String, UserConfig> users2;
    private volatile Map<String, SchemaConfig> schemas;
    private volatile Map<String, SchemaConfig> schemas2;
    private volatile Map<String, PhysicalDBNode> dataNodes;
    private volatile Map<String, PhysicalDBNode> dataNodes2;
    private volatile Map<String, PhysicalDBPool> dataHosts;
    private volatile Map<String, PhysicalDBPool> dataHosts2;
    private volatile Map<ERTable, Set<ERTable>> erRelations;
    private volatile Map<ERTable, Set<ERTable>> erRelations2;
    private volatile boolean dataHostWithoutWR;
    private volatile boolean dataHostWithoutWR2;
    private volatile long reloadTime;
    private volatile long rollbackTime;
    private volatile int status;
    private volatile boolean changing = false;
    private final ReentrantLock lock;

    public ServerConfig() {
        //read schema.xml,rule.xml and server.xml
        ConfigInitializer confInit = new ConfigInitializer(true, false);
        this.system = confInit.getSystem();
        this.users = confInit.getUsers();
        this.schemas = confInit.getSchemas();
        this.dataHosts = confInit.getDataHosts();
        this.dataNodes = confInit.getDataNodes();
        this.erRelations = confInit.getErRelations();
        this.dataHostWithoutWR = confInit.isDataHostWithoutWH();
        ConfigUtil.setSchemasForPool(dataHosts, dataNodes);

        this.firewall = confInit.getFirewall();

        this.reloadTime = TimeUtil.currentTimeMillis();
        this.rollbackTime = -1L;
        this.status = RELOAD;

        this.lock = new ReentrantLock();
        try {
            confInit.testConnection(true);
        } catch (ConfigException e) {
            LOGGER.warn("TestConnection fail", e);
            AlertUtil.alertSelf(AlarmCode.TEST_CONN_FAIL, Alert.AlertLevel.WARN, "TestConnection fail:" + e.getMessage(), null);
        }
    }


    public ServerConfig(ConfigInitializer confInit) {
        //read schema.xml,rule.xml and server.xml
        this.system = confInit.getSystem();
        this.users = confInit.getUsers();
        this.schemas = confInit.getSchemas();
        this.dataHosts = confInit.getDataHosts();
        this.dataNodes = confInit.getDataNodes();
        this.erRelations = confInit.getErRelations();
        this.dataHostWithoutWR = confInit.isDataHostWithoutWH();
        ConfigUtil.setSchemasForPool(dataHosts, dataNodes);

        this.firewall = confInit.getFirewall();

        this.reloadTime = TimeUtil.currentTimeMillis();
        this.rollbackTime = -1L;
        this.status = RELOAD;

        this.lock = new ReentrantLock();
    }

    private void waitIfChanging() {
        while (changing) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    public SystemConfig getSystem() {
        waitIfChanging();
        return system;
    }

    public boolean isDataHostWithoutWR() {
        waitIfChanging();
        return dataHostWithoutWR;
    }

    public Map<String, UserConfig> getUsers() {
        waitIfChanging();
        return users;
    }

    public Map<String, UserConfig> getBackupUsers() {
        waitIfChanging();
        return users2;
    }

    public Map<String, SchemaConfig> getSchemas() {
        waitIfChanging();
        return schemas;
    }

    public Map<String, SchemaConfig> getBackupSchemas() {
        waitIfChanging();
        return schemas2;
    }

    public Map<String, PhysicalDBNode> getDataNodes() {
        waitIfChanging();
        return dataNodes;
    }


    public Map<String, PhysicalDBNode> getBackupDataNodes() {
        waitIfChanging();
        return dataNodes2;
    }

    public Map<String, PhysicalDBPool> getDataHosts() {
        waitIfChanging();
        return dataHosts;
    }

    public Map<String, PhysicalDBPool> getBackupDataHosts() {
        waitIfChanging();
        return dataHosts2;
    }

    public Map<ERTable, Set<ERTable>> getErRelations() {
        waitIfChanging();
        return erRelations;
    }

    public Map<ERTable, Set<ERTable>> getBackupErRelations() {
        waitIfChanging();
        return erRelations2;
    }

    public FirewallConfig getFirewall() {
        waitIfChanging();
        return firewall;
    }

    public FirewallConfig getBackupFirewall() {
        waitIfChanging();
        return firewall2;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public long getReloadTime() {
        waitIfChanging();
        return reloadTime;
    }

    public long getRollbackTime() {
        waitIfChanging();
        return rollbackTime;
    }

    public boolean backDataHostWithoutWR() {
        waitIfChanging();
        return dataHostWithoutWR2;
    }

    public void reload(Map<String, UserConfig> newUsers, Map<String, SchemaConfig> newSchemas,
                       Map<String, PhysicalDBNode> newDataNodes, Map<String, PhysicalDBPool> newDataHosts,
                       Map<ERTable, Set<ERTable>> newErRelations, FirewallConfig newFirewall,
                       SystemVariables newSystemVariables, boolean newDataHostWithoutWR, boolean reloadAll,
                       final int loadAllMode) throws SQLNonTransientException {

        apply(newUsers, newSchemas, newDataNodes, newDataHosts, newErRelations, newFirewall,
                newSystemVariables, newDataHostWithoutWR, reloadAll, loadAllMode);
        this.reloadTime = TimeUtil.currentTimeMillis();
        this.status = reloadAll ? RELOAD_ALL : RELOAD;
    }

    private void calcDiffForMetaData(Map<String, SchemaConfig> newSchemas, Map<String, PhysicalDBNode> newDataNodes, boolean isLoadAll, int loadAllMode,
                                     List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables,
                                     List<String> delSchema, List<String> reloadSchema) {
        for (Map.Entry<String, SchemaConfig> schemaEntry : this.schemas.entrySet()) {
            String oldSchema = schemaEntry.getKey();
            SchemaConfig newSchemaConfig = newSchemas.get(oldSchema);
            if (newSchemaConfig == null) {
                delSchema.add(oldSchema);
            } else if (!isLoadAll || (loadAllMode & ManagerParseConfig.OPTR_MODE) == 0) { // reload @@config_all not contains -r
                SchemaConfig oldSchemaConfig = schemaEntry.getValue();
                if (!StringUtil.equalsWithEmpty(oldSchemaConfig.getDataNode(), newSchemaConfig.getDataNode())) {
                    delSchema.add(oldSchema);
                    reloadSchema.add(oldSchema);
                } else {
                    if (isLoadAll && newSchemaConfig.getDataNode() != null) { // reload config_all
                        //check data node and datahost change
                        List<String> strDataNodes = Collections.singletonList(newSchemaConfig.getDataNode());
                        if (isDataNodeChanged(strDataNodes, newDataNodes)) {
                            delSchema.add(oldSchema);
                            reloadSchema.add(oldSchema);
                            continue;
                        } else if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 && isDataHostChanged(strDataNodes, newDataNodes)) { // reload @@config_all not contains -s
                            delSchema.add(oldSchema);
                            reloadSchema.add(oldSchema);
                            continue;
                        }
                    }
                    calcTableDiffForMetaData(newDataNodes, loadAllMode, delTables, reloadTables, oldSchema, newSchemaConfig, oldSchemaConfig);
                }
            }
        }
        for (String newSchema : newSchemas.keySet()) {
            if ((loadAllMode & ManagerParseConfig.OPTR_MODE) != 0) { // reload @@config_all -r
                delSchema.add(newSchema);
                reloadSchema.add(newSchema);
            } else if (!this.schemas.containsKey(newSchema)) {
                reloadSchema.add(newSchema); // new added schema
            }
        }
    }

    private void calcTableDiffForMetaData(Map<String, PhysicalDBNode> newDataNodes, int loadAllMode, List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables, String oldSchema, SchemaConfig newSchemaConfig, SchemaConfig oldSchemaConfig) {
        for (Map.Entry<String, TableConfig> tableEntry : oldSchemaConfig.getTables().entrySet()) {
            String oldTable = tableEntry.getKey();
            TableConfig newTableConfig = newSchemaConfig.getTables().get(oldTable);
            if (newTableConfig == null) {
                delTables.add(new Pair<>(oldSchema, oldTable));
            } else {
                TableConfig oldTableConfig = tableEntry.getValue();
                if (!newTableConfig.getDataNodes().equals(oldTableConfig.getDataNodes()) ||
                        newTableConfig.getTableType() != oldTableConfig.getTableType()) {
                    Pair<String, String> table = new Pair<>(oldSchema, oldTable);
                    delTables.add(table);
                    reloadTables.add(table);
                } else if (isDataNodeChanged(oldTableConfig.getDataNodes(), newDataNodes)) {
                    Pair<String, String> table = new Pair<>(oldSchema, oldTable);
                    delTables.add(table);
                    reloadTables.add(table);
                } else if ((loadAllMode & ManagerParseConfig.OPTS_MODE) == 0 &&
                        isDataHostChanged(oldTableConfig.getDataNodes(), newDataNodes)) { // reload @@config_all not contains -s
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

    private boolean isDataNodeChanged(List<String> strDataNodes, Map<String, PhysicalDBNode> newDataNodes) {
        for (String strDataNode : strDataNodes) {
            PhysicalDBNode newDBNode = newDataNodes.get(strDataNode);
            PhysicalDBNode oldDBNode = dataNodes.get(strDataNode);
            if (!oldDBNode.getDatabase().equals(newDBNode.getDatabase()) ||
                    !oldDBNode.getDbPool().getHostName().equals(newDBNode.getDbPool().getHostName())) {
                return true;
            }
        }
        return false;
    }

    private boolean isDataHostChanged(List<String> strDataNodes, Map<String, PhysicalDBNode> newDataNodes) {
        for (String strDataNode : strDataNodes) {
            PhysicalDBPool newDBPool = newDataNodes.get(strDataNode).getDbPool();
            PhysicalDBPool oldDBPool = dataNodes.get(strDataNode).getDbPool();
            PhysicalDBPoolDiff diff = new PhysicalDBPoolDiff(oldDBPool, newDBPool);
            if (diff.getChangeType() != PhysicalDBPoolDiff.CHANGE_TYPE_NO) {
                return true;
            }
        }
        return false;
    }

    public boolean canRollbackAll() {
        return status == RELOAD_ALL && users2 != null && schemas2 != null && firewall2 != null && dataNodes2 != null && dataHosts2 != null;
    }

    public boolean canRollback() {
        return status == RELOAD && users2 != null && schemas2 != null && firewall2 != null;
    }

    public void rollback(Map<String, UserConfig> backupUsers, Map<String, SchemaConfig> backupSchemas,
                         Map<String, PhysicalDBNode> backupDataNodes, Map<String, PhysicalDBPool> backupDataHosts,
                         Map<ERTable, Set<ERTable>> backupErRelations, FirewallConfig backFirewall, boolean backDataHostWithoutWR) throws SQLNonTransientException {

        apply(backupUsers, backupSchemas, backupDataNodes, backupDataHosts, backupErRelations, backFirewall,
                DbleServer.getInstance().getSystemVariables(), backDataHostWithoutWR, status == RELOAD_ALL, ManagerParseConfig.OPTR_MODE);
        this.rollbackTime = TimeUtil.currentTimeMillis();
        this.status = ROLLBACK;
    }

    private void apply(Map<String, UserConfig> newUsers,
                       Map<String, SchemaConfig> newSchemas,
                       Map<String, PhysicalDBNode> newDataNodes,
                       Map<String, PhysicalDBPool> newDataHosts,
                       Map<ERTable, Set<ERTable>> newErRelations,
                       FirewallConfig newFirewall, SystemVariables newSystemVariables,
                       boolean newDataHostWithoutWR, boolean isLoadAll, final int loadAllMode) throws SQLNonTransientException {
        List<Pair<String, String>> delTables = new ArrayList<>();
        List<Pair<String, String>> reloadTables = new ArrayList<>();
        List<String> delSchema = new ArrayList<>();
        List<String> reloadSchema = new ArrayList<>();
        calcDiffForMetaData(newSchemas, newDataNodes, isLoadAll, loadAllMode, delTables, reloadTables, delSchema, reloadSchema);
        final ReentrantLock metaLock = DbleServer.getInstance().getTmManager().getMetaLock();
        metaLock.lock();
        this.changing = true;
        try {
            String checkResult = DbleServer.getInstance().getTmManager().metaCountCheck();
            if (checkResult != null) {
                LOGGER.warn(checkResult);
                throw new SQLNonTransientException(checkResult, "HY000", ErrorCode.ER_DOING_DDL);
            }
            // old data host
            // 1 stop heartbeat
            // 2 backup
            //--------------------------------------------
            if (isLoadAll) {
                Map<String, PhysicalDBPool> oldDataHosts = this.dataHosts;
                if (oldDataHosts != null) {
                    for (PhysicalDBPool oldDbPool : oldDataHosts.values()) {
                        if (oldDbPool != null) {
                            oldDbPool.stopHeartbeat();
                        }
                    }
                }
                this.dataNodes2 = this.dataNodes;
                this.dataHosts2 = this.dataHosts;
            }

            this.users2 = this.users;
            this.schemas2 = this.schemas;
            this.firewall2 = this.firewall;
            this.erRelations2 = this.erRelations;
            this.dataHostWithoutWR2 = this.dataHostWithoutWR;

            // new data host
            // 1 start heartbeat
            // 2 apply the configure
            //---------------------------------------------------
            if (isLoadAll) {
                if (newDataHosts != null) {
                    for (PhysicalDBPool newDbPool : newDataHosts.values()) {
                        if (newDbPool != null && !newDataHostWithoutWR) {
                            DbleServer.getInstance().saveDataHostIndex(newDbPool.getHostName(), newDbPool.getActiveIndex(),
                                    this.system.isUseZKSwitch() && DbleServer.getInstance().isUseZK());
                            newDbPool.startHeartbeat();
                        }
                    }
                }
                this.dataNodes = newDataNodes;
                this.dataHosts = newDataHosts;
                this.dataHostWithoutWR = newDataHostWithoutWR;
                DbleServer.getInstance().reloadSystemVariables(newSystemVariables);
                DbleServer.getInstance().getCacheService().reloadCache(newSystemVariables.isLowerCaseTableNames());
                DbleServer.getInstance().getRouterService().loadTableId2DataNodeCache(DbleServer.getInstance().getCacheService());
            }
            this.users = newUsers;
            this.schemas = newSchemas;
            this.firewall = newFirewall;
            this.erRelations = newErRelations;
            DbleServer.getInstance().getCacheService().clearCache();
            this.changing = false;
            if (!newDataHostWithoutWR) {
                reloadMetaData(delTables, reloadTables, delSchema, reloadSchema);
            }
        } finally {
            this.changing = false;
            metaLock.unlock();
        }
    }

    private void reloadMetaData(List<Pair<String, String>> delTables, List<Pair<String, String>> reloadTables, List<String> delSchema, List<String> reloadSchema) {
        if (delSchema.size() > 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("metadata will delete schema:" + StringUtil.join(delSchema, ","));
            }
            for (String schema : delSchema) {
                DbleServer.getInstance().getTmManager().getCatalogs().remove(schema);
            }
            LOGGER.info("metadata finished for deleted schemas");
        }
        if (delTables.size() > 0) {
            if (LOGGER.isDebugEnabled()) {
                String tables = changeTablesToString(delTables);
                LOGGER.debug("metadata will delete Tables:" + tables);
            }
            for (Pair<String, String> table : delTables) {
                DbleServer.getInstance().getTmManager().getCatalogs().get(table.getKey()).dropTable(table.getValue());
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
                    Set<String> tables = specifiedSchemas.get(table.getKey());
                    if (tables == null) {
                        tables = new HashSet<>();
                        specifiedSchemas.put(table.getKey(), tables);
                    }
                    tables.add(table.getValue());
                }
            }
            DbleServer.getInstance().reloadMetaData(this, specifiedSchemas);
            LOGGER.info("metadata finished for changes of schemas and tables");
        }
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

    public void simplyApply(Map<String, UserConfig> newUsers,
                            Map<String, SchemaConfig> newSchemas,
                            Map<String, PhysicalDBNode> newDataNodes,
                            Map<String, PhysicalDBPool> newDataHosts,
                            Map<ERTable, Set<ERTable>> newErRelations,
                            FirewallConfig newFirewall) {
        this.users = newUsers;
        this.schemas = newSchemas;
        this.dataNodes = newDataNodes;
        this.dataHosts = newDataHosts;
        this.firewall = newFirewall;
        this.erRelations = newErRelations;
    }


    /**
     * turned all the config into lowerCase config
     */
    public void reviseLowerCase() {

        //user schema
        for (UserConfig uc : users.values()) {
            if (uc.getPrivilegesConfig() != null) {
                uc.getPrivilegesConfig().changeMapToLowerCase();
                uc.changeMapToLowerCase();
            }
        }

        //dataNode
        for (PhysicalDBNode physicalDBNode : dataNodes.values()) {
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
            HashMap<ERTable, Set<ERTable>> newErMap = new HashMap<ERTable, Set<ERTable>>();
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
        //load global sequence
        if (system.getSequnceHandlerType() == SystemConfig.SEQUENCE_HANDLER_MYSQL) {
            IncrSequenceMySQLHandler.getInstance().load(DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
        }

        if (system.getSequnceHandlerType() == SystemConfig.SEQUENCE_HANDLER_LOCAL_TIME) {
            IncrSequenceTimeHandler.getInstance().load();
        }

        if (system.getSequnceHandlerType() == SystemConfig.SEQUENCE_HANDLER_ZK_DISTRIBUTED) {
            DistributedSequenceHandler.getInstance().load();
        }

        if (system.getSequnceHandlerType() == SystemConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT) {
            IncrSequenceZKHandler.getInstance().load(DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
        }
    }

    public void selfChecking0() throws ConfigException {
        // check 1.user's schemas are all existed in schema's conf
        // 2.schema's conf is not empty
        if (users == null || users.isEmpty()) {
            throw new ConfigException("SelfCheck### user all node is empty!");
        } else {
            for (UserConfig uc : users.values()) {
                if (uc == null) {
                    throw new ConfigException("SelfCheck### users node within the item is empty!");
                }
                if (!uc.isManager()) {
                    Set<String> authSchemas = uc.getSchemas();
                    if (authSchemas == null) {
                        throw new ConfigException("SelfCheck### user " + uc.getName() + "referred schemas is empty!");
                    }
                    for (String schema : authSchemas) {
                        if (!schemas.containsKey(schema)) {
                            String errMsg = "SelfCheck###  schema " + schema + " referred by user " + uc.getName() + " is not exist!";
                            throw new ConfigException(errMsg);
                        }
                    }
                }
            }
        }

        // check schema
        for (SchemaConfig sc : schemas.values()) {
            if (null == sc) {
                throw new ConfigException("SelfCheck### schema all node is empty!");
            } else {
                // check dataNode / dataHost
                if (this.dataNodes != null && this.dataHosts != null) {
                    Set<String> dataNodeNames = sc.getAllDataNodes();
                    for (String dataNodeName : dataNodeNames) {
                        PhysicalDBNode node = this.dataNodes.get(dataNodeName);
                        if (node == null) {
                            throw new ConfigException("SelfCheck### schema dataNode[" + dataNodeName + "] is empty!");
                        }
                    }
                }
            }
        }

    }

}


