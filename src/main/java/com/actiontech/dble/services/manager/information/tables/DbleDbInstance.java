/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.HeartBeat;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.services.manager.response.ShowHeartbeat;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DbleDbInstance extends ManagerWritableTable {

    public static final String TABLE_NAME = "dble_db_instance";

    private static final String COLUMN_NAME = "name";

    public static final String COLUMN_DB_GROUP = "db_group";

    private static final String COLUMN_ADDR = "addr";

    private static final String COLUMN_PORT = "port";

    private static final String COLUMN_USER = "user";

    private static final String COLUMN_PASSWORD_ENCRYPT = "password_encrypt";

    private static final String COLUMN_ENCRYPT_CONFIGURED = "encrypt_configured";

    private static final String COLUMN_PRIMARY = "primary";

    private static final String COLUMN_ACTIVE_CONN_COUNT = "active_conn_count";

    private static final String COLUMN_IDLE_CONN_COUNT = "idle_conn_count";

    private static final String COLUMN_READ_CONN_REQUEST = "read_conn_request";

    private static final String COLUMN_WRITE_CONN_REQUEST = "write_conn_request";

    private static final String COLUMN_DISABLED = "disabled";

    private static final String COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP = "last_heartbeat_ack_timestamp";

    private static final String COLUMN_LAST_HEARTBEAT_ACK = "last_heartbeat_ack";

    private static final String COLUMN_HEARTBEAT_STATUS = "heartbeat_status";

    private static final String COLUMN_HEARTBEAT_FAILURE_IN_LAST_5MIN = "heartbeat_failure_in_last_5min";

    private static final String COLUMN_MIN_CONN_COUNT = "min_conn_count";

    private static final String COLUMN_MAX_CONN_COUNT = "max_conn_count";

    private static final String COLUMN_READ_WEIGHT = "read_weight";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_CONNECTION_TIMEOUT = "connection_timeout";

    private static final String COLUMN_CONNECTION_HEARTBEAT_TIMEOUT = "connection_heartbeat_timeout";

    private static final String COLUMN_TEST_ON_CREATE = "test_on_create";

    private static final String COLUMN_TEST_ON_BORROW = "test_on_borrow";

    private static final String COLUMN_TEST_ON_RETURN = "test_on_return";

    private static final String COLUMN_TEST_WHILE_IDLE = "test_while_idle";

    private static final String COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS = "time_between_eviction_runs_millis";

    private static final String COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS = "evictor_shutdown_timeout_millis";

    private static final String COLUMN_IDLE_TIMEOUT = "idle_timeout";

    private static final String COLUMN_HEARTBEAT_PERIOD_MILLIS = "heartbeat_period_millis";

    public DbleDbInstance() {
        super(TABLE_NAME, 30);
        setNotWritableColumnSet(COLUMN_ACTIVE_CONN_COUNT, COLUMN_IDLE_CONN_COUNT, COLUMN_READ_CONN_REQUEST, COLUMN_WRITE_CONN_REQUEST,
                COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP, COLUMN_LAST_HEARTBEAT_ACK, COLUMN_HEARTBEAT_STATUS, COLUMN_HEARTBEAT_FAILURE_IN_LAST_5MIN);

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.DB_XML;
        this.setXmlFilePath(path);

    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_GROUP, new ColumnMeta(COLUMN_DB_GROUP, "varchar(64)", false, true));
        columnsType.put(COLUMN_DB_GROUP, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ADDR, new ColumnMeta(COLUMN_ADDR, "varchar(64)", false));
        columnsType.put(COLUMN_ADDR, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PORT, new ColumnMeta(COLUMN_PORT, "int(11)", false));
        columnsType.put(COLUMN_PORT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(64)", false));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PASSWORD_ENCRYPT, new ColumnMeta(COLUMN_PASSWORD_ENCRYPT, "varchar(256)", false));
        columnsType.put(COLUMN_PASSWORD_ENCRYPT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ENCRYPT_CONFIGURED, new ColumnMeta(COLUMN_ENCRYPT_CONFIGURED, "varchar(5)", true, "true"));
        columnsType.put(COLUMN_ENCRYPT_CONFIGURED, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PRIMARY, new ColumnMeta(COLUMN_PRIMARY, "varchar(5)", false));
        columnsType.put(COLUMN_PRIMARY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ACTIVE_CONN_COUNT, new ColumnMeta(COLUMN_ACTIVE_CONN_COUNT, "int(11)", true, "0"));
        columnsType.put(COLUMN_ACTIVE_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_IDLE_CONN_COUNT, new ColumnMeta(COLUMN_IDLE_CONN_COUNT, "int(11)", true));
        columnsType.put(COLUMN_IDLE_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_READ_CONN_REQUEST, new ColumnMeta(COLUMN_READ_CONN_REQUEST, "int(11)", true, "0"));
        columnsType.put(COLUMN_READ_CONN_REQUEST, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_WRITE_CONN_REQUEST, new ColumnMeta(COLUMN_WRITE_CONN_REQUEST, "int(11)", true, "0"));
        columnsType.put(COLUMN_WRITE_CONN_REQUEST, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DISABLED, new ColumnMeta(COLUMN_DISABLED, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_DISABLED, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP, new ColumnMeta(COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP, "varchar(64)", true));
        columnsType.put(COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_HEARTBEAT_ACK, new ColumnMeta(COLUMN_LAST_HEARTBEAT_ACK, "varchar(32)", true));
        columnsType.put(COLUMN_LAST_HEARTBEAT_ACK, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HEARTBEAT_STATUS, new ColumnMeta(COLUMN_HEARTBEAT_STATUS, "varchar(32)", true));
        columnsType.put(COLUMN_HEARTBEAT_STATUS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HEARTBEAT_FAILURE_IN_LAST_5MIN, new ColumnMeta(COLUMN_HEARTBEAT_FAILURE_IN_LAST_5MIN, "int(11)", true));
        columnsType.put(COLUMN_HEARTBEAT_FAILURE_IN_LAST_5MIN, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_MIN_CONN_COUNT, new ColumnMeta(COLUMN_MIN_CONN_COUNT, "int(11)", false));
        columnsType.put(COLUMN_MIN_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_MAX_CONN_COUNT, new ColumnMeta(COLUMN_MAX_CONN_COUNT, "int(11)", false));
        columnsType.put(COLUMN_MAX_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_READ_WEIGHT, new ColumnMeta(COLUMN_READ_WEIGHT, "int(11)", true, "0"));
        columnsType.put(COLUMN_READ_WEIGHT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONNECTION_TIMEOUT, new ColumnMeta(COLUMN_CONNECTION_TIMEOUT, "int(11)", true, String.valueOf(PoolConfig.CONNECTION_TIMEOUT)));
        columnsType.put(COLUMN_CONNECTION_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, new ColumnMeta(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, "int(11)", true, String.valueOf(PoolConfig.CON_HEARTBEAT_TIMEOUT)));
        columnsType.put(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TEST_ON_CREATE, new ColumnMeta(COLUMN_TEST_ON_CREATE, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_TEST_ON_CREATE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TEST_ON_BORROW, new ColumnMeta(COLUMN_TEST_ON_BORROW, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_TEST_ON_BORROW, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TEST_ON_RETURN, new ColumnMeta(COLUMN_TEST_ON_RETURN, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_TEST_ON_RETURN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TEST_WHILE_IDLE, new ColumnMeta(COLUMN_TEST_WHILE_IDLE, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_TEST_WHILE_IDLE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, new ColumnMeta(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, "int(11)", true, String.valueOf(PoolConfig.HOUSEKEEPING_PERIOD_MS)));
        columnsType.put(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, new ColumnMeta(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, "int(11)", true, String.valueOf(PoolConfig.DEFAULT_SHUTDOWN_TIMEOUT)));
        columnsType.put(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_IDLE_TIMEOUT, new ColumnMeta(COLUMN_IDLE_TIMEOUT, "int(11)", true, String.valueOf(PoolConfig.DEFAULT_IDLE_TIMEOUT)));
        columnsType.put(COLUMN_IDLE_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_HEARTBEAT_PERIOD_MILLIS, new ColumnMeta(COLUMN_HEARTBEAT_PERIOD_MILLIS, "int(11)", true, String.valueOf(PoolConfig.DEFAULT_HEARTBEAT_PERIOD)));
        columnsType.put(COLUMN_HEARTBEAT_PERIOD_MILLIS, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Set<String> nameSet = Sets.newHashSet();
        Map<String, PhysicalDbGroup> dbGroups = DbleServer.getInstance().getConfig().getDbGroups();
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        dbGroups.forEach((key, dbGroup) -> dbGroup.getDbInstances(true).forEach(dbInstance -> {
            if (nameSet.add(dbInstance.getName() + "-" + dbGroup.getGroupName())) {
                LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                DbInstanceConfig dbInstanceConfig = dbInstance.getConfig();
                MySQLHeartbeat heartbeat = dbInstance.getHeartbeat();
                PoolConfig poolConfig = dbInstanceConfig.getPoolConfig();
                map.put(COLUMN_NAME, dbInstance.getName());
                map.put(COLUMN_DB_GROUP, dbGroup.getGroupName());
                map.put(COLUMN_ADDR, dbInstanceConfig.getIp());
                map.put(COLUMN_PORT, String.valueOf(dbInstanceConfig.getPort()));
                map.put(COLUMN_USER, dbInstanceConfig.getUser());
                map.put(COLUMN_PASSWORD_ENCRYPT, getPasswordEncrypt(dbInstanceConfig.getInstanceName(), dbInstanceConfig.getUser(), dbInstanceConfig.getPassword()));
                map.put(COLUMN_ENCRYPT_CONFIGURED, String.valueOf(dbInstanceConfig.isUsingDecrypt()));
                map.put(COLUMN_PRIMARY, String.valueOf(dbInstanceConfig.isPrimary()));
                map.put(COLUMN_ACTIVE_CONN_COUNT, String.valueOf(dbInstance.getActiveConnections()));
                map.put(COLUMN_IDLE_CONN_COUNT, String.valueOf(dbInstance.getIdleConnections()));
                map.put(COLUMN_READ_CONN_REQUEST, String.valueOf(dbInstance.getCount(true)));
                map.put(COLUMN_WRITE_CONN_REQUEST, String.valueOf(dbInstance.getCount(false)));
                map.put(COLUMN_DISABLED, String.valueOf(dbInstanceConfig.isDisabled()));
                map.put(COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP, heartbeat.getLastActiveTime());
                map.put(COLUMN_LAST_HEARTBEAT_ACK, ShowHeartbeat.getRdCode(heartbeat.getStatus()));
                map.put(COLUMN_HEARTBEAT_STATUS, heartbeat.isChecking() ? MySQLHeartbeat.CHECK_STATUS_CHECKING : MySQLHeartbeat.CHECK_STATUS_IDLE);
                map.put(COLUMN_HEARTBEAT_FAILURE_IN_LAST_5MIN, String.valueOf(heartbeat.getErrorTimeInLast5MinCount()));
                map.put(COLUMN_MIN_CONN_COUNT, String.valueOf(dbInstanceConfig.getMinCon()));
                map.put(COLUMN_MAX_CONN_COUNT, String.valueOf(dbInstanceConfig.getMaxCon()));
                map.put(COLUMN_READ_WEIGHT, String.valueOf(dbInstanceConfig.getReadWeight()));
                map.put(COLUMN_ID, String.valueOf(dbInstanceConfig.getId()));
                //pool config
                map.put(COLUMN_CONNECTION_TIMEOUT, String.valueOf(poolConfig.getConnectionTimeout()));
                map.put(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, String.valueOf(poolConfig.getConnectionHeartbeatTimeout()));
                map.put(COLUMN_TEST_ON_CREATE, String.valueOf(poolConfig.getTestOnCreate()));
                map.put(COLUMN_TEST_ON_BORROW, String.valueOf(poolConfig.getTestOnBorrow()));
                map.put(COLUMN_TEST_ON_RETURN, String.valueOf(poolConfig.getTestOnReturn()));
                map.put(COLUMN_TEST_WHILE_IDLE, String.valueOf(poolConfig.getTestWhileIdle()));
                map.put(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, String.valueOf(poolConfig.getTimeBetweenEvictionRunsMillis()));
                map.put(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, String.valueOf(poolConfig.getEvictorShutdownTimeoutMillis()));
                map.put(COLUMN_IDLE_TIMEOUT, String.valueOf(poolConfig.getIdleTimeout()));
                map.put(COLUMN_HEARTBEAT_PERIOD_MILLIS, String.valueOf(poolConfig.getHeartbeatPeriodMillis()));
                rowList.add(map);
            }
        }));
        return rowList;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        return insertOrUpdate(rows, true);
    }

    private int insertOrUpdate(List<LinkedHashMap<String, String>> rows, boolean insertFlag) throws SQLException {
        decryptPassword(rows, insertFlag);
        final int size = rows.size();
        XmlProcessBase xmlProcess = new XmlProcessBase();
        DbGroups dbs = transformRow(xmlProcess, null, rows);
        //check logical foreign key
        if (!rows.isEmpty()) {
            String msg = String.format("Cannot add or update a child row: a logical foreign key '%s' constraint fails", COLUMN_DB_GROUP);
            throw new SQLException(msg, "42S22", ErrorCode.ER_NO_REFERENCED_ROW_2);
        }
        //check primary
        for (DBGroup group : dbs.getDbGroup()) {
            long primaryCount = group.getDbInstance().stream().filter(dbInstance -> null != dbInstance.getPrimary() && dbInstance.getPrimary()).count();
            if (primaryCount != 1) {
                String msg = String.format("dbGroup[%s] has one and only one primary instance", group.getName());
                throw new ConfigException(msg);
            }
        }
        //check connection
        checkInstanceConnection(dbs);
        //remove temp row
        DbleDbGroup dbleDbGroup = (DbleDbGroup) ManagerSchemaInfo.getInstance().getTables().get(DbleDbGroup.TABLE_NAME);
        for (DBGroup dbGroup : dbs.getDbGroup()) {
            dbleDbGroup.getTempRowList().removeIf(group -> StringUtil.equals(group.get(DbleDbGroup.COLUMN_NAME), dbGroup.getName()));
        }
        dbs.encryptPassword();
        //write to configuration
        xmlProcess.writeObjToXml(dbs, getXmlFilePath(), "db");
        return size;
    }


    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        affectPks.forEach(affectPk -> affectPk.putAll(values));
        return insertOrUpdate(Lists.newArrayList(affectPks), false);
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        DbGroups dbGroups = transformRow(xmlProcess, null, null);
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            for (DBGroup dbGroup : dbGroups.getDbGroup()) {
                dbGroup.getDbInstance().removeIf(dbInstance -> StringUtil.equals(affectPk.get(COLUMN_DB_GROUP), dbGroup.getName()) && StringUtil.equals(affectPk.get(COLUMN_NAME), dbInstance.getName()));
            }
        }
        for (DBGroup dbGroup : dbGroups.getDbGroup()) {
            boolean existPrimary = dbGroup.getDbInstance().stream().anyMatch(dbInstance -> null != dbInstance.getPrimary() && dbInstance.getPrimary());
            if (!existPrimary && !dbGroup.getDbInstance().isEmpty()) {
                throw new SQLException("Table dble_db_group[" + dbGroup.getName() + "] needs to retain a primary dbInstance", "42S22", ErrorCode.ER_YES);
            }
        }
        Set<DBGroup> removeDBGroupSet = dbGroups.getDbGroup().stream().filter(dbGroup -> dbGroup.getDbInstance().isEmpty()).collect(Collectors.toSet());
        //check remove empty instance
        checkDeleteRule(removeDBGroupSet);
        //remove empty instance
        dbGroups.getDbGroup().removeIf(dbGroup -> dbGroup.getDbInstance().isEmpty());
        dbGroups.encryptPassword();
        //write to configuration
        xmlProcess.writeObjToXml(dbGroups, getXmlFilePath(), "db");
        return affectPks.size();
    }

    private void checkDeleteRule(Set<DBGroup> removeDBGroupSet) {
        for (DBGroup dbGroup : removeDBGroupSet) {
            //check user-group
            DbleRwSplitEntry dbleRwSplitEntry = (DbleRwSplitEntry) ManagerSchemaInfo.getInstance().getTables().get(DbleRwSplitEntry.TABLE_NAME);
            boolean existUser = dbleRwSplitEntry.getRows().stream().anyMatch(entry -> entry.get(DbleRwSplitEntry.COLUMN_DB_GROUP).equals(dbGroup.getName()));
            if (existUser) {
                throw new ConfigException("Cannot delete or update a parent row: a foreign key constraint fails `dble_db_user`(`db_group`) REFERENCES `dble_db_group`(`name`)");
            }
            //check sharding_node-group
            DbleShardingNode dbleShardingNode = (DbleShardingNode) ManagerSchemaInfo.getInstance().getTables().get(DbleShardingNode.TABLE_NAME);
            boolean existShardingNode = dbleShardingNode.getRows().stream().anyMatch(entry -> entry.get(DbleShardingNode.COLUMN_DB_GROUP).equals(dbGroup.getName()));
            if (existShardingNode) {
                throw new ConfigException("Cannot delete or update a parent row: a foreign key constraint fails `dble_sharding_node`(`db_group`) REFERENCES `dble_db_group`(`name`)");
            }
        }
    }

    private void decryptPassword(List<LinkedHashMap<String, String>> rows, boolean insertFlag) {
        for (LinkedHashMap<String, String> row : rows) {
            checkBooleanVal(row);
            if ((insertFlag && Boolean.parseBoolean(row.get(COLUMN_ENCRYPT_CONFIGURED))) || !insertFlag) {
                row.put(COLUMN_PASSWORD_ENCRYPT, DecryptUtil.dbHostDecrypt(true, row.get(COLUMN_NAME),
                        row.get(COLUMN_USER), row.get(COLUMN_PASSWORD_ENCRYPT)));
            }
        }
    }

    private void checkBooleanVal(LinkedHashMap<String, String> row) {
        Set<String> keySet = Sets.newHashSet(COLUMN_ENCRYPT_CONFIGURED, COLUMN_PRIMARY, COLUMN_DISABLED, COLUMN_TEST_ON_CREATE, COLUMN_TEST_ON_BORROW, COLUMN_TEST_ON_RETURN,
                COLUMN_TEST_WHILE_IDLE);
        for (String key : keySet) {
            if (row.containsKey(key) && !StringUtil.isEmpty(row.get(key))) {
                String value = row.get(key);
                if (!StringUtil.equalsIgnoreCase(value, Boolean.FALSE.toString()) && !StringUtil.equalsIgnoreCase(value, Boolean.TRUE.toString())) {
                    throw new ConfigException("Column '" + key + "' values only support 'false' or 'true'.");
                }
            }
        }
    }


    public DbGroups transformRow(XmlProcessBase xmlProcess, List<LinkedHashMap<String, String>> changeDbGroupRows, List<LinkedHashMap<String, String>> changeDbInstanceRows) {
        if (null == xmlProcess) {
            return null;
        }
        DbGroups dbs = null;
        try {
            xmlProcess.addParseClass(DbGroups.class);
            xmlProcess.initJaxbClass();
            dbs = (DbGroups) xmlProcess.baseParseXmlToBean(ConfigFileName.DB_XML);
        } catch (JAXBException | XMLStreamException e) {
            e.printStackTrace();
        }
        if (null == dbs) {
            throw new ConfigException("configuration is empty");
        }
        for (DBGroup dbGroup : dbs.getDbGroup()) {
            for (DBInstance dbInstance : dbGroup.getDbInstance()) {
                String usingDecrypt = dbInstance.getUsingDecrypt();
                if (!StringUtil.isEmpty(usingDecrypt) && Boolean.parseBoolean(usingDecrypt)) {
                    dbInstance.setPassword(DecryptUtil.dbHostDecrypt(true, dbInstance.getName(), dbInstance.getUser(), dbInstance.getPassword()));
                }
            }
        }
        DbleDbGroup dbleDbGroup = (DbleDbGroup) ManagerSchemaInfo.getInstance().getTables().get(DbleDbGroup.TABLE_NAME);
        List<LinkedHashMap<String, String>> dbGroupRowList = dbleDbGroup.getRows();
        for (LinkedHashMap<String, String> dbGroupRow : dbGroupRowList) {
            DBGroup dbGroup = initDBGroup(dbGroupRow, changeDbGroupRows, dbs);
            initDBInstance(dbGroupRow, changeDbInstanceRows, dbGroup);
            dbs.addDbGroup(dbGroup);
        }
        dbs.getDbGroup().removeIf(dbGroup -> null == dbGroup.getDbInstance() || dbGroup.getDbInstance().isEmpty());
        return dbs;
    }

    private void initDBInstance(LinkedHashMap<String, String> dbGroupRow, List<LinkedHashMap<String, String>> changeDbInstanceRows, DBGroup dbGroup) {
        if (null == changeDbInstanceRows) {
            return;
        }
        List<LinkedHashMap<String, String>> instanceRowList = changeDbInstanceRows.stream().filter(row -> StringUtil.equals(dbGroupRow.get(DbleDbGroup.COLUMN_NAME), row.get(COLUMN_DB_GROUP))).collect(Collectors.toList());
        if (!instanceRowList.isEmpty()) {
            instanceRowList.forEach(instanceRowMap -> {
                List<Property> propertyList = Lists.newArrayList();
                String testOnCreate = instanceRowMap.get(COLUMN_TEST_ON_CREATE);
                if (!StringUtil.isEmpty(testOnCreate)) {
                    propertyList.add(new Property(testOnCreate, "testOnCreate"));
                }
                String testOnBorrow = instanceRowMap.get(COLUMN_TEST_ON_BORROW);
                if (!StringUtil.isEmpty(testOnBorrow)) {
                    propertyList.add(new Property(testOnBorrow, "testOnBorrow"));
                }
                String testOnReturn = instanceRowMap.get(COLUMN_TEST_ON_RETURN);
                if (!StringUtil.isEmpty(testOnReturn)) {
                    propertyList.add(new Property(testOnReturn, "testOnReturn"));
                }
                String testWhileIdle = instanceRowMap.get(COLUMN_TEST_WHILE_IDLE);
                if (!StringUtil.isEmpty(testWhileIdle)) {
                    propertyList.add(new Property(testWhileIdle, "testWhileIdle"));
                }
                String connectionTimeout = instanceRowMap.get(COLUMN_CONNECTION_TIMEOUT);
                if (!StringUtil.isEmpty(connectionTimeout)) {
                    propertyList.add(new Property(connectionTimeout, "connectionTimeout"));
                }
                String connectionHeartbeatTimeout = instanceRowMap.get(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT);
                if (!StringUtil.isEmpty(connectionHeartbeatTimeout)) {
                    propertyList.add(new Property(connectionHeartbeatTimeout, "connectionHeartbeatTimeout"));
                }
                String timeBetweenEvictionRunsMillis = instanceRowMap.get(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
                if (!StringUtil.isEmpty(timeBetweenEvictionRunsMillis)) {
                    propertyList.add(new Property(timeBetweenEvictionRunsMillis, "timeBetweenEvictionRunsMillis"));
                }
                String idleTimeout = instanceRowMap.get(COLUMN_IDLE_TIMEOUT);
                if (!StringUtil.isEmpty(idleTimeout)) {
                    propertyList.add(new Property(idleTimeout, "idleTimeout"));
                }
                String heartbeatPeriodMillis = instanceRowMap.get(COLUMN_HEARTBEAT_PERIOD_MILLIS);
                if (!StringUtil.isEmpty(heartbeatPeriodMillis)) {
                    propertyList.add(new Property(heartbeatPeriodMillis, "heartbeatPeriodMillis"));
                }
                String evictorShutdownTimeoutMillis = instanceRowMap.get(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS);
                if (!StringUtil.isEmpty(evictorShutdownTimeoutMillis)) {
                    propertyList.add(new Property(evictorShutdownTimeoutMillis, "evictorShutdownTimeoutMillis"));
                }
                Integer maxCon = StringUtil.isEmpty(instanceRowMap.get(COLUMN_MAX_CONN_COUNT)) ? null : Integer.valueOf(instanceRowMap.get(COLUMN_MAX_CONN_COUNT));
                Integer minCon = StringUtil.isEmpty(instanceRowMap.get(COLUMN_MIN_CONN_COUNT)) ? null : Integer.valueOf(instanceRowMap.get(COLUMN_MIN_CONN_COUNT));
                Boolean primary = StringUtil.isEmpty(instanceRowMap.get(COLUMN_PRIMARY)) ? null : Boolean.valueOf(instanceRowMap.get(COLUMN_PRIMARY));
                DBInstance dbInstance = new DBInstance(instanceRowMap.get(COLUMN_NAME), instanceRowMap.get(COLUMN_ADDR) + ":" + instanceRowMap.get(COLUMN_PORT),
                        instanceRowMap.get(COLUMN_PASSWORD_ENCRYPT), instanceRowMap.get(COLUMN_USER), maxCon, minCon, instanceRowMap.get(COLUMN_DISABLED),
                        instanceRowMap.get(COLUMN_ID), instanceRowMap.get(COLUMN_READ_WEIGHT), primary, propertyList, instanceRowMap.get(COLUMN_ENCRYPT_CONFIGURED));
                if (dbGroup.getDbInstance().stream().anyMatch(instance -> StringUtil.equals(instance.getName(), dbInstance.getName()))) {
                    dbGroup.getDbInstance().removeIf(instance -> StringUtil.equals(instance.getName(), dbInstance.getName()));
                }
                dbGroup.addDbInstance(dbInstance);
                changeDbInstanceRows.remove(instanceRowMap);
            });
        }
    }

    private DBGroup initDBGroup(LinkedHashMap<String, String> dbGroupRow, List<LinkedHashMap<String, String>> changeDbGroupRows, DbGroups dbs) {
        changeDbGroupRows = null != changeDbGroupRows ? changeDbGroupRows : Lists.newArrayList();
        LinkedHashMap<String, String> finalDbGroupRow = dbGroupRow;
        Optional<LinkedHashMap<String, String>> changeDbGroupRow = changeDbGroupRows.stream().filter(changeGroupRow -> StringUtil.equals(changeGroupRow.get(DbleDbGroup.COLUMN_NAME), finalDbGroupRow.get(COLUMN_NAME))).findFirst();
        if (changeDbGroupRow.isPresent()) {
            dbGroupRow = changeDbGroupRow.get();
        }
        Integer timeout = StringUtil.isEmpty(dbGroupRow.get(DbleDbGroup.COLUMN_HEARTBEAT_TIMEOUT)) ? null : Integer.valueOf(dbGroupRow.get(DbleDbGroup.COLUMN_HEARTBEAT_TIMEOUT));
        Integer errorRetryCount = StringUtil.isEmpty(dbGroupRow.get(DbleDbGroup.COLUMN_HEARTBEAT_RETRY)) ? null : Integer.valueOf(dbGroupRow.get(DbleDbGroup.COLUMN_HEARTBEAT_RETRY));
        Integer rwSplitMode = StringUtil.isEmpty(dbGroupRow.get(DbleDbGroup.COLUMN_RW_SPLIT_MODE)) ? null : Integer.valueOf(dbGroupRow.get(DbleDbGroup.COLUMN_RW_SPLIT_MODE));
        Integer delayThreshold = StringUtil.isEmpty(dbGroupRow.get(DbleDbGroup.COLUMN_DELAY_THRESHOLD)) ? null : Integer.valueOf(dbGroupRow.get(DbleDbGroup.COLUMN_DELAY_THRESHOLD));
        HeartBeat heartBeat = new HeartBeat(dbGroupRow.get(DbleDbGroup.COLUMN_HEARTBEAT_STMT), timeout, errorRetryCount);
        DBGroup dbGroup = new DBGroup(rwSplitMode, dbGroupRow.get(DbleDbGroup.COLUMN_NAME), delayThreshold, dbGroupRow.get(DbleDbGroup.COLUMN_DISABLE_HA), heartBeat);
        Optional<DBGroup> first = dbs.getDbGroup().stream().filter(group -> StringUtil.equals(group.getName(), dbGroup.getName())).findFirst();
        if (first.isPresent()) {
            DBGroup oldDbGroup = first.get();
            dbs.getDbGroup().removeIf(group -> StringUtil.equals(group.getName(), dbGroup.getName()));
            dbGroup.addAllDbInstance(oldDbGroup.getDbInstance());
        }
        return dbGroup;
    }

    private void checkInstanceConnection(DbGroups dbs) {
        try {
            for (DBGroup dbGroup : dbs.getDbGroup()) {
                List<DBInstance> dbInstanceList = dbGroup.getDbInstance();
                DbInstanceConfig tmpDbInstanceConfig = null;
                List<DbInstanceConfig> dbInstanceConfigList = Lists.newArrayList();
                for (DBInstance dbInstance : dbInstanceList) {
                    String url = dbInstance.getUrl();
                    int colonIndex = url.indexOf(':');
                    String ip = url.substring(0, colonIndex).trim();
                    int port = Integer.parseInt(url.substring(colonIndex + 1).trim());
                    boolean disabled = !StringUtil.isEmpty(dbInstance.getDisabled()) && Boolean.parseBoolean(dbInstance.getDisabled());
                    int readWeight = StringUtil.isEmpty(dbInstance.getReadWeight()) ? 0 : Integer.parseInt(dbInstance.getReadWeight());
                    boolean usingDecrypt = !StringUtil.isEmpty(dbInstance.getUsingDecrypt()) && Boolean.parseBoolean(dbInstance.getUsingDecrypt());
                    List<Property> propertyList = dbInstance.getProperty();
                    PoolConfig poolConfig = null;
                    if (!propertyList.isEmpty()) {
                        Map<String, String> propertyMap = propertyList.stream().collect(Collectors.toMap(Property::getName, Property::getValue));
                        poolConfig = new PoolConfig();
                        ParameterMapping.mapping(poolConfig, propertyMap, null);
                    }
                    String password = dbInstance.getPassword();
                    boolean primary = (null == dbInstance.getPrimary() ? false : dbInstance.getPrimary());
                    if (primary) {
                        tmpDbInstanceConfig = new DbInstanceConfig(dbInstance.getName(), ip, port, url, dbInstance.getUser(), password, readWeight, dbInstance.getId(),
                                disabled, true, dbInstance.getMaxCon(), dbInstance.getMinCon(), poolConfig, usingDecrypt);
                    } else {
                        dbInstanceConfigList.add(new DbInstanceConfig(dbInstance.getName(), ip, port, url, dbInstance.getUser(), password, readWeight, dbInstance.getId(),
                                disabled, false, dbInstance.getMaxCon(), dbInstance.getMinCon(), poolConfig, usingDecrypt));
                    }
                }
                boolean disableHA = !StringUtil.isEmpty(dbGroup.getDisableHA()) && Boolean.parseBoolean(dbGroup.getDisableHA());
                DbInstanceConfig[] dbInstanceConfigs = dbInstanceConfigList.isEmpty() ? new DbInstanceConfig[0] : dbInstanceConfigList.toArray(new DbInstanceConfig[0]);
                DbGroupConfig dbGroupConf = new DbGroupConfig(dbGroup.getName(), tmpDbInstanceConfig, dbInstanceConfigs, dbGroup.getDelayThreshold(), disableHA);
                //test connection
                PhysicalDbInstance writeSource = new MySQLInstance(dbGroupConf.getWriteInstanceConfig(), dbGroupConf, true);
                boolean isConnected = writeSource.testConnection();
                if (!isConnected) {
                    throw new ConfigException("Can't connect to [" + dbGroupConf.getName() + "," + writeSource.getName() + "," + writeSource.getConfig().getUrl() + "]");
                }
                for (DbInstanceConfig readInstanceConfig : dbGroupConf.getReadInstanceConfigs()) {
                    MySQLInstance readInstance = new MySQLInstance(readInstanceConfig, dbGroupConf, false);
                    isConnected = readInstance.testConnection();
                    if (!isConnected) {
                        throw new ConfigException("Can't connect to [" + dbGroupConf.getName() + "," + readInstance.getName() + "," + readInstance.getConfig().getUrl() + "]");
                    }
                }
            }
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new ConfigException(e);
        }
    }


    public static String getPasswordEncrypt(String instanceName, String name, String password) {
        try {
            return DecryptUtil.encrypt("1:" + instanceName + ":" + name + ":" + password);
        } catch (Exception e) {
            return "******";
        }
    }

}
