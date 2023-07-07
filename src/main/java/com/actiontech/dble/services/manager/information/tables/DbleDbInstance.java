/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.converter.DBConverter;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.IntegerUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.File;
import java.nio.charset.Charset;
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

    private static final String COLUMN_DATABASE_TYPE = "database_type";

    private static final String COLUMN_DB_DISTRICT = "db_district";

    private static final String COLUMN_DB_DATA_CENTER = "db_data_center";

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

    private static final String COLUMN_FLOW_HIGH_LEVEL = "flow_high_level";

    private static final String COLUMN_FLOW_LOW_LEVEL = "flow_low_level";

    public DbleDbInstance() {
        super(TABLE_NAME, 36);
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

        columns.put(COLUMN_DATABASE_TYPE, new ColumnMeta(COLUMN_DATABASE_TYPE, "varchar(11)", true, "mysql"));
        columnsType.put(COLUMN_DATABASE_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_DISTRICT, new ColumnMeta(COLUMN_DB_DISTRICT, "varchar(11)", true, null));
        columnsType.put(COLUMN_DB_DISTRICT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_DATA_CENTER, new ColumnMeta(COLUMN_DB_DATA_CENTER, "varchar(11)", true, null));
        columnsType.put(COLUMN_DB_DATA_CENTER, Fields.FIELD_TYPE_VAR_STRING);

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


        columns.put(COLUMN_FLOW_HIGH_LEVEL, new ColumnMeta(COLUMN_FLOW_HIGH_LEVEL, "int(11)", true, String.valueOf(SystemConfig.FLOW_CONTROL_HIGH_LEVEL)));
        columnsType.put(COLUMN_FLOW_HIGH_LEVEL, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_FLOW_LOW_LEVEL, new ColumnMeta(COLUMN_FLOW_LOW_LEVEL, "int(11)", true, String.valueOf(SystemConfig.FLOW_CONTROL_LOW_LEVEL)));
        columnsType.put(COLUMN_FLOW_LOW_LEVEL, Fields.FIELD_TYPE_LONG);
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
                map.put(COLUMN_PRIMARY, String.valueOf(!dbInstance.isReadInstance()));
                map.put(COLUMN_ACTIVE_CONN_COUNT, String.valueOf(dbInstance.getActiveConnections()));
                map.put(COLUMN_IDLE_CONN_COUNT, String.valueOf(dbInstance.getIdleConnections()));
                map.put(COLUMN_READ_CONN_REQUEST, String.valueOf(dbInstance.getCount(true)));
                map.put(COLUMN_WRITE_CONN_REQUEST, String.valueOf(dbInstance.getCount(false)));
                map.put(COLUMN_DISABLED, String.valueOf(dbInstance.isDisabled()));
                map.put(COLUMN_DATABASE_TYPE, String.valueOf(dbInstanceConfig.getDataBaseType()).toLowerCase());
                map.put(COLUMN_DB_DISTRICT, dbInstanceConfig.getDbDistrict());
                map.put(COLUMN_DB_DATA_CENTER, dbInstanceConfig.getDbDataCenter());
                map.put(COLUMN_LAST_HEARTBEAT_ACK_TIMESTAMP, heartbeat.getLastActiveTime());
                map.put(COLUMN_LAST_HEARTBEAT_ACK, heartbeat.getStatus().toString());
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
                map.put(COLUMN_FLOW_HIGH_LEVEL, String.valueOf(poolConfig.getFlowHighLevel()));
                map.put(COLUMN_FLOW_LOW_LEVEL, String.valueOf(poolConfig.getFlowLowLevel()));

                rowList.add(map);
            }
        }));
        return rowList;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        checkInsertDbInstanceType(rows);
        List<DBInstance> dbInstanceList = rows.stream().map(this::transformRowToDBInstance).collect(Collectors.toList());

        DBConverter dbConverter = new DBConverter();
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        DbGroups dbGroups = dbConverter.dbJsonToBean(dbConfig, false);

        DbleDbGroup dbleDbGroup = (DbleDbGroup) ManagerSchemaInfo.getInstance().getTables().get(DbleDbGroup.TABLE_NAME);
        List<LinkedHashMap<String, String>> tempDbGroupMapList = dbleDbGroup.getTempRowList();
        List<DBGroup> tempDbGroupList = tempDbGroupMapList.stream().map(dbleDbGroup::transformRowToDBGroup).collect(Collectors.toList());

        for (DBInstance dbInstance : dbInstanceList) {
            Optional<DBGroup> dbGroupOp = dbGroups.getDbGroup().stream().filter(dbGroup -> StringUtil.equals(dbGroup.getName(), dbInstance.getDbGroup())).findFirst();
            if (!dbGroupOp.isPresent()) {
                dbGroupOp = tempDbGroupList.stream().filter(dbGroup -> StringUtil.equals(dbGroup.getName(), dbInstance.getDbGroup())).findFirst();
                if (!dbGroupOp.isPresent()) {
                    String msg = String.format("Cannot add or update a child row: a logical foreign key '%s':%s constraint fails", COLUMN_DB_GROUP, dbInstance.getDbGroup());
                    throw new SQLException(msg, "42S22", ErrorCode.ER_NO_REFERENCED_ROW_2);
                }
                dbGroups.addDbGroup(dbGroupOp.get());
            }
            dbGroupOp.get().addDbInstance(dbInstance);
        }

        dbConfig = DBConverter.dbBeanToJson(dbGroups);
        DbleTempConfig.getInstance().setDbConfig(dbConfig);
        return rows.size();
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        checkUpdateDbInstanceType(affectPks, values);
        affectPks.forEach(affectPk -> {
            if (Boolean.FALSE.toString().equalsIgnoreCase(affectPk.get(COLUMN_ENCRYPT_CONFIGURED))) {
                String password = DecryptUtil.dbHostDecrypt(true, affectPk.get(COLUMN_NAME), affectPk.get(COLUMN_USER), affectPk.get(COLUMN_PASSWORD_ENCRYPT));
                affectPk.put(COLUMN_PASSWORD_ENCRYPT, password);
            }
            affectPk.putAll(values);
        });
        List<DBInstance> dbInstanceList = affectPks.stream().map(this::transformRowToDBInstance).collect(Collectors.toList());

        DBConverter dbConverter = new DBConverter();
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        DbGroups dbGroups = dbConverter.dbJsonToBean(dbConfig, false);

        for (DBInstance dbInstance : dbInstanceList) {
            Optional<DBGroup> dbGroupOp = dbGroups.getDbGroup().stream().filter(dbGroup -> StringUtil.equals(dbGroup.getName(), dbInstance.getDbGroup())).findFirst();
            if (!dbGroupOp.isPresent()) {
                String msg = String.format("Cannot add or update a child row: a logical foreign key '%s':%s constraint fails", COLUMN_DB_GROUP, dbInstance.getDbGroup());
                throw new SQLException(msg, "42S22", ErrorCode.ER_NO_REFERENCED_ROW_2);
            }
            dbGroupOp.get().getDbInstance().removeIf(sourceDbInstance -> StringUtil.equals(sourceDbInstance.getName(), dbInstance.getName()));
            dbGroupOp.get().addDbInstance(dbInstance);
        }
        dbConfig = DBConverter.dbBeanToJson(dbGroups);
        DbleTempConfig.getInstance().setDbConfig(dbConfig);
        return affectPks.size();
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        List<DBInstance> dbInstanceList = affectPks.stream().map(this::transformRowToDBInstance).collect(Collectors.toList());

        DBConverter dbConverter = new DBConverter();
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        DbGroups dbGroups = dbConverter.dbJsonToBean(dbConfig, false);

        for (DBInstance dbInstance : dbInstanceList) {
            Optional<DBGroup> dbGroupOp = dbGroups.getDbGroup().stream().filter(dbGroup -> StringUtil.equals(dbGroup.getName(), dbInstance.getDbGroup())).findFirst();
            if (!dbGroupOp.isPresent()) {
                String msg = String.format("Cannot add or update a child row: a logical foreign key '%s':%s constraint fails", COLUMN_DB_GROUP, dbInstance.getDbGroup());
                throw new SQLException(msg, "42S22", ErrorCode.ER_NO_REFERENCED_ROW_2);
            }
            dbGroupOp.get().getDbInstance().removeIf(sourceDbInstance -> StringUtil.equals(sourceDbInstance.getName(), dbInstance.getName()));
        }
        //remove empty instance
        dbGroups.getDbGroup().removeIf(dbGroup -> dbGroup.getDbInstance().isEmpty());
        dbConfig = DBConverter.dbBeanToJson(dbGroups);
        DbleTempConfig.getInstance().setDbConfig(dbConfig);
        return affectPks.size();
    }

    @Override
    public void afterExecute() {
        //remove temp dbGroup
        DBConverter dbConverter = new DBConverter();
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        DbGroups dbGroups = dbConverter.dbJsonToBean(dbConfig, false);
        DbleDbGroup dbleDbGroup = (DbleDbGroup) ManagerSchemaInfo.getInstance().getTables().get(DbleDbGroup.TABLE_NAME);
        for (DBGroup dbGroup : dbGroups.getDbGroup()) {
            dbleDbGroup.getTempRowList().removeIf(group -> StringUtil.equals(group.get(DbleDbGroup.COLUMN_NAME), dbGroup.getName()));
        }
    }

    private DBInstance transformRowToDBInstance(LinkedHashMap<String, String> map) {
        if (null == map || map.isEmpty()) {
            return null;
        }
        checkBooleanVal(map);
        DBInstance dbInstance = new DBInstance();
        StringBuilder url = new StringBuilder();
        List<Property> propertyList = Lists.newArrayList();
        String key;
        String entryValue;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case COLUMN_NAME:
                    dbInstance.setName(entry.getValue());
                    break;
                case COLUMN_DB_GROUP:
                    dbInstance.setDbGroup(entry.getValue());
                    break;
                case COLUMN_ADDR:
                case COLUMN_PORT:
                    url.append(entry.getValue()).append(":");
                    break;
                case COLUMN_USER:
                    dbInstance.setUser(entry.getValue());
                    break;
                case COLUMN_PASSWORD_ENCRYPT:
                    dbInstance.setPassword(entry.getValue());
                    break;
                case COLUMN_ENCRYPT_CONFIGURED:
                    dbInstance.setUsingDecrypt(entry.getValue());
                    break;
                case COLUMN_PRIMARY:
                    dbInstance.setPrimary(!StringUtil.isEmpty(entry.getValue()) && Boolean.parseBoolean(entry.getValue()));
                    break;
                case COLUMN_DISABLED:
                    dbInstance.setDisabled(entry.getValue());
                    break;
                case COLUMN_DATABASE_TYPE:
                    DataBaseType.valueOf(entry.getValue().toUpperCase());
                    dbInstance.setDatabaseType(entry.getValue().toLowerCase());
                    break;
                case COLUMN_DB_DISTRICT:
                    String value = entry.getValue();
                    checkChineseProperty(value, COLUMN_DB_DISTRICT);
                    dbInstance.setDbDistrict(value);
                    break;
                case COLUMN_DB_DATA_CENTER:
                    String val = entry.getValue();
                    checkChineseProperty(val, COLUMN_DB_DATA_CENTER);
                    dbInstance.setDbDataCenter(val);
                    break;
                case COLUMN_MIN_CONN_COUNT:
                    if (!StringUtil.isBlank(entry.getValue())) {
                        dbInstance.setMinCon(IntegerUtil.parseInt(entry.getValue()));
                    }
                    if (dbInstance.getMinCon() < 0) {
                        throw new ConfigException("Column 'min_conn_count' value cannot be less than 0.");
                    }
                    break;
                case COLUMN_MAX_CONN_COUNT:
                    if (!StringUtil.isBlank(entry.getValue())) {
                        dbInstance.setMaxCon(IntegerUtil.parseInt(entry.getValue()));
                    }
                    if (dbInstance.getMaxCon() < 0) {
                        throw new ConfigException("Column 'max_conn_count' value cannot be less than 0.");
                    }
                    break;
                case COLUMN_READ_WEIGHT:
                    dbInstance.setReadWeight(entry.getValue());
                    break;
                case COLUMN_ID:
                    dbInstance.setId(entry.getValue());
                    break;
                case COLUMN_TEST_ON_CREATE:
                case COLUMN_TEST_ON_BORROW:
                case COLUMN_TEST_ON_RETURN:
                case COLUMN_TEST_WHILE_IDLE:
                    key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
                    entryValue = entry.getValue();
                    if (StringUtil.isBlank(entryValue) || (!StringUtil.equalsIgnoreCase(entryValue, Boolean.FALSE.toString()) && !StringUtil.equalsIgnoreCase(entryValue, Boolean.TRUE.toString()))) {
                        throw new ConfigException("Column '" + entry.getKey() + "' values only support 'false' or 'true'.");
                    }
                    propertyList.add(new Property(entryValue, key));
                    break;
                case COLUMN_CONNECTION_TIMEOUT:
                case COLUMN_CONNECTION_HEARTBEAT_TIMEOUT:
                case COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS:
                case COLUMN_IDLE_TIMEOUT:
                case COLUMN_HEARTBEAT_PERIOD_MILLIS:
                case COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS:
                case COLUMN_FLOW_HIGH_LEVEL:
                case COLUMN_FLOW_LOW_LEVEL:
                    key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
                    entryValue = entry.getValue();
                    if (StringUtil.isBlank(entryValue) || IntegerUtil.parseInt(entryValue) <= 0) {
                        throw new ConfigException("Column '" + entry.getKey() + "' should be an integer greater than 0!");
                    }
                    propertyList.add(new Property(entryValue, key));
                    break;
                default:
                    break;
            }
        }
        dbInstance.setUrl(url.substring(0, url.length() - 1));
        dbInstance.setProperty(propertyList);
        return dbInstance;
    }

    public void checkUpdateDbInstanceType(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) {
        if (affectPks.size() > 1 && values.containsKey(COLUMN_DATABASE_TYPE)) {
            LinkedHashMap<String, String> next = affectPks.iterator().next();
            if (!StringUtil.equalsIgnoreCase(next.get(COLUMN_DATABASE_TYPE), values.get(COLUMN_DATABASE_TYPE))) {
                throw new ConfigException("all dbInstance database type need to be consistent");
            }
        }
    }

    public void checkInsertDbInstanceType(List<LinkedHashMap<String, String>> insertRows) {
        Map<String, String> dataBaseTypeMap = new HashMap<>();
        for (LinkedHashMap<String, String> insertRow : insertRows) {
            String databaseType = insertRow.get(COLUMN_DATABASE_TYPE);
            String dbGroup = insertRow.get(COLUMN_DB_GROUP);
            if (Strings.isNullOrEmpty(databaseType)) {
                dataBaseTypeMap.put(insertRow.get(COLUMN_DB_GROUP), DataBaseType.MYSQL.name());
            } else if (dataBaseTypeMap.containsKey(dbGroup) && !StringUtil.equalsIgnoreCase(insertRow.get(dbGroup), dataBaseTypeMap.get(COLUMN_DATABASE_TYPE))) {
                throw new ConfigException("all dbInstance database type need to be consistent");
            } else {
                dataBaseTypeMap.put(insertRow.get(COLUMN_DB_GROUP), databaseType);
            }
        }

        List<LinkedHashMap<String, String>> rows = getRows();
        for (LinkedHashMap<String, String> row : rows) {
            String dbGroup = row.get(COLUMN_DB_GROUP);
            if (dataBaseTypeMap.containsKey(row.get(dbGroup)) &&
                    !StringUtil.equalsIgnoreCase(row.get(COLUMN_DATABASE_TYPE), dataBaseTypeMap.get(row.get(dbGroup)))) {
                throw new ConfigException("all dbInstance database type need to be consistent");
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

    private String getPasswordEncrypt(String instanceName, String name, String password) {
        try {
            return DecryptUtil.encrypt("1:" + instanceName + ":" + name + ":" + password);
        } catch (Exception e) {
            return "******";
        }
    }

    @Override
    public void updateTempConfig() {
        DbleTempConfig.getInstance().setDbConfig(DbleServer.getInstance().getConfig().getDbConfig());
    }

    private void checkChineseProperty(String val, String name) {
        if (Objects.nonNull(val)) {
            if (StringUtil.isBlank(val)) {
                throw new ConfigException("Column [ " + name + " ] " + val + " is illegal, the value not be null or empty");
            }
            int length = 11;
            if (val.length() > length) {
                throw new ConfigException("Column [ " + name + " ] " + val + " is illegal, the value contains a maximum of  " + length + "  characters");
            }
            String chinese = val.replaceAll(DBConverter.PATTERN_DB.toString(), "");
            if (Strings.isNullOrEmpty(chinese)) {
                return;
            }
            if (!StringUtil.isChinese(chinese)) {
                throw new ConfigException("Column [ " + name + " ] " + val + " is illegal,the " + Charset.defaultCharset().name() + " encoding is recommended, Column [ " + name + " ]  show be use  u4E00-u9FA5a-zA-Z_0-9\\-\\.");
            }
        }
    }
}
