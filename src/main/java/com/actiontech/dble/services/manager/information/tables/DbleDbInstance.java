/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.manager.response.ShowHeartbeat;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

public class DbleDbInstance extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_db_instance";

    private static final String COLUMN_NAME = "name";

    private static final String COLUMN_DB_GROUP = "db_group";

    private static final String COLUMN_ADDR = "addr";

    private static final String COLUMN_PORT = "port";

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

    private static final String COLUMN_NUM_TESTS_PER_EVICTION_RUN = "num_tests_per_eviction_run";

    private static final String COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS = "evictor_shutdown_timeout_millis";

    private static final String COLUMN_IDLE_TIMEOUT = "idle_timeout";

    private static final String COLUMN_HEARTBEAT_PERIOD_MILLIS = "heartbeat_period_millis";

    public DbleDbInstance() {
        super(TABLE_NAME, 29);
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

        columns.put(COLUMN_PRIMARY, new ColumnMeta(COLUMN_PRIMARY, "varchar(5)", false));
        columnsType.put(COLUMN_PRIMARY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ACTIVE_CONN_COUNT, new ColumnMeta(COLUMN_ACTIVE_CONN_COUNT, "int(11)", true));
        columnsType.put(COLUMN_ACTIVE_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_IDLE_CONN_COUNT, new ColumnMeta(COLUMN_IDLE_CONN_COUNT, "int(11)", true));
        columnsType.put(COLUMN_IDLE_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_READ_CONN_REQUEST, new ColumnMeta(COLUMN_READ_CONN_REQUEST, "int(11)", true));
        columnsType.put(COLUMN_READ_CONN_REQUEST, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_WRITE_CONN_REQUEST, new ColumnMeta(COLUMN_WRITE_CONN_REQUEST, "int(11)", true));
        columnsType.put(COLUMN_WRITE_CONN_REQUEST, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DISABLED, new ColumnMeta(COLUMN_DISABLED, "varchar(5)", true));
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

        columns.put(COLUMN_READ_WEIGHT, new ColumnMeta(COLUMN_READ_WEIGHT, "int(11)", true));
        columnsType.put(COLUMN_READ_WEIGHT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONNECTION_TIMEOUT, new ColumnMeta(COLUMN_CONNECTION_TIMEOUT, "int(11)", true));
        columnsType.put(COLUMN_CONNECTION_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, new ColumnMeta(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, "int(11)", true));
        columnsType.put(COLUMN_CONNECTION_HEARTBEAT_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TEST_ON_CREATE, new ColumnMeta(COLUMN_TEST_ON_CREATE, "varchar(64)", true));
        columnsType.put(COLUMN_TEST_ON_CREATE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TEST_ON_BORROW, new ColumnMeta(COLUMN_TEST_ON_BORROW, "varchar(64)", true));
        columnsType.put(COLUMN_TEST_ON_BORROW, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TEST_ON_RETURN, new ColumnMeta(COLUMN_TEST_ON_RETURN, "varchar(64)", true));
        columnsType.put(COLUMN_TEST_ON_RETURN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TEST_WHILE_IDLE, new ColumnMeta(COLUMN_TEST_WHILE_IDLE, "varchar(64)", true));
        columnsType.put(COLUMN_TEST_WHILE_IDLE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, new ColumnMeta(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, "int(11)", true));
        columnsType.put(COLUMN_TIME_BETWEEN_EVICTION_RUNS_MILLIS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_NUM_TESTS_PER_EVICTION_RUN, new ColumnMeta(COLUMN_NUM_TESTS_PER_EVICTION_RUN, "int(11)", true));
        columnsType.put(COLUMN_NUM_TESTS_PER_EVICTION_RUN, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, new ColumnMeta(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, "int(11)", true));
        columnsType.put(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_IDLE_TIMEOUT, new ColumnMeta(COLUMN_IDLE_TIMEOUT, "int(11)", true));
        columnsType.put(COLUMN_IDLE_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_HEARTBEAT_PERIOD_MILLIS, new ColumnMeta(COLUMN_HEARTBEAT_PERIOD_MILLIS, "int(11)", true));
        columnsType.put(COLUMN_HEARTBEAT_PERIOD_MILLIS, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Set<String> nameSet = Sets.newHashSet();
        Map<String, PhysicalDbGroup> dbGroups = DbleServer.getInstance().getConfig().getDbGroups();
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        dbGroups.entrySet().forEach(dbGroupEntry -> {
            PhysicalDbGroup dbGroup = dbGroupEntry.getValue();
            dbGroup.getDbInstances(true).forEach(dbInstance -> {
                if (nameSet.add(dbInstance.getName() + "-" + dbGroup.getGroupName())) {
                    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                    DbInstanceConfig dbInstanceConfig = dbInstance.getConfig();
                    MySQLHeartbeat heartbeat = dbInstance.getHeartbeat();
                    PoolConfig poolConfig = dbInstanceConfig.getPoolConfig();
                    map.put(COLUMN_NAME, dbInstance.getName());
                    map.put(COLUMN_DB_GROUP, dbGroup.getGroupName());
                    map.put(COLUMN_ADDR, dbInstanceConfig.getIp());
                    map.put(COLUMN_PORT, String.valueOf(dbInstanceConfig.getPort()));
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
                    map.put(COLUMN_NUM_TESTS_PER_EVICTION_RUN, String.valueOf(poolConfig.getNumTestsPerEvictionRun()));
                    map.put(COLUMN_EVICTOR_SHUTDOWN_TIMEOUT_MILLIS, String.valueOf(poolConfig.getEvictorShutdownTimeoutMillis()));
                    map.put(COLUMN_IDLE_TIMEOUT, String.valueOf(poolConfig.getIdleTimeout()));
                    map.put(COLUMN_HEARTBEAT_PERIOD_MILLIS, String.valueOf(poolConfig.getHeartbeatPeriodMillis()));
                    rowList.add(map);
                }
            });
        });
        return rowList;
    }
}
