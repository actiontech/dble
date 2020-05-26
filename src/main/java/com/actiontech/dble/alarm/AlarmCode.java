/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;


/**
 * Created by szf on 2017/12/13.
 */
public final class AlarmCode {

    private AlarmCode() {
    }

    public static final String WRITE_TEMP_RESULT_FAIL = "DBLE_WRITE_TEMP_RESULT_FAIL";
    public static final String XA_RECOVER_FAIL = "DBLE_XA_RECOVER_FAIL";
    public static final String XA_READ_XA_STREAM_FAIL = "DBLE_XA_READ_XA_STREAM_FAIL";
    public static final String XA_READ_DECODE_FAIL = "DBLE_XA_READ_DECODE_FAIL";
    public static final String XA_READ_IO_FAIL = "DBLE_XA_READ_IO_FAIL";
    public static final String XA_WRITE_IO_FAIL = "DBLE_XA_WRITE_IO_FAIL";
    public static final String XA_WRITE_CHECK_POINT_FAIL = "DBLE_XA_WRITE_CHECK_POINT_FAIL"; //Resolve by trigger
    public static final String XA_BACKGROUND_RETRY_FAIL = "DBLE_XA_BACKGROUND_RETRY_FAIL"; //Resolve by trigger
    public static final String REACH_MAX_CON = "DBLE_REACH_MAX_CON"; //Resolve by trigger
    public static final String TABLE_NOT_CONSISTENT_IN_SHARDINGS = "DBLE_TABLE_NOT_CONSISTENT_IN_SHARDINGS"; //Resolve by trigger
    public static final String TABLE_NOT_CONSISTENT_IN_MEMORY = "DBLE_TABLE_NOT_CONSISTENT_IN_MEMORY"; //Resolve by trigger
    public static final String GLOBAL_TABLE_COLUMN_LOST = "DBLE_GLOBAL_TABLE_COLUMN_LOST"; //Resolve by trigger
    public static final String CREATE_CONN_FAIL = "DBLE_CREATE_CONN_FAIL"; //Resolve by trigger
    public static final String DB_INSTANCE_CAN_NOT_REACH = "DBLE_DB_INSTANCE_CAN_NOT_REACH";
    public static final String KILL_BACKEND_CONN_FAIL = "DBLE_KILL_BACKEND_CONN_FAIL";
    public static final String NIOREACTOR_UNKNOWN_EXCEPTION = "DBLE_NIOREACTOR_UNKNOWN_EXCEPTION";
    public static final String NIOREACTOR_UNKNOWN_THROWABLE = "DBLE_NIOREACTOR_UNKNOWN_THROWABLE";
    public static final String NIOCONNECTOR_UNKNOWN_EXCEPTION = "DBLE_NIOCONNECTOR_UNKNOWN_EXCEPTION";
    public static final String TABLE_LACK = "DBLE_TABLE_LACK"; //Resolve by trigger
    public static final String GET_TABLE_META_FAIL = "DBLE_GET_TABLE_META_FAIL";
    public static final String TEST_CONN_FAIL = "DBLE_TEST_CONN_FAIL";
    public static final String HEARTBEAT_FAIL = "DBLE_HEARTBEAT_FAIL"; //Resolve by trigger
    public static final String SHARDING_NODE_LACK = "DBLE_SHARDING_NODE_LACK"; //Resolve by trigger
    public static final String DB_INSTANCE_LOWER_CASE_ERROR = "DBLE_DB_INSTANCE_LOWER_CASE_ERROR"; //Resolve by trigger
}
