/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.alarm;


/**
 * Created by szf on 2017/12/13.
 */
public final class AlarmCode {

    private AlarmCode() {

    }

    public static final String ALARM_SPLIT = "::";
    public static final String WRITE_NODE_INDEX_FAIL = "DBLE_WRITE_NODE_INDEX_FAIL" + ALARM_SPLIT;
    public static final String WRITE_TEMP_RESULT_FAIL = "DBLE_WRITE_TEMP_RESULT_FAIL" + ALARM_SPLIT;
    public static final String XA_READ_IO_FAIL = "DBLE_XA_READ_IO_FAIL" + ALARM_SPLIT;
    public static final String XA_READ_XA_STREAM_FAIL = "DBLE_XA_READ_XA_STREAM_FAIL" + ALARM_SPLIT;
    public static final String XA_READ_DECODE_FAIL = "DBLE_XA_READ_DECODE_FAIL" + ALARM_SPLIT;
    public static final String XA_WRITE_CHECK_POINT_FAIL = "DBLE_XA_WRITE_CHECK_POINT_FAIL" + ALARM_SPLIT;
    public static final String REACH_MAX_CON = "DBLE_REACH_MAX_CON" + ALARM_SPLIT;
    public static final String TABLE_NOT_CONSISTENT_IN_DATAHOSTS = "DBLE_TABLE_NOT_CONSISTENT_IN_DATAHOSTS" + ALARM_SPLIT;
    public static final String TABLE_NOT_CONSISTENT_IN_MEMORY = "DBLE_TABLE_NOT_CONSISTENT_IN_MEMORY" + ALARM_SPLIT;
    public static final String GLOBAL_TABLE_COLUMN_LOST = "DBLE_GLOBAL_TABLE_COLUMN_LOST" + ALARM_SPLIT;
    public static final String CREATE_CONN_FAIL = "DBLE_CREATE_CONN_FAIL" + ALARM_SPLIT;
    public static final String DATA_HOST_CAN_NOT_REACH = "DBLE_DATA_HOST_CAN_NOT_REACH" + ALARM_SPLIT;
    public static final String TABLE_LACK = "DBLE_TABLE_LACK" + ALARM_SPLIT;
    public static final String GET_TABLE_META_FAIL = "DBLE_GET_TABLE_META_FAIL" + ALARM_SPLIT;
    public static final String NIOREACTOR_UNKNOWN_EXCEPTION = "DBLE_NIOREACTOR_UNKNOWN_EXCEPTION" + ALARM_SPLIT;
    public static final String NIOREACTOR_UNKNOWN_THROWABLE = "DBLE_NIOREACTOR_UNKNOWN_THROWABLE" + ALARM_SPLIT;
    public static final String NIOCONNECTOR_UNKNOWN_EXCEPTION = "DBLE_NIOCONNECTOR_UNKNOWN_EXCEPTION" + ALARM_SPLIT;
    public static final String TEST_CONN_FAIL = "DBLE_TEST_CONN_FAIL" + ALARM_SPLIT;
    public static final String KILL_BACKEND_CONN_FAIL = "DBLE_KILL_BACKEND_CONN_FAIL" + ALARM_SPLIT;




}
