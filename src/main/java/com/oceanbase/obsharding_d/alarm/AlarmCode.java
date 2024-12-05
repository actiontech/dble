/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.alarm;


/**
 * Created by szf on 2017/12/13.
 */
public final class AlarmCode {

    private AlarmCode() {
    }

    public static final String WRITE_TEMP_RESULT_FAIL = "OBsharding-D_WRITE_TEMP_RESULT_FAIL";
    public static final String XA_RECOVER_FAIL = "OBsharding-D_XA_RECOVER_FAIL";
    public static final String XA_SUSPECTED_RESIDUE = "OBsharding-D_XA_SUSPECTED_RESIDUE";
    public static final String XA_READ_XA_STREAM_FAIL = "OBsharding-D_XA_READ_XA_STREAM_FAIL";
    public static final String XA_READ_DECODE_FAIL = "OBsharding-D_XA_READ_DECODE_FAIL";
    public static final String XA_READ_IO_FAIL = "OBsharding-D_XA_READ_IO_FAIL";
    public static final String XA_WRITE_IO_FAIL = "OBsharding-D_XA_WRITE_IO_FAIL";
    public static final String XA_WRITE_CHECK_POINT_FAIL = "OBsharding-D_XA_WRITE_CHECK_POINT_FAIL"; //Resolve by trigger
    public static final String XA_BACKGROUND_RETRY_FAIL = "OBsharding-D_XA_BACKGROUND_RETRY_FAIL"; //Resolve by trigger
    public static final String XA_BACKGROUND_RETRY_STOP = "OBsharding-D_XA_BACKGROUND_RETRY_STOP";
    public static final String REACH_MAX_CON = "OBsharding-D_REACH_MAX_CON"; //Resolve by trigger
    public static final String TABLE_NOT_CONSISTENT_IN_SHARDINGS = "OBsharding-D_TABLE_NOT_CONSISTENT_IN_SHARDINGS"; //Resolve by trigger
    public static final String TABLE_NOT_CONSISTENT_IN_MEMORY = "OBsharding-D_TABLE_NOT_CONSISTENT_IN_MEMORY"; //Resolve by trigger
    public static final String GLOBAL_TABLE_NOT_CONSISTENT = "OBsharding-D_GLOBAL_TABLE_NOT_CONSISTENT"; //Resolve by trigger
    public static final String CREATE_CONN_FAIL = "OBsharding-D_CREATE_CONN_FAIL"; //Resolve by trigger
    public static final String DB_INSTANCE_CAN_NOT_REACH = "OBsharding-D_DB_INSTANCE_CAN_NOT_REACH";
    public static final String KILL_BACKEND_CONN_FAIL = "OBsharding-D_KILL_BACKEND_CONN_FAIL";
    public static final String NIOREACTOR_UNKNOWN_EXCEPTION = "OBsharding-D_NIOREACTOR_UNKNOWN_EXCEPTION";
    public static final String NIOREACTOR_UNKNOWN_THROWABLE = "OBsharding-D_NIOREACTOR_UNKNOWN_THROWABLE";
    public static final String NIOCONNECTOR_UNKNOWN_EXCEPTION = "OBsharding-D_NIOCONNECTOR_UNKNOWN_EXCEPTION";
    public static final String TABLE_LACK = "OBsharding-D_TABLE_LACK"; //Resolve by trigger
    public static final String GET_TABLE_META_FAIL = "OBsharding-D_GET_TABLE_META_FAIL";
    public static final String TEST_CONN_FAIL = "OBsharding-D_TEST_CONN_FAIL";
    public static final String HEARTBEAT_FAIL = "OBsharding-D_HEARTBEAT_FAIL"; //Resolve by trigger
    public static final String SHARDING_NODE_LACK = "OBsharding-D_SHARDING_NODE_LACK"; //Resolve by trigger
    public static final String DB_INSTANCE_LOWER_CASE_ERROR = "OBsharding-D_DB_INSTANCE_LOWER_CASE_ERROR"; //Resolve by trigger
    public static final String DB_SLAVE_INSTANCE_DELAY = "OBsharding-D_DB_SLAVE_INSTANCE_DELAY"; //Resolve by trigger
    public static final String DB_MASTER_INSTANCE_DELAY_FAIL = "DB_MASTER_INSTANCE_DELAY_FAIL";
    public static final String SLOW_QUERY_QUEUE_POLICY_ABORT = "SLOW_QUERY_QUEUE_POLICY_ABORT";
    public static final String SLOW_QUERY_QUEUE_POLICY_WAIT = "SLOW_QUERY_QUEUE_POLICY_WAIT";
}
