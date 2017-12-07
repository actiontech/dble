package com.actiontech.dble.log.alarm;


/**
 * Created by szf on 2017/12/13.
 */
public final class AlarmCode {

    private AlarmCode() {

    }


    public static final String USHARD_ALARM_SPLITE = "::";
    public static final String USHARD_CORE_CLUSTER_WARN = "USHARD_CORE_CLUSTER_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_FILE_WRITE_WARN = "USHARD_CORE_FILE_WRITE_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_ZK_WARN = "USHARD_CORE_ZK_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_ZK_ERROR = "USHARD_CORE_ZK_ERROR" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_PERFORMANCE_WARN = "USHARD_CORE_PERFORMANCE_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_DDL_WARN = "USHARD_CORE_DDL_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_TABLE_CHECK_WARN = "USHARD_CORE_TABLE_CHECK_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_GENERAL_WARN = "USHARD_CORE_GENERAL_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_ERROR = "USHARD_CORE_ERROR" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_BACKEND_SWITCH = "USHARD_CORE_BACKEND_SWITCH" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_XA_WARN = "USHARD_CORE_XA_WARN" + USHARD_ALARM_SPLITE;
    public static final String USHARD_CORE_SEQUENCE_WARN = "USHARD_CORE_SEQUENCE_WARN" + USHARD_ALARM_SPLITE;


}
