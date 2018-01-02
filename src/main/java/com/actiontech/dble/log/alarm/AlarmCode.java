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

    public static final String ALARM_SPLITE = "::";
    public static final String CORE_CLUSTER_WARN = "DBLE_CORE_CLUSTER_WARN" + ALARM_SPLITE;
    public static final String CORE_FILE_WRITE_WARN = "DBLE_CORE_FILE_WRITE_WARN" + ALARM_SPLITE;
    public static final String CORE_ZK_WARN = "DBLE_CORE_ZK_WARN" + ALARM_SPLITE;
    public static final String CORE_ZK_ERROR = "DBLE_CORE_ZK_ERROR" + ALARM_SPLITE;
    public static final String CORE_PERFORMANCE_WARN = "DBLE_CORE_PERFORMANCE_WARN" + ALARM_SPLITE;
    public static final String CORE_DDL_WARN = "DBLE_CORE_DDL_WARN" + ALARM_SPLITE;
    public static final String CORE_TABLE_CHECK_WARN = "DBLE_CORE_TABLE_CHECK_WARN" + ALARM_SPLITE;
    public static final String CORE_GENERAL_WARN = "DBLE_CORE_GENERAL_WARN" + ALARM_SPLITE;
    public static final String CORE_ERROR = "DBLE_CORE_ERROR" + ALARM_SPLITE;
    public static final String CORE_BACKEND_SWITCH = "DBLE_CORE_BACKEND_SWITCH" + ALARM_SPLITE;
    public static final String CORE_XA_WARN = "DBLE_CORE_XA_WARN" + ALARM_SPLITE;
    public static final String CORE_SEQUENCE_WARN = "DBLE_CORE_SEQUENCE_WARN" + ALARM_SPLITE;


}
