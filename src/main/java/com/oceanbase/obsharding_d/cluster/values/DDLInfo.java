/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.oceanbase.obsharding_d.util.TimeUtil;

/**
 * Created by huqing.yan on 2017/6/7.
 */
public class DDLInfo {
    public enum DDLStatus {
        INIT, SUCCESS, FAILED
    }

    // The execution of ddl in the cluster is in two phases, PREPARE and COMPLETE
    public enum NodeStatus {
        PREPARE, COMPLETE
    }

    public enum DDLType {
        CREATE_DATABASE,
        CREATE_TABLE,
        DROP_TABLE,
        ALTER_TABLE,
        TRUNCATE_TABLE,
        CREATE_INDEX,
        DROP_INDEX,
        CREATEORREPLACE_VIEW,
        ALTER_VIEW,
        DROP_VIEW,
        UNKNOWN
    }

    private String schema;
    private String sql;
    private String from;
    private DDLStatus status;
    private DDLType type;
    private transient String split = ";";

    public DDLInfo() {
    }

    public DDLInfo(String schema, String sql, String from, DDLStatus ddlStatus, DDLType ddlType) {
        this.schema = schema;
        this.sql = sql;
        this.from = from;
        this.status = ddlStatus;
        this.type = ddlType;
    }

    @Override
    public String toString() {
        return TimeUtil.currentTimeNanos() + split + status.toString() + split + type.toString() + split + schema + split + from + split + sql;
    }

    public String getFrom() {
        return from;
    }

    public String getSql() {
        return sql;
    }

    public DDLStatus getStatus() {
        return status;
    }

    public DDLType getType() {
        return type;
    }

}
