/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

import com.actiontech.dble.util.TimeUtil;

/**
 * Created by huqing.yan on 2017/6/7.
 */
public class DDLInfo {
    public enum DDLStatus {
        INIT, SUCCESS, FAILED
    }

    public enum DDLType {
        CREATE_TABLE, DROP_TABLE, ALTER_TABLE, TRUNCATE_TABLE, CREATE_INDEX, DROP_INDEX, UNKNOWN
    }

    private String schema;
    private String sql;
    private String from;
    private DDLStatus status;
    private DDLType type;
    private String split = ";";

    public DDLInfo(String schema, String sql, String from, DDLStatus ddlStatus, DDLType ddlType) {
        this.schema = schema;
        this.sql = sql;
        this.from = from;
        this.status = ddlStatus;
        this.type = ddlType;
    }

    public DDLInfo(String info) {
        String[] infoDetail = info.split(split);
        this.status = DDLStatus.valueOf(infoDetail[1]);
        this.type = DDLType.valueOf(infoDetail[2]);
        this.schema = infoDetail[3];
        this.from = infoDetail[4];
        this.sql = infoDetail[5];
        if (infoDetail.length > 6) {
            StringBuilder sb = new StringBuilder(this.sql);
            for (int i = 6; i < infoDetail.length; i++) {
                sb.append(infoDetail[i]);
            }
            this.sql = sb.toString();
        }
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
