/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.slow;

import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.trace.TraceResult;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class SlowQueryLogEntry {
    private TraceResult trace;
    private long timeStamp;
    private String sql;
    private UserName user;
    private String clientIp;
    private long connID;

    SlowQueryLogEntry(String sql, TraceResult traceResult, UserName user, String clientIp, long connID) {
        this.timeStamp = System.currentTimeMillis();
        this.sql = RouterUtil.getFixedSql(sql);
        this.trace = traceResult;
        this.user = user;
        this.clientIp = clientIp;
        this.connID = connID;
    }

    @Override
    public String toString() {
        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        StringBuilder sb = new StringBuilder();
        sb.append("\n# Time: ");
        sb.append(dataFormat.format(new Date(timeStamp)));
        sb.append("000Z");
        sb.append("\n");

        sb.append("# User@Host: ");
        sb.append(user);
        sb.append("[");
        sb.append(user);
        sb.append("] @  [");
        sb.append(clientIp);
        sb.append("]  Id:   ");
        sb.append(connID);
        sb.append("\n");

        sb.append("# Query_time: ");
        sb.append(trace.getOverAllSecond());
        sb.append("  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0");
        List<String[]> results = trace.genLogResult();
        if (results != null) {
            for (String[] result : results) {
                sb.append("  ");
                sb.append(result[0]);
                sb.append(": ");
                sb.append(result[1]);
            }
        }
        sb.append("  ").append(this.trace.getType());
        sb.append("\n");

        sb.append("SET timestamp=");
        sb.append(timeStamp);
        sb.append(";\n");

        sb.append(sql);
        sb.append(";");
        return sb.toString();
    }
}
