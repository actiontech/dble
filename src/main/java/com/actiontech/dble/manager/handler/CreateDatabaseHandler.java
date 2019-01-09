/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.SplitUtil;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2018/7/2.
 */
public final class CreateDatabaseHandler {

    private static final OkPacket OK = new OkPacket();
    private static final Pattern PATTERN = Pattern.compile("^\\s*create\\s*database\\s*@@dataNode\\s*=\\s*(['\"])([a-zA-Z_0-9,$\\-]+)(['\"])\\s*$", Pattern.CASE_INSENSITIVE);

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    private CreateDatabaseHandler() {
    }

    public static void handle(String stmt, ManagerConnection c) {

        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches() || !ma.group(1).equals(ma.group(3))) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql did not match create database @@dataNode ='dn......'");
            return;
        }
        String dataNodeStr = ma.group(2);
        Set<String> dataNodes = new HashSet<>(Arrays.asList(SplitUtil.split(dataNodeStr, ',', '$', '-')));
        //check dataNodes
        for (String singleDn : dataNodes) {
            if (DbleServer.getInstance().getConfig().getDataNodes().get(singleDn) == null) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "DataNode " + singleDn + " does not exists");
                return;
            }
        }
        final List<String> errMsg = new CopyOnWriteArrayList<>();
        final AtomicInteger numberCount = new AtomicInteger(dataNodes.size());
        for (final String dataNode : dataNodes) {
            PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
            final PhysicalDatasource ds = dn.getDbPool().getSource();
            final String schema = dn.getDatabase();
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SQLQueryResultListener<SQLQueryResult<Map<String, String>>>() {
                @Override
                public void onResult(SQLQueryResult<Map<String, String>> result) {
                    if (!result.isSuccess()) {
                        errMsg.add(dataNode);
                    } else {
                        String key = "DataHost[" + ds.getHostConfig().getName() + "." + ds.getConfig().getHostName() + "],data_node[" + dataNode + "],schema[" + schema + "]";
                        if (ToResolveContainer.DATA_NODE_LACK.contains(key)) {
                            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", ds.getHostConfig().getName() + "-" + ds.getConfig().getHostName());
                            labels.put("data_node", dataNode);
                            if (AlertUtil.alertResolve(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "mysql", ds.getConfig().getId(), labels)) {
                                ToResolveContainer.DATA_NODE_LACK.remove(key);
                            }
                        }
                    }
                    numberCount.decrementAndGet();
                }

            });
            SQLJob sqlJob = new SQLJob("create database if not exists " + dn.getDatabase(), null, resultHandler, ds);
            sqlJob.run();
        }

        while (numberCount.get() > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }

        writeResponse(c, errMsg);
    }

    private static void writeResponse(ManagerConnection c, List<String> errMsg) {
        if (errMsg.size() == 0) {
            OK.write(c);
        } else {
            String msg = "create database error in [" + StringUtils.join(errMsg, ',') + "]";
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
            errMsg.clear();
        }
    }
}
