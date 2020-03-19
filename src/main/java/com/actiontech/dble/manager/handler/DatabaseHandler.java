/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
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
public final class DatabaseHandler {

    private static final OkPacket OK = new OkPacket();
    private static final Pattern PATTERN = Pattern.compile("^\\s*(create|drop)\\s*database\\s*@@dataNode\\s*=\\s*(['\"])([a-zA-Z_0-9,$\\-]+)(['\"])\\s*$", Pattern.CASE_INSENSITIVE);

    private static final String CREATE_DATABASE = "create database if not exists `%s`";
    private static final String DROP_DATABASE = "drop database if exists `%s`";

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    private DatabaseHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, boolean isCreate) {

        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches() || !ma.group(2).equals(ma.group(4))) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql did not match create|drop database @@dataNode ='dn......'");
            return;
        }
        String dataNodeStr = ma.group(3);
        Set<String> dataNodes = new HashSet<>(Arrays.asList(SplitUtil.split(dataNodeStr, ',', '$', '-')));
        //check dataNodes
        for (String singleDn : dataNodes) {
            if (DbleServer.getInstance().getConfig().getDataNodes().get(singleDn) == null) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "DataNode " + singleDn + " does not exists");
                return;
            }
        }

        final List<String> errDataNodes = new CopyOnWriteArrayList<>();
        final Map<String, PhysicalDataNode> allDataNodes = DbleServer.getInstance().getConfig().getDataNodes();
        final AtomicInteger numberCount = new AtomicInteger(dataNodes.size());
        for (final String dataNode : dataNodes) {
            PhysicalDataNode dn = allDataNodes.get(dataNode);
            final PhysicalDataSource ds = dn.getDataHost().getWriteSource();
            final String schema = dn.getDatabase();
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SQLQueryResultListener<SQLQueryResult<Map<String, String>>>() {
                @Override
                public void onResult(SQLQueryResult<Map<String, String>> result) {
                    if (!result.isSuccess()) {
                        dn.setSchemaExists(false);
                        errDataNodes.add(dataNode);
                    } else if (isCreate) {
                        dn.setSchemaExists(true);
                        tryResolve(ds.getHostConfig().getName(), ds.getConfig().getHostName(), dataNode, schema, ds.getConfig().getId());
                    } else {
                        dn.setSchemaExists(false);
                        tryAlert(ds.getHostConfig().getName(), ds.getConfig().getHostName(), dataNode, schema, ds.getConfig().getId());
                    }
                    numberCount.decrementAndGet();
                }

            });
            SQLJob sqlJob = new SQLJob(isCreate ? String.format(CREATE_DATABASE, schema) : String.format(DROP_DATABASE, schema), null, resultHandler, ds);
            sqlJob.run();
        }

        while (numberCount.get() > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }

        writeResponse(c, errDataNodes);
    }

    private static void writeResponse(ManagerConnection c, List<String> errMsg) {
        if (errMsg.size() == 0) {
            OK.write(c);
        } else {
            String msg = "Unknown error occurs in [" + StringUtils.join(errMsg, ',') + "],please manually confirm result again.";
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
            errMsg.clear();
        }
    }

    private static void tryResolve(String dataHost, String dataHost2, String dataNode, String schema, String dataHostId) {
        String key = "DataHost[" + dataHost + "." + dataHost2 + "],data_node[" + dataNode + "],schema[" + schema + "]";
        if (ToResolveContainer.DATA_NODE_LACK.contains(key)) {
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", dataHost + "-" + dataHost2);
            labels.put("data_node", dataNode);
            AlertUtil.alertResolve(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "mysql", dataHostId, labels,
                    ToResolveContainer.DATA_NODE_LACK, key);
        }
    }

    private static void tryAlert(String dataHost, String dataHost2, String dataNode, String schema, String dataHostId) {
        String key = "DataHost[" + dataHost + "." + dataHost2 + "],data_node[" + dataNode + "],schema[" + schema + "]";
        if (ToResolveContainer.DATA_NODE_LACK.contains(key)) {
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", dataHost + "-" + dataHost2);
            labels.put("data_node", dataNode);
            AlertUtil.alert(AlarmCode.DATA_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", dataHostId, labels);
            ToResolveContainer.DATA_NODE_LACK.add(key);
        }
    }
}
