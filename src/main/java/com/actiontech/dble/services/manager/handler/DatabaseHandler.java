/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
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
    private static final Pattern PATTERN = Pattern.compile("^\\s*(create|drop)\\s*database\\s*@@shardingNode\\s*=\\s*(['\"])([a-zA-Z_0-9,$\\-]+)(['\"])\\s*$", Pattern.CASE_INSENSITIVE);

    private static final String CREATE_DATABASE = "create database if not exists `%s`";
    private static final String DROP_DATABASE = "drop database if exists `%s`";

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    private DatabaseHandler() {
    }

    public static void handle(String stmt, ManagerService service, boolean isCreate) {

        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches() || !ma.group(2).equals(ma.group(4))) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql did not match create|drop database @@shardingNode ='dn......'");
            return;
        }
        String shardingNodeStr = ma.group(3);
        Set<String> shardingNodes = new HashSet<>(Arrays.asList(SplitUtil.split(shardingNodeStr, ',', '$', '-')));
        //check shardingNodes
        for (String singleDn : shardingNodes) {
            if (DbleServer.getInstance().getConfig().getShardingNodes().get(singleDn) == null) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "shardingNode " + singleDn + " does not exists");
                return;
            }
        }

        final List<String> errShardingNodes = new CopyOnWriteArrayList<>();
        final Map<String, ShardingNode> allShardingNodes = DbleServer.getInstance().getConfig().getShardingNodes();
        final AtomicInteger numberCount = new AtomicInteger(shardingNodes.size());
        for (final String shardingNode : shardingNodes) {
            ShardingNode dn = allShardingNodes.get(shardingNode);
            final PhysicalDbInstance ds = dn.getDbGroup().getWriteDbInstance();
            final String schema = dn.getDatabase();
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SQLQueryResultListener<SQLQueryResult<Map<String, String>>>() {
                @Override
                public void onResult(SQLQueryResult<Map<String, String>> result) {
                    if (!result.isSuccess()) {
                        dn.setSchemaExists(false);
                        errShardingNodes.add(shardingNode);
                    } else if (isCreate) {
                        dn.setSchemaExists(true);
                        tryResolve(ds.getDbGroupConfig().getName(), ds.getConfig().getInstanceName(), shardingNode, schema, ds.getConfig().getId());
                    } else {
                        dn.setSchemaExists(false);
                        tryAlert(ds.getDbGroupConfig().getName(), ds.getConfig().getInstanceName(), shardingNode, schema, ds.getConfig().getId());
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

        writeResponse(service, errShardingNodes);
    }

    private static void writeResponse(ManagerService service, List<String> errMsg) {
        if (errMsg.size() == 0) {
            OK.write(service.getConnection());
        } else {
            String msg = "Unknown error occurs in [" + StringUtils.join(errMsg, ',') + "],please manually confirm result again.";
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
            errMsg.clear();
        }
    }

    private static void tryResolve(String dbGroupName, String dbInstanceName, String shardingNode, String schema, String dbInstanceId) {
        String key = "dbInstance[" + dbGroupName + "." + dbInstanceName + "],sharding_node[" + shardingNode + "],schema[" + schema + "]";
        if (ToResolveContainer.SHARDING_NODE_LACK.contains(key)) {
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", dbGroupName + "-" + dbInstanceName);
            labels.put("sharding_node", shardingNode);
            AlertUtil.alertResolve(AlarmCode.SHARDING_NODE_LACK, Alert.AlertLevel.WARN, "mysql", dbInstanceId, labels,
                    ToResolveContainer.SHARDING_NODE_LACK, key);
        }
    }

    private static void tryAlert(String dbGroupName, String dbInstanceName, String shardingNode, String schema, String dbInstanceId) {
        String key = "dbInstance[" + dbGroupName + "." + dbInstanceName + "],sharding_node[" + shardingNode + "],schema[" + schema + "]";
        if (ToResolveContainer.SHARDING_NODE_LACK.contains(key)) {
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", dbGroupName + "-" + dbInstanceName);
            labels.put("sharding_node", shardingNode);
            AlertUtil.alert(AlarmCode.SHARDING_NODE_LACK, Alert.AlertLevel.WARN, "{" + key + "} is lack", "mysql", dbInstanceId, labels);
            ToResolveContainer.SHARDING_NODE_LACK.add(key);
        }
    }
}
