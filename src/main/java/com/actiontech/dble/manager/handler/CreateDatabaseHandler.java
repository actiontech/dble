package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final Pattern PATTERN = Pattern.compile("\\s*create\\s*database\\s*@@([a-zA-Z_0-9]+)\\s*", 2);

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    private CreateDatabaseHandler() {
    }

    public static void handle(String stmt, ManagerConnection c) {

        Matcher ma = PATTERN.matcher(stmt);
        if (!ma.matches()) {
            c.writeErrMessage(1105, "The sql did not match create database @@schema");
            return;
        }
        String schemaName = ma.group(1);

        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
        if (schema == null) {
            c.writeErrMessage(1105, "The schema can not be found");
            return;
        }
        Set<String> dataNodes = schema.getAllDataNodes();

        List<String> errMsg = new ArrayList<>();
        AtomicInteger numberCount = new AtomicInteger(dataNodes.size());
        for (String dataNode : dataNodes) {
            PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);

            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new SQLQueryResultListener<SQLQueryResult<Map<String, String>>>() {
                @Override
                public void onResult(SQLQueryResult<Map<String, String>> result) {
                    if (!result.isSuccess()) {
                        errMsg.add(dataNode);
                    }
                    numberCount.decrementAndGet();
                }

            });
            SQLJob sqlJob = new SQLJob("create database if not exists " + dn.getDatabase(), null, resultHandler, dn.getDbPool().getSource());
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
            String msg = "create database error in [" + String.join(",", errMsg) + "]";
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
            errMsg.clear();
        }
    }
}
