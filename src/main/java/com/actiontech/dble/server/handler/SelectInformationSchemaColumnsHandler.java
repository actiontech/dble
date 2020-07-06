package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.SingleTableConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.manager.response.ShowConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.sqlengine.*;
import com.actiontech.dble.util.IntegerUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class SelectInformationSchemaColumnsHandler {

    private static final String TABLE_SCHEMA = "TABLE_SCHEMA";
    private static final String TABLE_NAME = "TABLE_NAME";

    private SelectInformationSchemaColumnsHandler() {
    }

    public static void handle(ServerConnection c, FieldPacket[] fields, MySqlSelectQueryBlock mySqlSelectQueryBlock) {
        SQLExpr whereExpr = mySqlSelectQueryBlock.getWhere();

        Map<String, String> whereInfo = new ConcurrentHashMap<>();
        ShowConnection.getWhereCondition(whereExpr, whereInfo);

        String cSchema = null;
        String table = null;
        SchemaConfig schemaConfig = null;

        if ((cSchema = containsKeyIngoreCase(whereInfo, TABLE_SCHEMA)) == null || (schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(cSchema)) == null) {
            c.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(c.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            c.writeErrMessage("42000", "Access denied for user '" + c.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        if ((table = containsKeyIngoreCase(whereInfo, TABLE_NAME)) == null || !schemaConfig.getTables().containsKey(table)) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + cSchema + "." + table + "] doesn't exist");
            return;
        }

        BaseTableConfig tableConfig = schemaConfig.getTables().get(table);
        if (tableConfig == null) {
            c.writeErrMessage(ErrorCode.ER_YES, "The table " + table + " doesn‘t exist");
            return;
        } else if (tableConfig instanceof SingleTableConfig) {
            c.writeErrMessage(ErrorCode.ER_YES, "The table " + table + " is Single table");
            return;
        }


        String shardingNode = tableConfig.getShardingNodes().get(0);
        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
        String shardingDataBase = dn.getDatabase();

        // sql
        updateWhereCondition(whereExpr, TABLE_SCHEMA, shardingDataBase);
        String sql = "SELECT character_octet_length FROM INFORMATION_SCHEMA.COLUMNS WHERE " + whereExpr.toString();


        // == OneRaw
        ReentrantLock lock = new ReentrantLock();
        Condition cond = lock.newCondition();
        List<Integer> results = new ArrayList<>();
        AtomicBoolean succeed = new AtomicBoolean(true);

        OneRawSQLQueryResultHandler resultHandler2 = new OneRawSQLQueryResultHandler(new String[]{"character_octet_length"}, new SelectInformationSchemaColumnsListener(lock, cond, results, succeed));
        SQLJob sqlJob = new SQLJob(sql, shardingNode, resultHandler2, true);
        sqlJob.run();

        lock.lock();
        try {
            while (results.size() == 0) {
                cond.await();
            }
        } catch (InterruptedException e) {
            c.writeErrMessage(ErrorCode.ER_YES, "occur InterruptedException, so try again later ");
            return;
        } finally {
            lock.unlock();
        }

        if (!succeed.get()) {
            c.writeErrMessage(ErrorCode.ER_YES, "occur Exception, so see dble.log to check reason");
            return;
        }

        RowDataPacket[] rows = null;
        if (results != null) {
            rows = new RowDataPacket[1];
            int index = 0;
            for (Integer value : results) {
                RowDataPacket row = new RowDataPacket(fields.length);
                row.add(IntegerUtil.toBytes(value));
                rows[index++] = row;
            }
        }
        MysqlSystemSchemaHandler.doWrite(fields.length, fields, rows, c);
    }

    public static String containsKeyIngoreCase(Map<String, String> map, String key) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static void updateWhereCondition(SQLExpr whereExpr, String whereKey, String whereValue) {
        if (whereExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr tmp = (SQLBinaryOpExpr) whereExpr;
            if (tmp.getLeft() instanceof SQLBinaryOpExpr) {
                updateWhereCondition(tmp.getLeft(), whereKey, whereValue);
                updateWhereCondition(tmp.getRight(), whereKey, whereValue);
            } else {
                if (tmp.getLeft().toString().equalsIgnoreCase(whereKey)) {
                    tmp.setRight(new SQLCharExpr(whereValue));
                }
            }
        }
    }

    static class SelectInformationSchemaColumnsListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private ReentrantLock lock;
        private Condition cond;
        private List<Integer> results;
        private AtomicBoolean succeed;

        SelectInformationSchemaColumnsListener(ReentrantLock lock, Condition cond, List<Integer> results, AtomicBoolean succeed) {
            this.lock = lock;
            this.cond = cond;
            this.results = results;
            this.succeed = succeed;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (!result.isSuccess()) {
                succeed.set(false);
            } else {
                String value = result.getResult().get("character_octet_length");
                results.add(Integer.valueOf(value));
            }
            lock.lock();
            try {
                cond.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
