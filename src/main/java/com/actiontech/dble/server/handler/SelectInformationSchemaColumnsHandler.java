package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.manager.response.ShowConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SelectInformationSchemaColumnsHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectInformationSchemaColumnsHandler.class);

    private static final String[] INFORMATION_SCHEMACOLUMNS_COLS = new String[]{
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "COLUMN_TYPE",
            "COLUMN_KEY",
            "EXTRA",
            "PRIVILEGES",
            "COLUMN_COMMENT",
            "GENERATION_EXPRESSION"};

    private List<Map<String, String>> result;
    private Lock lock;
    private Condition done;
    private boolean finished = false;
    private boolean success = false;

    public SelectInformationSchemaColumnsHandler() {
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    public void handle(ServerConnection c, FieldPacket[] fields0, MySqlSelectQueryBlock mySqlSelectQueryBlock) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                sqlhandle(c, fields0, mySqlSelectQueryBlock);
            }
        });
    }

    public void sqlhandle(ServerConnection c, FieldPacket[] fields0, MySqlSelectQueryBlock mySqlSelectQueryBlock) {
        SQLExpr whereExpr = mySqlSelectQueryBlock.getWhere();

        Map<String, String> whereInfo = new HashMap<>();
        ShowConnection.getWhereCondition(whereExpr, whereInfo);

        String cSchema = null;
        String table = null;

        // the where condition should be contain table_schema, table_name equivalence judgment
        if ((cSchema = containsKeyIngoreCase(whereInfo, INFORMATION_SCHEMACOLUMNS_COLS[1])) == null || (table = containsKeyIngoreCase(whereInfo, INFORMATION_SCHEMACOLUMNS_COLS[2])) == null) {
            MysqlSystemSchemaHandler.doWrite(fields0.length, fields0, null, c);
            return;
        }

        SchemaConfig schemaConfig = null;
        if ((schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(cSchema)) == null) {
            c.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig userConfig = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(c.getUser()));
        if (userConfig == null || !userConfig.getSchemas().contains(cSchema)) {
            c.writeErrMessage("42000", "Access denied for user '" + c.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        String shardingNode = null;
        if (!schemaConfig.getTables().containsKey(table)) {
            if ((shardingNode = schemaConfig.getShardingNode()) == null) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + cSchema + "." + table + "] doesn't exist");
                return;
            }
        } else {
            BaseTableConfig tableConfig = schemaConfig.getTables().get(table);
            if (tableConfig == null) {
                c.writeErrMessage(ErrorCode.ER_YES, "The table " + table + " doesn't exist");
                return;
            }
            shardingNode = tableConfig.getShardingNodes().get(0);
        }

        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
        String shardingDataBase = dn.getDatabase();

        List<SQLSelectItem> selectItems = mySqlSelectQueryBlock.getSelectList();

        int fieldCount = selectItems.size();
        String[] selectCols = null;
        String[] selectColsAsAlias = null;

        FieldPacket[] fields = null;
        if (fieldCount == 1 && selectItems.get(0).toString().equals("*")) {
            fieldCount = INFORMATION_SCHEMACOLUMNS_COLS.length;
            fields = new FieldPacket[fieldCount];
            selectCols = INFORMATION_SCHEMACOLUMNS_COLS;
            selectColsAsAlias = INFORMATION_SCHEMACOLUMNS_COLS;
            for (int i = 0; i < fieldCount; i++) {
                fields[i] = PacketUtil.getField(INFORMATION_SCHEMACOLUMNS_COLS[i], Fields.FIELD_TYPE_VAR_STRING);
            }
        } else {
            fields = new FieldPacket[fieldCount];
            // columns
            selectCols = new String[fieldCount];
            // column as alia
            selectColsAsAlias = new String[fieldCount];

            SQLSelectItem selectItem = null;
            String columnName = null;
            for (int i = 0; i < fieldCount; i++) {
                selectItem = selectItems.get(i);
                if (selectItem.getAlias() != null) {
                    columnName = StringUtil.removeBackQuote(selectItems.get(i).getAlias());
                    selectColsAsAlias[i] = StringUtil.removeBackQuote(selectItem.toString()) + " as " + columnName;
                    selectCols[i] = StringUtil.removeBackQuote(selectItems.get(i).getAlias());
                } else {
                    columnName = StringUtil.removeBackQuote(selectItem.toString());
                    selectColsAsAlias[i] = columnName;
                    selectCols[i] = columnName;
                }
                fields[i] = PacketUtil.getField(columnName, Fields.FIELD_TYPE_VAR_STRING);
            }
        }

        replaceSchema(whereExpr, shardingDataBase);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(StringUtils.join(selectColsAsAlias, ", ")).append(" ").
                append("FROM INFORMATION_SCHEMA.COLUMNS WHERE ").
                append(whereExpr.toString());

        PhysicalDbInstance ds = dn.getDbGroup().getWriteDbInstance();
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(selectCols, new SelectInformationSchemaColumnsListener(shardingNode));
        if (ds.isAlive()) {
            SQLJob sqlJob = new SQLJob(sql.toString(), null, resultHandler, ds);
            sqlJob.run();
        } else {
            SQLJob sqlJob = new SQLJob(sql.toString(), shardingNode, resultHandler, false);
            sqlJob.run();
        }

        waitDone();

        if (!success) {
            c.writeErrMessage(ErrorCode.ER_YES, "occur Exception, so see dble.log to check reason");
            return;
        }

        RowDataPacket[] rows = null;
        if (result != null) {
            rows = new RowDataPacket[result.size()];
            int index = 0;
            for (Map<String, String> data : result) {
                RowDataPacket row = new RowDataPacket(fieldCount);
                for (String col : selectCols) {
                    row.add(null == data.get(col) ? null : StringUtil.encode(data.get(col), c.getCharset().getResults()));
                }
                rows[index++] = row;
            }
        }
        MysqlSystemSchemaHandler.doWrite(fieldCount, fields, rows, c);
    }

    public void replaceSchema(SQLExpr whereExpr, String realSchema) {
        updateWhereCondition(whereExpr, INFORMATION_SCHEMACOLUMNS_COLS[1], realSchema);
    }

    public String containsKeyIngoreCase(Map<String, String> map, String key) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public void updateWhereCondition(SQLExpr whereExpr, String whereKey, String whereValue) {
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

    private void waitDone() {
        lock.lock();
        try {
            while (!finished) {
                done.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("wait 'select informationschema.columns' grapping done " + e);
        } finally {
            lock.unlock();
        }
    }

    void signalDone() {
        lock.lock();
        try {
            finished = true;
            done.signal();
        } finally {
            lock.unlock();
        }
    }

    public List<Map<String, String>> getResult() {
        return this.result;
    }

    class SelectInformationSchemaColumnsListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
        private String shardingNode;

        SelectInformationSchemaColumnsListener(String shardingNode) {
            this.shardingNode = shardingNode;
        }

        @Override
        public void onResult(SQLQueryResult<List<Map<String, String>>> res) {
            if (!res.isSuccess()) {
                LOGGER.warn("execute 'select information_schema.columns' error in " + shardingNode);
            } else {
                success = true;
                result = res.getResult();
            }
            signalDone();
        }
    }
}
