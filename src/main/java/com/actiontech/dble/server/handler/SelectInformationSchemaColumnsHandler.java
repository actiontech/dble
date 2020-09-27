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
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.services.manager.response.ShowConnection;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.*;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    public void handle(ShardingService service, FieldPacket[] fields0, MySqlSelectQueryBlock mySqlSelectQueryBlock, String tableAlias) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                sqlhandle(service, fields0, mySqlSelectQueryBlock, tableAlias);
            }
        });
    }

    public void sqlhandle(ShardingService shardingService, FieldPacket[] fields0, MySqlSelectQueryBlock mySqlSelectQueryBlock, String tableAlias) {
        SQLExpr whereExpr = mySqlSelectQueryBlock.getWhere();

        Map<String, String> whereInfo = new HashMap<>();
        ShowConnection.getWhereCondition(whereExpr, whereInfo);

        String cSchema = null;
        String table = null;

        // the where condition should be contain table_schema, table_name equivalence judgment
        if ((cSchema = containsKeyIngoreCase(whereInfo, INFORMATION_SCHEMACOLUMNS_COLS[1])) == null || (table = containsKeyIngoreCase(whereInfo, INFORMATION_SCHEMACOLUMNS_COLS[2])) == null) {
            MysqlSystemSchemaHandler.doWrite(fields0.length, fields0, null, shardingService);
            return;
        }

        SchemaConfig schemaConfig = null;
        if ((schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(cSchema)) == null) {
            shardingService.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig userConfig = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(shardingService.getUser()));
        if (userConfig == null || !userConfig.getSchemas().contains(cSchema)) {
            shardingService.writeErrMessage("42000", "Access denied for user '" + shardingService.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        String shardingNode = null;
        if (!schemaConfig.getTables().containsKey(table)) {
            if ((shardingNode = schemaConfig.getShardingNode()) == null) {
                shardingService.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + cSchema + "." + table + "] doesn't exist");
                return;
            }
        } else {
            BaseTableConfig tableConfig = schemaConfig.getTables().get(table);
            if (tableConfig == null) {
                shardingService.writeErrMessage(ErrorCode.ER_YES, "The table " + table + " doesn‘t exist");
                return;
            }
            shardingNode = tableConfig.getShardingNodes().get(0);
        }

        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
        String shardingDataBase = dn.getDatabase();

        List<SQLSelectItem> selectItems = mySqlSelectQueryBlock.getSelectList();

        List<String> sle = new ArrayList<>();
        List<String> sle2 = new ArrayList<>();
        for (int i = 0; i < selectItems.size(); i++) {
            SQLSelectItem selectItem = selectItems.get(i);
            if (selectItem.toString().equals("*") || (selectItem.getExpr() instanceof SQLPropertyExpr && ((SQLPropertyExpr) selectItem.getExpr()).getName().equals("*"))) {
                sle2.addAll(Arrays.asList(INFORMATION_SCHEMACOLUMNS_COLS));
            } else {
                sle2.add(selectItem.getAlias() == null ? selectItem.toString() : selectItem.getAlias());
            }
            sle.add(selectItem.toString());
        }

        FieldPacket[] fields = new FieldPacket[sle2.size()];
        for (int i = 0; i < sle2.size(); i++) {
            fields[i] = PacketUtil.getField(sle2.get(i), Fields.FIELD_TYPE_VAR_STRING);
        }
        replaceSchema(whereExpr, shardingDataBase);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").
                append(StringUtils.join(sle, ", ") + " ").
                append("FROM INFORMATION_SCHEMA.COLUMNS " + (tableAlias == null ? "" : "AS " + tableAlias) + " WHERE ").
                append(whereExpr.toString());

        PhysicalDbInstance ds = dn.getDbGroup().getWriteDbInstance();
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(sle2.toArray(new String[sle2.size()]), new SelectInformationSchemaColumnsListener(shardingNode));
        if (ds.isAlive()) {
            SQLJob sqlJob = new SQLJob(sql.toString(), null, resultHandler, ds);
            sqlJob.run();
        } else {
            SQLJob sqlJob = new SQLJob(sql.toString(), shardingNode, resultHandler, false);
            sqlJob.run();
        }

        waitDone();

        if (!success) {
            shardingService.writeErrMessage(ErrorCode.ER_YES, "occur Exception, so see dble.log to check reason");
            return;
        }

        RowDataPacket[] rows = null;
        if (result != null) {
            rows = new RowDataPacket[result.size()];
            int index = 0;
            for (Map<String, String> data : result) {
                RowDataPacket row = new RowDataPacket(fields.length);
                for (String col : sle2) {
                    row.add(null == data.get(col) ? null : StringUtil.encode(data.get(col), shardingService.getCharset().getResults()));
                }
                rows[index++] = row;
            }
        }
        MysqlSystemSchemaHandler.doWrite(fields.length, fields, rows, shardingService);
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
