package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.services.manager.dump.DumpException;
import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.sql.SQLNonTransientException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertHandler extends DefaultHandler {

    private static final Pattern INSERT_STMT = Pattern.compile("insert\\s+(ignore\\s+)?into\\s+`?(.*)`\\s+values", Pattern.CASE_INSENSITIVE);
    private final ShardingValuesHandler shardingValuesHandler = new ShardingValuesHandler();
    private final DefaultValuesHandler defaultValuesHandler = new DefaultValuesHandler();
    private DefaultValuesHandler valuesHandler;
    private String currentTable;

    @Override
    public SQLStatement preHandle(DumpFileContext context, String stmt) throws DumpException, SQLNonTransientException {
        // get table name simply
        String table = null;
        Matcher matcher = InsertHandler.INSERT_STMT.matcher(stmt);
        if (matcher.find()) {
            table = matcher.group(2);
        }
        context.setTable(table);
        if (table != null && table.equalsIgnoreCase(currentTable)) {
            if (context.isSkipContext() || !(context.getTableConfig() instanceof ShardingTableConfig)) {
                return null;
            }
            return RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
        } else {
            currentTable = table;
        }

        if (context.isSkipContext() || !(context.getTableConfig() instanceof ShardingTableConfig)) {
            return null;
        }

        MySqlInsertStatement insert = (MySqlInsertStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
        // check columns from insert columns
        checkColumns(context, insert.getColumns());
        // add
        StringBuilder insertHeader = new StringBuilder("INSERT ");
        if (insert.isIgnore() || context.getConfig().isIgnore()) {
            insert.setIgnore(true);
            insertHeader.append("IGNORE ");
        }
        insertHeader.append("INTO ");
        insertHeader.append("`");
        insertHeader.append(context.getTable());
        insertHeader.append("`");
        if (!CollectionUtil.isEmpty(insert.getColumns())) {
            insertHeader.append(insert.getColumns().toString());
        }
        insertHeader.append(" VALUES");
        if (context.getTableConfig() instanceof ShardingTableConfig) {
            shardingValuesHandler.reset();
            valuesHandler = shardingValuesHandler;
        } else {
            valuesHandler = defaultValuesHandler;
        }
        valuesHandler.setInsertHeader(insertHeader);
        return insert;
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        MySqlInsertStatement insert = (MySqlInsertStatement) sqlStatement;
        SQLInsertStatement.ValuesClause valueClause;

        valuesHandler.preProcess(context);
        for (int i = 0; i < insert.getValuesList().size(); i++) {
            valueClause = insert.getValuesList().get(i);
            try {
                processIncrementColumn(context, valueClause.getValues());
                valuesHandler.process(context, valueClause.getValues(), i == 0);
            } catch (SQLNonTransientException e) {
                context.addError(e.getMessage());
            }
        }
        valuesHandler.postProcess(context);
    }

    @Override
    public void handle(DumpFileContext context, String stmt) throws InterruptedException {
        if (context.getConfig().isIgnore()) {
            Matcher matcher = InsertHandler.INSERT_STMT.matcher(stmt);
            if (matcher.find() && matcher.group(1) == null) {
                stmt = stmt.replaceFirst("(?i)(insert\\s+into)", "INSERT IGNORE INTO");
            }
        }
        super.handle(context, stmt);
    }

    private void processIncrementColumn(DumpFileContext context, List<SQLExpr> values) throws SQLNonTransientException {
        int incrementIndex = context.getIncrementColumnIndex();
        if (incrementIndex == -1) {
            return;
        }

        String tableKey = StringUtil.getFullName(context.getSchema(), context.getTable());
        long val = SequenceManager.getHandler().nextId(tableKey);
        SQLExpr value = values.get(incrementIndex);
        if (!StringUtil.isEmpty(SQLUtils.toMySqlString(value)) && !context.isNeedSkipError()) {
            context.addError("For table using global sequence, dble has set increment column values for you.");
            context.setNeedSkipError(true);
        }
        values.set(incrementIndex, new SQLIntegerExpr(val));
    }

    /**
     * if sharding column index or increment column index is -1,
     * find from dble meta data or columns in insert statement
     *
     * @param context context
     * @param columns columns
     * @throws DumpException
     * @throws SQLNonTransientException
     */
    private void checkColumns(DumpFileContext context, List<SQLExpr> columns) throws DumpException, SQLNonTransientException {
        int partitionColumnIndex = context.getPartitionColumnIndex();
        int incrementColumnIndex = context.getIncrementColumnIndex();

        ShardingTableConfig tableConfig = (ShardingTableConfig) (context.getTableConfig());
        // partition column check
        if ((tableConfig.getShardingColumn() != null && partitionColumnIndex != -1) ||
                (tableConfig.getIncrementColumn() != null && incrementColumnIndex != -1)) {
            return;
        }

        if (tableConfig.getIncrementColumn() != null || tableConfig.getShardingColumn() != null) {
            if (!CollectionUtil.isEmpty(columns)) {
                for (int i = 0; i < columns.size(); i++) {
                    SQLExpr column = columns.get(i);
                    String columnName = StringUtil.removeBackQuote(column.toString());
                    if (tableConfig.getIncrementColumn() != null && columnName.equalsIgnoreCase(tableConfig.getIncrementColumn())) {
                        incrementColumnIndex = i;
                    }
                    if (columnName.equalsIgnoreCase(tableConfig.getShardingColumn())) {
                        partitionColumnIndex = i;
                    }
                }
            } else {
                TableMeta tableMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(context.getSchema(), context.getTable());
                if (tableMeta == null) {
                    throw new DumpException("can't find meta of table and the table has no create statement.");
                }

                for (int i = 0; i < tableMeta.getColumns().size(); i++) {
                    ColumnMeta column = tableMeta.getColumns().get(i);
                    String columnName = column.getName();
                    if (tableConfig.getIncrementColumn() != null && columnName.equalsIgnoreCase(tableConfig.getIncrementColumn())) {
                        incrementColumnIndex = i;
                    }
                    if (columnName.equalsIgnoreCase(tableConfig.getShardingColumn())) {
                        partitionColumnIndex = i;
                    }
                }
            }
        }

        // partition column check
        if (tableConfig.getShardingColumn() != null && partitionColumnIndex == -1) {
            throw new DumpException("can't find partition column in insert.");
        }
        // increment column check
        if (tableConfig.getIncrementColumn() != null && incrementColumnIndex == -1) {
            throw new DumpException("can't find increment column in insert.");
        }
        context.setIncrementColumnIndex(incrementColumnIndex);
        context.setPartitionColumnIndex(partitionColumnIndex);
    }
}
