/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.handler;

import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;
import com.oceanbase.obsharding_d.services.manager.dump.DumpException;
import com.oceanbase.obsharding_d.services.manager.dump.DumpFileContext;
import com.oceanbase.obsharding_d.services.manager.dump.parse.InsertParser;
import com.oceanbase.obsharding_d.services.manager.dump.parse.InsertQueryPos;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLNonTransientException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertHandler extends DefaultHandler {

    public static final Pattern INSERT_STMT = Pattern.compile("insert\\s+(ignore\\s+)?into\\s+`?(.*)`\\s+values", Pattern.CASE_INSENSITIVE);
    private final ShardingValuesHandler shardingValuesHandler = new ShardingValuesHandler();
    private final DefaultValuesHandler defaultValuesHandler = new DefaultValuesHandler();

    @Override
    public SQLStatement preHandle(DumpFileContext fileContext, String stmt) throws DumpException, SQLNonTransientException {
        // get table name simply
        int type = ServerParseFactory.getShardingParser().parse(stmt);

        InsertParser insertParser = new InsertParser(stmt);
        InsertQueryPos insertQueryPos = insertParser.parseStatement();
        insertQueryPos.setInsertString(stmt);
        String table = StringUtil.removeBackQuote(insertQueryPos.getTableName());
        fileContext.setTable(table);

        if (fileContext.isSkipContext() || !(fileContext.getTableConfig() instanceof ShardingTableConfig)) {
            if (!fileContext.isSkipContext() || type == ServerParse.UNLOCK) {
                try {
                    handleSQL(fileContext, stmt);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        // check columns from insert columns
        checkColumns(fileContext, insertQueryPos.getColumns());
        if (fileContext.getConfig().isIgnore()) {
            insertQueryPos.setIgnore(true);
        }
        DefaultValuesHandler valuesHandler;
        if (fileContext.getTableConfig() instanceof ShardingTableConfig) {
            valuesHandler = shardingValuesHandler;
        } else {
            valuesHandler = defaultValuesHandler;
        }
        handleStatement(fileContext, insertQueryPos, valuesHandler);
        return null;
    }

    public void handleStatement(DumpFileContext context, InsertQueryPos insertQueryPos, DefaultValuesHandler valuesHandler) {
        for (List<Pair<Integer, Integer>> valuePair : insertQueryPos.getValueItemsRange()) {
            try {
                valuesHandler.process(context, insertQueryPos, valuePair);
            } catch (SQLNonTransientException e) {
                context.addError(e.getMessage());
            }
        }
    }

    public void handleSQL(DumpFileContext context, String stmt) throws InterruptedException {
        if (context.getConfig().isIgnore()) {
            Matcher matcher = InsertHandler.INSERT_STMT.matcher(stmt);
            if (matcher.find() && matcher.group(1) == null) {
                stmt = stmt.replaceFirst("(?i)(insert\\s+into)", "INSERT IGNORE INTO");
            }
        }
        super.handle(context, stmt);
    }

    @Override
    public void handle(DumpFileContext context, SQLStatement sqlStatement) throws InterruptedException {
        return;
    }

    @Override
    public void handle(DumpFileContext context, String stmt) throws InterruptedException {
        return;
    }

    /**
     * if sharding column index or increment column index is -1,
     * find from OBsharding-D meta data or columns in insert statement
     *
     * @param context context
     * @param columns columns
     */
    private void checkColumns(DumpFileContext context, List<String> columns) throws DumpException, SQLNonTransientException {
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
                    String columnName = StringUtil.removeBackQuote(columns.get(i));
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
