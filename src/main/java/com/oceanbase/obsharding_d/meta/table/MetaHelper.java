/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta.table;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.meta.ViewMeta;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class MetaHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaHelper.class);

    private MetaHelper() {
    }

    static ViewMeta initViewMeta(String schema, String sql, long timeStamp, ProxyMetaManager tmManager) {
        if (sql == null) {
            return null;
        }

        int viewIndex = sql.indexOf("VIEW");
        String str = sql.substring(viewIndex);
        ViewMeta meta = null;
        try {
            meta = new ViewMeta(schema, "CREATE " + str, tmManager);
            meta.init();
            meta.setTimestamp(timeStamp);
        } catch (Exception e) {
            LOGGER.warn("sql[" + sql + "] parser error:", e);
        }
        return meta;
    }

    static TableMeta initTableMeta(String table, String sql, long timeStamp, String schema) {
        if (sql == null) {
            return null;
        }

        try {
            SQLStatementParser parser = new OBsharding_DCreateTableParser(sql);
            SQLCreateTableStatement createStatement = parser.parseCreateTable();
            return MetaHelper.initTableMeta(table, createStatement, timeStamp, schema);
        } catch (Exception e) {
            LOGGER.warn("sql[" + sql + "] parser error:", e);
            AlertUtil.alertSelf(AlarmCode.GET_TABLE_META_FAIL, Alert.AlertLevel.WARN, "sql[" + sql + "] parser error:" + e.getMessage(), null);
            return null;
        }
    }

    private static TableMeta initTableMeta(String table, SQLCreateTableStatement createStatement, long timeStamp, String schema) {
        TableMeta tableMeta = new TableMeta();
        tableMeta.setTableName(table);
        tableMeta.setVersion(timeStamp);
        tableMeta.setSchemaName(schema);
        tableMeta.setCreateSql(createStatement.toString());

        List<ColumnMeta> columns = new ArrayList<>(createStatement.getTableElementList().size());
        for (SQLTableElement tableElement : createStatement.getTableElementList()) {
            if (tableElement instanceof SQLColumnDefinition) {
                columns.add(new ColumnMeta((SQLColumnDefinition) tableElement));
            }
        }
        tableMeta.setColumns(columns);
        return tableMeta;
    }

    public static String electionShardingColumn(String sql) {

        SQLStatementParser parser = new OBsharding_DCreateTableParser(sql);
        SQLCreateTableStatement createStatement = parser.parseCreateTable();

        Set<String> unavailable = new HashSet<>();
        Set<String> available = new LinkedHashSet<>();

        for (SQLTableElement tableElement : createStatement.getTableElementList()) {
            if (tableElement instanceof SQLColumnDefinition) {
                SQLColumnDefinition columnElement = (SQLColumnDefinition) tableElement;
                String column = StringUtil.removeBackQuote((columnElement).getName().getSimpleName());
                if (columnElement.isAutoIncrement())
                    unavailable.add(column);
                else
                    available.add(column);
            } else if (tableElement instanceof MySqlPrimaryKey || tableElement instanceof MySqlUnique || tableElement instanceof MySqlKey) {
                for (SQLSelectOrderByItem item : ((MySqlKey) tableElement).getIndexDefinition().getColumns()) {
                    String column = StringUtil.removeBackQuote(item.getExpr().toString());
                    if (unavailable.contains(column))
                        continue;
                    else
                        return column;
                }
            }
        }

        if (!CollectionUtil.isEmpty(available)) {
            Iterator<String> iterator = available.iterator();
            while (iterator.hasNext()) {
                String column = iterator.next();
                if ("id".equalsIgnoreCase(column))
                    return column;
            }
            return available.iterator().next();
        } else {
            return StringUtil.removeBackQuote(((SQLColumnDefinition) createStatement.getTableElementList().get(0)).getName().getSimpleName());
        }
    }


}
