/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

final class MetaHelper {
    private static final String PRIMARY = "PRIMARY";
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
            meta.init(false);
            meta.setTimestamp(timeStamp);
        } catch (Exception e) {
            LOGGER.warn("sql[" + sql + "] parser error:", e);
        }
        return meta;
    }

    static StructureMeta.TableMeta initTableMeta(String table, String sql, long timeStamp) {
        if (sql == null) {
            return null;
        }
        try {
            SQLStatementParser parser = new DbleCreateTableParser(sql);
            SQLCreateTableStatement createStatement = parser.parseCreateTable();
            return MetaHelper.initTableMeta(table, createStatement, timeStamp);
        } catch (Exception e) {
            LOGGER.warn("sql[" + sql + "] parser error:", e);
            AlertUtil.alertSelf(AlarmCode.GET_TABLE_META_FAIL, Alert.AlertLevel.WARN, "sql[" + sql + "] parser error:" + e.getMessage(), null);
            return null;
        }
    }

    private static StructureMeta.TableMeta initTableMeta(String table, SQLCreateTableStatement createStatement, long timeStamp) {
        StructureMeta.TableMeta.Builder tmBuilder = StructureMeta.TableMeta.newBuilder();
        tmBuilder.setTableName(table);
        tmBuilder.setVersion(timeStamp);
        tmBuilder.setCreateSql(createStatement.toString());
        Set<String> indexNames = new HashSet<>();
        for (SQLTableElement tableElement : createStatement.getTableElementList()) {
            if (tableElement instanceof SQLColumnDefinition) {
                SQLColumnDefinition column = (SQLColumnDefinition) tableElement;
                StructureMeta.ColumnMeta.Builder cmBuilder = makeColumnMeta(tmBuilder, column, indexNames);
                if (cmBuilder.getAutoIncre()) {
                    tmBuilder.setAiColPos(tmBuilder.getColumnsCount());
                }
                tmBuilder.addColumns(cmBuilder.build());
            } else if (tableElement instanceof MySqlPrimaryKey) {
                MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) tableElement;
                tmBuilder.setPrimary(makeIndexMeta(PRIMARY, IndexType.PRI, primaryKey.getColumns()));
            } else if (tableElement instanceof MySqlUnique) {
                MySqlUnique unique = (MySqlUnique) tableElement;
                String indexName = genIndexName(unique.getName(), unique.getColumns(), indexNames);
                tmBuilder.addUniIndex(makeIndexMeta(indexName, IndexType.UNI, unique.getColumns()));
            } else if (tableElement instanceof MySqlTableIndex) {
                MySqlTableIndex index = (MySqlTableIndex) tableElement;
                String indexName = genIndexName(index.getName(), index.getColumns(), indexNames);
                tmBuilder.addIndex(makeIndexMeta(indexName, IndexType.MUL, index.getColumns()));
            } else if (tableElement instanceof MySqlKey) {
                MySqlKey index = (MySqlKey) tableElement;
                String indexName = genIndexName(index.getName(), index.getColumns(), indexNames);
                tmBuilder.addIndex(makeIndexMeta(indexName, IndexType.MUL, index.getColumns()));
            } else {
                // ignore
            }
        }
        return tmBuilder.build();
    }

    private static String genIndexName(SQLName srcIndexName, List<SQLSelectOrderByItem> selectOrderByItemList, Set<String> indexNames) {
        String indexName;
        if (srcIndexName != null) {
            indexName = StringUtil.removeBackAndDoubleQuote(srcIndexName.getSimpleName());
        } else {
            SQLExpr firstColumn = selectOrderByItemList.get(0).getExpr();
            String columnName = firstColumn.toString();
            if (firstColumn instanceof SQLIdentifierExpr) {
                columnName = StringUtil.removeBackAndDoubleQuote(((SQLIdentifierExpr) firstColumn).getName());
            } else if (firstColumn instanceof SQLMethodInvokeExpr) {
                columnName = StringUtil.removeBackAndDoubleQuote(((SQLMethodInvokeExpr) firstColumn).getMethodName());
            }
            indexName = columnName;
            int indexNum = 1;
            while (indexNames.contains(indexName)) {
                indexNum++;
                indexName = columnName + "_" + indexNum;
            }
        }
        indexNames.add(indexName);
        return indexName;
    }

    /**
     * the "`" will be removed
     *
     * @param indexName String
     * @param indexType IndexType
     * @param selectOrderByItemList List<SQLSelectOrderByItem>
     * @return StructureMeta.IndexMeta
     */
    private static StructureMeta.IndexMeta makeIndexMeta(String indexName, IndexType indexType, List<SQLSelectOrderByItem> selectOrderByItemList) {
        StructureMeta.IndexMeta.Builder indexBuilder = StructureMeta.IndexMeta.newBuilder();
        indexBuilder.setName(StringUtil.removeBackAndDoubleQuote(indexName));
        indexBuilder.setType(indexType.toString());
        for (SQLSelectOrderByItem selectOrderByItem : selectOrderByItemList) {
            SQLExpr columnExpr = selectOrderByItem.getExpr();
            if (columnExpr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr column = (SQLIdentifierExpr) columnExpr;
                indexBuilder.addColumns(StringUtil.removeBackAndDoubleQuote(column.getName()));
            } else if (columnExpr instanceof MySqlOrderingExpr) {
                MySqlOrderingExpr column = (MySqlOrderingExpr) columnExpr;
                if (column.getExpr() instanceof SQLIdentifierExpr) {
                    indexBuilder.addColumns(StringUtil.removeBackAndDoubleQuote(((SQLIdentifierExpr) column.getExpr()).getName()));
                } else if (column.getExpr() instanceof SQLMethodInvokeExpr) {
                    indexBuilder.addColumns(StringUtil.removeBackAndDoubleQuote(((SQLMethodInvokeExpr) column.getExpr()).getMethodName()));
                }
            }
        }
        return indexBuilder.build();
    }

    private static StructureMeta.ColumnMeta.Builder makeColumnMeta(StructureMeta.TableMeta.Builder tmBuilder, SQLColumnDefinition column, Set<String> indexNames) {
        StructureMeta.ColumnMeta.Builder cmBuilder = StructureMeta.ColumnMeta.newBuilder().setCanNull(true);
        cmBuilder.setName(StringUtil.removeBackAndDoubleQuote(column.getName().getSimpleName()));
        cmBuilder.setDataType(column.getDataType().getName());
        for (SQLColumnConstraint constraint : column.getConstraints()) {
            if (constraint instanceof SQLNotNullConstraint) {
                cmBuilder.setCanNull(false);
            } else if (constraint instanceof SQLNullConstraint) {
                cmBuilder.setCanNull(true);
            } else if (constraint instanceof SQLColumnPrimaryKey) {
                tmBuilder.setPrimary(makeIndexMeta(PRIMARY, IndexType.PRI, new ArrayList<>(Collections.singletonList(new SQLSelectOrderByItem(column.getName())))));
            } else if (constraint instanceof SQLColumnUniqueKey) {
                List<SQLSelectOrderByItem> columnItems = new ArrayList<>(Collections.singletonList(new SQLSelectOrderByItem(column.getName())));
                String indexName = genIndexName(null, columnItems, indexNames);
                tmBuilder.addUniIndex(makeIndexMeta(indexName, IndexType.UNI, columnItems));
            }
        }
        if (column.getDefaultExpr() != null) {
            StringBuilder builder = new StringBuilder();
            MySqlOutputVisitor visitor = new MySqlOutputVisitor(builder);
            visitor.setShardingSupport(false);
            column.getDefaultExpr().accept(visitor);
            cmBuilder.setSdefault(builder.toString());
        }
        if (column.isAutoIncrement()) {
            cmBuilder.setAutoIncre(true);
        }
        return cmBuilder;
    }

    public enum IndexType {
        PRI, UNI, MUL
    }
}
