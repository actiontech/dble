package io.mycat.meta.table;

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
import io.mycat.meta.protocol.StructureMeta.ColumnMeta;
import io.mycat.meta.protocol.StructureMeta.IndexMeta;
import io.mycat.meta.protocol.StructureMeta.TableMeta;
import io.mycat.util.StringUtil;

import java.util.*;

public class MetaHelper {
    public static enum INDEX_TYPE {
        PRI, UNI, MUL
    }

    public static final String PRIMARY = "PRIMARY";

    public static TableMeta initTableMeta(String table, SQLCreateTableStatement createStment, long timeStamp) {
        TableMeta.Builder tmBuilder = TableMeta.newBuilder();
        tmBuilder.setTableName(table);
        tmBuilder.setVersion(timeStamp);
        Set<String> indexNames = new HashSet<>();
        for (SQLTableElement tableElement : createStment.getTableElementList()) {
            if (tableElement instanceof SQLColumnDefinition) {
                SQLColumnDefinition column = (SQLColumnDefinition) tableElement;
                ColumnMeta.Builder cmBuilder = makeColumnMeta(tmBuilder, column, indexNames);
                if (cmBuilder.getAutoIncre()) {
                    tmBuilder.setAiColPos(tmBuilder.getColumnsCount());
                }
                tmBuilder.addColumns(cmBuilder.build());
            } else if (tableElement instanceof MySqlPrimaryKey) {
                MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) tableElement;
                tmBuilder.setPrimary(makeIndexMeta(PRIMARY, INDEX_TYPE.PRI, primaryKey.getColumns()));
            } else if (tableElement instanceof MySqlUnique) {
                MySqlUnique unique = (MySqlUnique) tableElement;
                String indexName = genIndexName(unique.getName(), unique.getColumns(), indexNames);
                tmBuilder.addUniIndex(makeIndexMeta(indexName, INDEX_TYPE.UNI, unique.getColumns()));
            } else if (tableElement instanceof MySqlTableIndex) {
                MySqlTableIndex index = (MySqlTableIndex) tableElement;
                String indexName = genIndexName(index.getName(), index.getColumns(), indexNames);
                tmBuilder.addIndex(makeIndexMeta(indexName, INDEX_TYPE.MUL, index.getColumns()));
            } else if (tableElement instanceof MySqlKey) {
                MySqlKey index = (MySqlKey) tableElement;
                String indexName = genIndexName(index.getName(), index.getColumns(), indexNames);
                tmBuilder.addIndex(makeIndexMeta(indexName, INDEX_TYPE.MUL, index.getColumns()));
            } else {
                // ignore
            }
        }
        return tmBuilder.build();
    }

    public static String genIndexName(SQLName srcIndexName, List<SQLExpr> columnExprs, Set<String> indexNames) {
        String indexName;
        if (srcIndexName != null) {
            indexName = StringUtil.removeBackQuote(srcIndexName.getSimpleName());
        } else {
            SQLIdentifierExpr column = (SQLIdentifierExpr) columnExprs.get(0);
            String columnName = StringUtil.removeBackQuote(column.getName());
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
     * @param indexName
     * @param indexType
     * @param columnExprs
     * @return
     */
    public static IndexMeta makeIndexMeta(String indexName, INDEX_TYPE indexType, List<SQLExpr> columnExprs) {
        IndexMeta.Builder indexBuilder = IndexMeta.newBuilder();
        indexBuilder.setName(StringUtil.removeBackQuote(indexName));
        indexBuilder.setType(indexType.toString());
        for (int i = 0; i < columnExprs.size(); i++) {
            if (columnExprs.get(i) instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr column = (SQLIdentifierExpr) columnExprs.get(i);
                indexBuilder.addColumns(StringUtil.removeBackQuote(column.getName()));
            } else if (columnExprs.get(i) instanceof MySqlOrderingExpr) {
                MySqlOrderingExpr column = (MySqlOrderingExpr) columnExprs.get(i);
                if (column.getExpr() instanceof SQLIdentifierExpr) {
                    indexBuilder.addColumns(StringUtil.removeBackQuote(((SQLIdentifierExpr) column.getExpr()).getName()));
                } else if (column.getExpr() instanceof SQLMethodInvokeExpr) {
                    indexBuilder.addColumns(StringUtil.removeBackQuote(((SQLMethodInvokeExpr) column.getExpr()).getMethodName()));
                }
            }
        }
        return indexBuilder.build();
    }

    public static ColumnMeta.Builder makeColumnMeta(TableMeta.Builder tmBuilder, SQLColumnDefinition column, Set<String> indexNames) {
        ColumnMeta.Builder cmBuilder = ColumnMeta.newBuilder();
        cmBuilder.setName(StringUtil.removeBackQuote(column.getName().getSimpleName()));
        cmBuilder.setDataType(column.getDataType().getName());
        for (SQLColumnConstraint constraint : column.getConstraints()) {
            if (constraint instanceof SQLNotNullConstraint) {
                cmBuilder.setCanNull(false);
            } else if (constraint instanceof SQLNullConstraint) {
                cmBuilder.setCanNull(true);
            } else if (constraint instanceof SQLColumnPrimaryKey) {
                tmBuilder.setPrimary(makeIndexMeta(PRIMARY, INDEX_TYPE.PRI, new ArrayList<SQLExpr>(Arrays.asList(column.getName()))));
            } else if (constraint instanceof SQLColumnUniqueKey) {
                List<SQLExpr> columnExprs = new ArrayList<SQLExpr>(Arrays.asList(column.getName()));
                String indexName = genIndexName(null, columnExprs, indexNames);
                tmBuilder.addUniIndex(makeIndexMeta(indexName, INDEX_TYPE.UNI, columnExprs));
            }
        }
        if (column.getDefaultExpr() != null) {
            StringBuilder builder = new StringBuilder();
            MySqlOutputVisitor visitor = new MySqlOutputVisitor(builder);
            column.getDefaultExpr().accept(visitor);
            cmBuilder.setSdefault(builder.toString());
        }
        if (column.isAutoIncrement()) {
            cmBuilder.setAutoIncre(true);
        }
        return cmBuilder;
    }
}
