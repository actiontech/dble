/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.common.collect.Sets;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ManagerWritableTable extends ManagerBaseTable {
    private final ReentrantLock lock = new ReentrantLock();

    private LinkedHashSet<String> mustSetColumns = new LinkedHashSet<>();
    private LinkedHashSet<String> notNullColumns = new LinkedHashSet<>();
    private LinkedHashSet<String> primaryKeyColumns = new LinkedHashSet<>();

    private Set<String> notWritableColumnSet = Sets.newHashSet();
    private Set<String> logicalPrimaryKeySet = Sets.newHashSet();
    private String xmlFilePath;


    protected ManagerWritableTable(String tableName, int filedSize) {
        super(tableName, filedSize);
        this.isWritable = true;
        for (Map.Entry<String, ColumnMeta> column : columns.entrySet()) {
            String columnName = column.getKey();
            ColumnMeta columnMeta = column.getValue();
            if (!columnMeta.isCanNull()) {
                notNullColumns.add(columnName);
                if (columnMeta.getDefaultVal() == null) {
                    mustSetColumns.add(columnName);
                }
            }
            if (columnMeta.isPrimaryKey()) {
                primaryKeyColumns.add(columnName);
            }
        }
    }

    public void setNotWritableColumnSet(String... columns) {
        Collections.addAll(this.notWritableColumnSet, columns);
    }

    public void setLogicalPrimaryKeySet(String... columns) {
        Collections.addAll(this.logicalPrimaryKeySet, columns);
    }

    public Set<String> getNotWritableColumnSet() {
        return notWritableColumnSet;
    }

    public Set<String> getLogicalPrimaryKeySet() {
        return logicalPrimaryKeySet;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public LinkedHashSet<String> getNotNullColumns() {
        return notNullColumns;
    }

    public LinkedHashSet<String> getMustSetColumns() {
        return mustSetColumns;
    }

    public LinkedHashSet<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public String getXmlFilePath() {
        return xmlFilePath;
    }

    public void setXmlFilePath(String xmlFilePath) {
        this.xmlFilePath = xmlFilePath;
    }

    public List<LinkedHashMap<String, String>> makeInsertRows(List<String> insertColumns, List<SQLInsertStatement.ValuesClause> values) throws SQLException {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(values.size());
        for (SQLInsertStatement.ValuesClause valuesClause : values) {
            List<SQLExpr> value = valuesClause.getValues();
            LinkedHashMap<String, String> row = new LinkedHashMap<>();
            int index;
            for (Map.Entry<String, ColumnMeta> column : columns.entrySet()) {
                String columnName = column.getKey();
                String insertColumn;
                index = insertColumns.indexOf(columnName);
                String insertColumnVal;
                if (-1 != index && insertColumns.size() > index && columnName.equals(insertColumn = insertColumns.get(index))) {
                    insertColumnVal = ManagerTableUtil.valueToString(value.get(index));
                    if (this.notWritableColumnSet.contains(columnName) && !StringUtil.isEmpty(insertColumnVal)) {
                        throw new SQLException("Column '" + insertColumn + "' is not writable", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
                    }
                    if (null == insertColumnVal) {
                        insertColumnVal = column.getValue().getDefaultVal();
                    }
                } else {
                    insertColumnVal = column.getValue().getDefaultVal();
                }
                row.put(columnName, insertColumnVal);
            }
            lst.add(row);
        }
        return lst;
    }


    public void checkPrimaryKeyDuplicate(List<LinkedHashMap<String, String>> rows) throws SQLException {
        List<LinkedHashMap<String, String>> originRows = getRows();
        Set<String> pks = new HashSet<>(originRows.size() + rows.size());

        for (LinkedHashMap<String, String> originRow : originRows) {
            StringBuilder pk = new StringBuilder();
            for (String pkColumn : primaryKeyColumns) {
                if (pk.length() > 0) {
                    pk.append("-");
                }
                pk.append(originRow.get(pkColumn));
            }
            pks.add(pk.toString());
        }
        for (LinkedHashMap<String, String> row : rows) {
            StringBuilder pk = new StringBuilder();
            for (String pkColumn : primaryKeyColumns) {
                if (pk.length() > 0) {
                    pk.append("-");
                }
                pk.append(row.get(pkColumn));
            }
            String pkValue = pk.toString();
            if (pks.contains(pkValue)) {
                throw new SQLException("Duplicate entry '" + pkValue + "' for key 'PRIMARY'", "23000", ErrorCode.ER_DUP_ENTRY);
            } else if (!StringUtil.isBlank(pkValue) && !StringUtil.equalsIgnoreCase(pkValue, "null")) {
                pks.add(pkValue);
            }
        }
    }

    /**
     * @param rows rows wanted to be insert
     * @throws SQLException eg.: new SQLException("Access denied for table '" + tableName + "'", "42000", ErrorCode.ER_ACCESS_DENIED_ERROR);
     */
    public abstract int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException;

    public abstract int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException;

    public abstract int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException;

    @Override
    public List<RowDataPacket> getRow(LinkedHashSet<Item> realSelects, String charset) {
        lock.lock();
        try {
            return super.getRow(realSelects, charset);
        } finally {
            lock.unlock();
        }
    }

    public void afterExecute() {
    }

    public void updateTempConfig() {
    }
}
