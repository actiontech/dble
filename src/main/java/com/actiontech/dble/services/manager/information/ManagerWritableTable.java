/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ManagerWritableTable extends ManagerBaseTable {
    private final ReentrantLock lock = new ReentrantLock();

    private LinkedHashSet<String> mustSetColumns = new LinkedHashSet<>();
    private LinkedHashSet<String> notNullColumns = new LinkedHashSet<>();
    private LinkedHashSet<String> primaryKeyColumns = new LinkedHashSet<>();

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


    public List<LinkedHashMap<String, String>> makeInsertRows(List<String> insertColumns, List<SQLInsertStatement.ValuesClause> values) throws SQLException {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(values.size());
        for (SQLInsertStatement.ValuesClause valuesClause : values) {
            List<SQLExpr> value = valuesClause.getValues();
            LinkedHashMap<String, String> row = new LinkedHashMap<>();
            int index = 0;
            for (Map.Entry<String, ColumnMeta> column : columns.entrySet()) {
                String columnName = column.getKey();
                if (insertColumns.size() > index && columnName.equals(insertColumns.get(index))) {
                    row.put(columnName, ManagerTableUtil.valueToString(value.get(index)));
                    index++;
                } else {
                    row.put(columnName, column.getValue().getDefaultVal());
                }
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
            } else {
                pks.add(pkValue);
            }
        }
    }

    /**
     * @param rows rows wanted to be insert
     * @throws SQLException eg.: new SQLException("Access denied for table '" + tableName + "'", "42000", ErrorCode.ER_ACCESS_DENIED_ERROR);
     */
    public abstract void insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException;

    public abstract int updateRows(Set<LinkedHashMap<String, String>> affectPks, Map<String, String> values) throws SQLException;

    public abstract int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException;

    @Override
    public List<RowDataPacket> getRow(List<Item> realSelects, String charset) {
        lock.lock();
        try {
            return super.getRow(realSelects, charset);
        } finally {
            lock.unlock();
        }
    }
}
