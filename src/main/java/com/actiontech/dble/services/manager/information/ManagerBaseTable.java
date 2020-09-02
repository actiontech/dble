/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information;

import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.util.StringUtil;

import java.util.*;

public abstract class ManagerBaseTable {
    protected final String tableName;
    protected boolean isWritable = false;
    protected final LinkedHashMap<String, ColumnMeta> columns;
    protected final LinkedHashMap<String, Integer> columnsType;

    protected ManagerBaseTable(String tableName, int filedSize) {
        this.tableName = tableName;
        this.columns = new LinkedHashMap<>(filedSize);
        this.columnsType = new LinkedHashMap<>(filedSize);
        initColumnAndType();
    }

    protected abstract void initColumnAndType();

    protected abstract List<LinkedHashMap<String, String>> getRows();


    public boolean isWritable() {
        return isWritable;
    }

    public List<RowDataPacket> getRow(LinkedHashSet<Item> realSelects, String charset) {
        List<LinkedHashMap<String, String>> lst = getRows();
        List<RowDataPacket> rows = new ArrayList<>(lst.size());
        for (LinkedHashMap<String, String> rowForTable : lst) {
            RowDataPacket row = new RowDataPacket(realSelects.size());
            for (Item select : realSelects) {
                if (select.basicConstItem()) {
                    row.add(StringUtil.encode(select.valStr(), charset));
                } else {
                    row.add(StringUtil.encode(rowForTable.get(select.getItemName()), charset));
                }
            }
            rows.add(row);
        }
        return rows;
    }


    public String getTableName() {
        return tableName;
    }

    public Collection<ColumnMeta> getColumnsMeta() {
        return columns.values();
    }

    public Set<String> getColumnNames() {
        return columns.keySet();
    }

    public Integer getColumnType(String columnName) {
        return columnsType.get(columnName);
    }
}
