/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;

public class ColumnMeta {
    private String name;
    private String dataType;
    private boolean canNull = true;
    private String defaultVal;
    private boolean isPrimaryKey = false; // used for manager table now

    public ColumnMeta(SQLColumnDefinition def) {
        this.name = StringUtil.removeBackAndDoubleQuote(def.getName().getSimpleName());
        this.dataType = def.getDataType().getName();
        for (SQLColumnConstraint constraint : def.getConstraints()) {
            if (constraint instanceof SQLNotNullConstraint) {
                this.canNull = false;
                break;
            }
        }
        this.defaultVal = null == def.getDefaultExpr() ? null : StringUtil.removeApostrophe(def.getDefaultExpr().toString());
    }

    // used for tables of dble_information
    public ColumnMeta(String name, String dataType, boolean canNull) {
        this.name = name;
        this.dataType = dataType;
        this.canNull = canNull;
    }
    // used for tables of dble_information
    public ColumnMeta(String name, String dataType, boolean canNull, boolean isPrimaryKey) {
        this(name, dataType, canNull);
        this.isPrimaryKey = isPrimaryKey;
    }

    public ColumnMeta(String name, String dataType, boolean canNull, String defaultVal) {
        this(name, dataType, canNull);
        this.defaultVal = defaultVal;
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public boolean isCanNull() {
        return canNull;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public String getDefaultVal() {
        return defaultVal;
    }
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ColumnMeta)) {
            return false;
        }
        ColumnMeta temp = (ColumnMeta) other;
        return name.equalsIgnoreCase(temp.getName()) && dataType.equals(temp.getDataType()) &&
                canNull == temp.isCanNull();
    }

    @Override
    public int hashCode() {
        int result = 31 + name.hashCode();
        result = 31 * result + dataType.hashCode();
        result = 31 * result + (canNull ? 1231 : 1237);

        return result;
    }
}
