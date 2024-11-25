/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import org.apache.commons.lang.StringUtils;

public class NamedField {
    private final String schema;
    private final String table;
    private final String name;
    private final int hashCode;
    // which node of the field belong
    public final PlanNode planNode;
    private int charsetIndex;
    private boolean isNatualShow = true; // when natual join ,use it

    public NamedField(String inputSchema, String inputTable, String name, PlanNode planNode, boolean isNatualShow) {
        String tempTableSchmea;
        String tempTableName;
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tempTableSchmea = inputSchema == null ? null : inputSchema.toLowerCase();
            tempTableName = inputTable == null ? null : inputTable.toLowerCase();
        } else {
            tempTableSchmea = inputSchema;
            tempTableName = inputTable;
        }
        this.schema = tempTableSchmea;
        this.table = tempTableName;
        this.name = name;
        this.planNode = planNode;

        //init hashCode
        int prime = 2;
        int hash = tempTableSchmea == null ? 0 : tempTableSchmea.hashCode();
        hash = hash * prime + (tempTableName == null ? 0 : tempTableName.hashCode());
        this.hashCode = hash * prime + (name == null ? 0 : name.toLowerCase().hashCode());
        this.isNatualShow = isNatualShow;
    }

    public NamedField(String inputSchema, String inputTable, String name, PlanNode planNode) {
        this(inputSchema, inputTable, name, planNode, true);
    }

    @Override
    public int hashCode() {

        return hashCode;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    public boolean isNatualShow() {
        return isNatualShow;
    }

    public void noShow() {
        isNatualShow = false;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof NamedField))
            return false;
        NamedField other = (NamedField) obj;
        return StringUtils.equals(schema, other.schema) && StringUtils.equals(table, other.table) && StringUtils.equalsIgnoreCase(name, other.name);
    }

    @Override
    public String toString() {
        return "schema:" + schema + "table:" + table + ",name:" + name;
    }
}
