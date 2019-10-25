/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.plan.node.PlanNode;
import org.apache.commons.lang.StringUtils;

public class NamedField {
    private final String table;
    private final int hashCode;
    // which node of the field belong
    public final PlanNode planNode;

    private final NamedFieldDetail namedFieldDetail;

    public NamedField(String inputSchema, String inputTable, String name, PlanNode planNode) {
        String tempTableSchmea;
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tempTableSchmea = inputTable == null ? null : inputTable.toLowerCase();
        } else {
            tempTableSchmea = inputTable;
        }
        this.namedFieldDetail = new NamedFieldDetail(inputSchema, name);
        this.table = tempTableSchmea;
        this.planNode = planNode;
        this.hashCode = namedFieldDetail.hashCode() * 2 + (tempTableSchmea == null ? 0 : tempTableSchmea.hashCode());
    }

    public NamedField(String inputTable, int hash, NamedFieldDetail namedFieldDetail, PlanNode planNode) {
        this.namedFieldDetail = namedFieldDetail;
        this.planNode = planNode;
        this.table = inputTable.toLowerCase();
        hashCode = namedFieldDetail.hashCode() * 2 + hash;
    }

    public NamedField(NamedField oldField) {
        this.namedFieldDetail = oldField.namedFieldDetail;
        this.planNode = oldField.planNode;
        this.table = oldField.table;
        hashCode = oldField.hashCode();
    }

    @Override
    public int hashCode() {

        return hashCode;
    }

    public String getSchema() {
        return namedFieldDetail.getSchema();
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return namedFieldDetail.getName();
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
        if (other.hashCode() != this.hashCode()) {
            return false;
        }
        return StringUtils.equals(namedFieldDetail.getSchema(), other.namedFieldDetail.getSchema()) &&
                StringUtils.equals(table, other.table) && StringUtils.equalsIgnoreCase(namedFieldDetail.getName(), other.namedFieldDetail.getName());
    }

    @Override
    public String toString() {
        return "schema:" + namedFieldDetail.getSchema() + "table:" + table + ",name:" + namedFieldDetail.getName();
    }
}
