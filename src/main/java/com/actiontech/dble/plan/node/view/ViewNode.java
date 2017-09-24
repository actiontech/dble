/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node.view;

import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.ToStringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * present a view
 *
 * @author ActionTech
 */
public class ViewNode extends QueryNode {
    public PlanNodeType type() {
        return PlanNodeType.VIEW;
    }

    private String catalog;
    private String viewname;
    private String createSql;

    public ViewNode(String catalog, String viewName, PlanNode selNode, String createSql) {
        super(selNode);
        this.catalog = catalog;
        this.viewname = viewName;
        this.createSql = createSql;
        this.getChild().setAlias(viewname);
    }

    public QueryNode toQueryNode() {
        QueryNode newNode = new QueryNode(this.getChild().copy());
        this.copySelfTo(newNode);
        return newNode;
    }

    public String getCatalog() {
        return catalog;
    }

    /**
     * getColumns
     *
     * @return
     */
    public List<StructureMeta.ColumnMeta> getColumns() {
        List<StructureMeta.ColumnMeta> cols = new ArrayList<>();
        for (Item sel : getColumnsSelected()) {
            String cn = sel.getAlias() == null ? sel.getItemName() : sel.getAlias();
            StructureMeta.ColumnMeta.Builder cmBuilder = StructureMeta.ColumnMeta.newBuilder();
            cmBuilder.setName(cn);
            cols.add(cmBuilder.build());
        }
        return cols;
    }

    /**
     * getReferedTables
     *
     * @return
     */
    public List<String> getReferedTables() {
        List<String> tls = new ArrayList<>();
        for (TableNode tn : this.getReferedTableNodes()) {
            tls.add(tn.getTableName());
        }
        return tls;
    }

    public String getCreateSql() {
        return createSql;
    }

    @Override
    public ViewNode copy() {
        PlanNode selCopy = this.getChild().copy();
        return new ViewNode(catalog, viewname, selCopy, createSql);
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        ToStringUtil.appendln(sb, tabTittle + "View  " + this.viewname + " AS: ");
        ToStringUtil.appendln(sb, this.getChild().toString(level + 1));
        return sb.toString();
    }

}
