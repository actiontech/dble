/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.plan.util.ToStringUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * NoNameNode eg:select 1,only exists selecteditems
 *
 * @author ActionTech
 */

public class NoNameNode extends PlanNode {

    private final String catalog;

    public PlanNodeType type() {
        return PlanNodeType.NONAME;
    }

    public NoNameNode(String catalog, String sql) {
        this.catalog = catalog;
        this.sql = sql;
        Set<String> set = new HashSet<>();
        set.addAll(DbleServer.getInstance().getConfig().getDataNodes().keySet());
        this.setNoshardNode(set);
        this.keepFieldSchema = true;
    }

    @Override
    public NoNameNode copy() {
        NoNameNode noNameNode = new NoNameNode(catalog, sql);
        this.copySelfTo(noNameNode);
        return noNameNode;
    }

    @Override
    public String getPureName() {
        return "";
    }

    @Override
    public String getPureSchema() {
        return "";
    }


    public String getCatalog() {
        return this.catalog;
    }

    @Override
    public int getHeight() {
        return 0;
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);

        if (this.getAlias() != null) {
            ToStringUtil.appendln(sb, tabTittle + "Query from[ " + this.getSql() + " ] as " + this.getAlias());
        } else {
            ToStringUtil.appendln(sb, tabTittle + "Query from[ " + this.getSql() + " ]");
        }
        return sb.toString();
    }
}
