/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.node;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.plan.util.ToStringUtil;
import com.oceanbase.obsharding_d.route.parser.druid.RouteTableConfigInfo;

import java.util.HashSet;

/**
 * NoNameNode eg:select 1,only exists selecteditems
 *
 * @author oceanbase
 */

public class NoNameNode extends PlanNode {

    private final String catalog;
    private boolean isFakeNode;

    public PlanNodeType type() {
        return PlanNodeType.NONAME;
    }

    public NoNameNode(String catalog, String sql) {
        this.catalog = catalog;
        this.sql = sql;
        this.setNoshardNode(new HashSet<>(OBsharding_DServer.getInstance().getConfig().getShardingNodes().keySet()));
        this.keepFieldSchema = true;
    }


    @Override
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        return null;
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

    public boolean isFakeNode() {
        return isFakeNode;
    }

    public void setFakeNode(boolean fakeNode) {
        isFakeNode = fakeNode;
    }
}
