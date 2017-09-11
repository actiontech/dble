/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.util.ToStringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(catalog);
        this.setNoshardNode(new HashSet<>(Collections.singletonList(schema.getMetaDataNode())));
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
    public List<TableNode> getReferedTableNodes() {
        return new ArrayList<>();
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
