/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.util.StringUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SchemaMeta {

    /**
     * <table,tableMeta>
     */
    private final ConcurrentMap<String, TableMeta> tableMetas;

    private volatile ConcurrentMap<String, ViewMeta> viewMetas;

    public SchemaMeta() {
        this.tableMetas = new ConcurrentHashMap<>();
        this.viewMetas = new ConcurrentHashMap<>();
    }

    public Map<String, TableMeta> getTableMetas() {
        return tableMetas;
    }

    public void addTableMeta(String tbName, TableMeta tblMeta) {
        this.tableMetas.put(tbName, tblMeta);
    }

    public TableMeta dropTable(String tbName) {
        return this.tableMetas.remove(tbName);
    }

    public TableMeta getTableMeta(String tbName) {
        return this.tableMetas.get(tbName);
    }

    public void addViewMeta(String viewName, ViewMeta viewMeta) {
        this.viewMetas.put(viewName, viewMeta);
    }

    /**
     * try to get a view meta of querynode
     *
     * @param name
     * @return
     */
    public PlanNode getView(String name) {
        if (name.contains("`")) {
            name = StringUtil.removeBackQuote(name);
        }
        ViewMeta view = viewMetas.get(name);
        PlanNode queryNode = null;
        if (view != null) {
            if (view.getViewQuery() != null) {
                queryNode = view.getViewQuery().copy();
            }
        }
        return queryNode;
    }

    public ViewMeta getViewMeta(String name) {
        if (name.contains("`")) {
            name = StringUtil.removeBackQuote(name);
        }
        return viewMetas.get(name);
    }

    public ConcurrentMap<String, ViewMeta> getViewMetas() {
        return viewMetas;
    }

    public void setViewMetas(ConcurrentMap<String, ViewMeta> viewMetas) {
        this.viewMetas = viewMetas;
    }

    public SchemaMeta metaCopy() {
        SchemaMeta newMeta = new SchemaMeta();
        for (Map.Entry<String, TableMeta> entry : this.tableMetas.entrySet()) {
            newMeta.tableMetas.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, ViewMeta> entry : this.viewMetas.entrySet()) {
            newMeta.viewMetas.put(entry.getKey(), entry.getValue());
        }
        return newMeta;
    }

}
