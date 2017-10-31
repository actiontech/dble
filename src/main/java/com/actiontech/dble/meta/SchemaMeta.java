/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.node.QueryNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SchemaMeta {

    /**
     * <table,tableMeta>
     */
    private final ConcurrentMap<String, StructureMeta.TableMeta> tableMetas;

    private final ConcurrentMap<String, ViewMeta> viewMetas;

    public SchemaMeta() {
        this.tableMetas = new ConcurrentHashMap<>();
        this.viewMetas = new ConcurrentHashMap<>();
    }

    public Map<String, StructureMeta.TableMeta> getTableMetas() {
        return tableMetas;
    }


    public void addTableMeta(String tbName, StructureMeta.TableMeta tblMeta) {
        this.tableMetas.put(tbName, tblMeta);
    }

    public StructureMeta.TableMeta dropTable(String tbName) {
        return this.tableMetas.remove(tbName);
    }

    public StructureMeta.TableMeta getTableMeta(String tbName) {
        return this.tableMetas.get(tbName);
    }

    /**
     * try to get a view meta of querynode
     *
     * @param name
     * @return
     */
    public QueryNode getView(String name) {
        ViewMeta view = viewMetas.get(name);
        QueryNode queryNode = null;
        if (view != null) {
            queryNode = view.getViewQuery().copy();
        }
        return queryNode;
    }

    public ConcurrentMap<String, ViewMeta> getViewMetas() {
        return viewMetas;
    }

    public String getViewMetaJson() {
        return null;
    }

}
