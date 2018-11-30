/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.util.StringUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SchemaMeta {

    /**
     * <table,tableMeta>
     */
    private final ConcurrentMap<String, StructureMeta.TableMeta> tableMetas;

    private volatile ConcurrentMap<String, ViewMeta> viewMetas;

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
        if (name.indexOf("`") != -1) {
            name = StringUtil.removeBackQuote(name);
        }
        ViewMeta view = viewMetas.get(name);
        QueryNode queryNode = null;
        if (view != null) {
            if (view.getViewQuery() != null) {
                queryNode = view.getViewQuery().copy();
            } else {
                ErrorPacket error = view.initAndSet(true, false);
                if (error != null) {
                    throw new RuntimeException(" View '" + view.getViewName() +
                            "' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them");
                } else {
                    queryNode = view.getViewQuery().copy();
                }
            }
        }
        return queryNode;
    }

    public ConcurrentMap<String, ViewMeta> getViewMetas() {
        return viewMetas;
    }

    public void setViewMetas(ConcurrentMap<String, ViewMeta> viewMetas) {
        this.viewMetas = viewMetas;
    }

    public String getViewMetaJson() {
        return null;
    }

}
