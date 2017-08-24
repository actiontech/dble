package io.mycat.meta;

import io.mycat.meta.protocol.StructureMeta.TableMeta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SchemaMeta {

    /**
     * <table,tableMeta>
     */
    private final ConcurrentMap<String, TableMeta> tableMetas;

    public SchemaMeta() {
        this.tableMetas = new ConcurrentHashMap<>();
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

}
