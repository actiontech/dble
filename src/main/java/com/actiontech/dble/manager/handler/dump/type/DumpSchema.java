package com.actiontech.dble.manager.handler.dump.type;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;

import java.util.Map;

public class DumpSchema extends DumpContent {

    private String schema;
    private Map<String, PhysicalDBNode> databases;
    private Map<String, StringBuilder> dataNodeStmts;

    public DumpSchema(String schema) {
        this.schema = schema;
        this.dataNodes = DbleServer.getInstance().getConfig().getSchemas().get(schema).getAllDataNodes();
        this.databases = DbleServer.getInstance().getConfig().getDataNodes();
    }

    public String getSchema() {
        return schema;
    }

    public void setDataNodeStmts(Map<String, StringBuilder> dataNodeStmts) {
        this.dataNodeStmts = dataNodeStmts;
    }

    public Map<String, PhysicalDBNode> getDatabases() {
        return this.databases;
    }

    @Override
    public String get(String dataNode) {
        StringBuilder sb = this.dataNodeStmts.remove(dataNode);
        if (sb != null) {
            return sb.toString();
        }
        return null;
    }

    @Override
    public String toString() {
        return "schema[" + schema + ']';
    }

}
